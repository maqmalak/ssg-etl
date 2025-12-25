"""
Optimized Hanger Line Data ETL Utilities
---------------------------------------
Handles extraction from MSSQL and upsert to PostgreSQL with:
✅ Safe numeric casting
✅ Memory management
✅ ETL logging
✅ Fault tolerance
"""

from __future__ import annotations
import logging, time, uuid, sys, os, gc, psutil
from datetime import datetime
from typing import Dict, List, Optional, Any, Generator

import pendulum
import pyodbc
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert

# Project imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from scripts.create_target_pg_hl_table import (
    HangerLaneData,
    create_etl_log_table_if_not_exists,
    create_table_if_not_exists
)

# ---------------- CONFIG ---------------- #
PKT = pendulum.timezone("Asia/Karachi")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

MAX_RETRIES = 3
RETRY_DELAY = 5
BATCH_SIZE = 1000
MAX_MEMORY_USAGE_PERCENT = 80.0


# ---------------- MEMORY MANAGEMENT ---------------- #
def get_memory_usage() -> float:
    return psutil.virtual_memory().percent


def perform_memory_cleanup(operation: str = "GC") -> None:
    gc.collect()
    logger.info(f"[MEMORY] {operation} → cleanup done ({get_memory_usage():.2f}%)")


def check_memory(operation: str) -> None:
    usage = get_memory_usage()
    if usage > MAX_MEMORY_USAGE_PERCENT:
        logger.warning(f"[MEMORY] High usage ({usage:.2f}%) during {operation}")
        perform_memory_cleanup(operation)


# ---------------- RETRY DECORATOR ---------------- #
def retry_on_exception(max_retries: int = MAX_RETRIES, delay: int = RETRY_DELAY):
    def decorator(func):
        def wrapper(*args, **kwargs):
            for attempt in range(1, max_retries + 1):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    logger.warning(f"[Retry {attempt}/{max_retries}] {func.__name__} failed: {e}")
                    if attempt < max_retries:
                        time.sleep(delay)
                    else:
                        logger.error(f"❌ {func.__name__} failed after {max_retries} attempts.")
                        raise
        return wrapper
    return decorator


# ---------------- DB CONNECTIONS ---------------- #
def get_postgres_engine():
    """Create PostgreSQL engine with Airflow connection or fallback."""
    from urllib.parse import quote_plus
    try:
        c = BaseHook.get_connection("pg-ssg")
        uri = f"postgresql://{c.login}:{quote_plus(c.password or '')}@{c.host}:{c.port}/{c.schema}"
        logger.info(f"[PG] Connected via Airflow: {c.host}/{c.schema}")
    except Exception as e:
        logger.warning(f"[PG] Airflow conn failed ({e}), using fallback")
        uri = f"postgresql://postgres:{quote_plus('P@kistan12')}@172.16.7.6:5432/ssg"

    return create_engine(uri, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600, echo=False)


def build_mssql_conn_str(connection) -> str:
    return (
        f"DRIVER={{FreeTDS}};SERVER={connection.host};PORT=1433;"
        f"DATABASE={connection.schema};UID={connection.login};PWD={connection.password};"
        "TDS_Version=7.0;Connect Timeout=30;Login Timeout=30;Query Timeout=60;"
    )


# ---------------- SANITIZERS ---------------- #
def sanitize_numeric(value: Any) -> Optional[int]:
    """Safe integer conversion."""
    try:
        if value is None or str(value).strip().upper() in ("", "NULL", "NONE", "N/A", "BASE"):
            return None
        return int(float(value))
    except Exception:
        return None


def sanitize_float(value: Any) -> Optional[float]:
    """Safe float conversion."""
    try:
        if value is None or str(value).strip().upper() in ("", "NULL", "NONE", "N/A", "BASE"):
            return None
        return float(value)
    except Exception:
        return None


# ---------------- ETL LOGGING ---------------- #
def insert_etl_log(pid: str, src: str, count: int, start: datetime, end: datetime,
                   last_dt: Optional[datetime], success: bool, status: str, msg: Optional[str]):
    """Insert ETL run log."""
    engine = get_postgres_engine()
    try:
        create_etl_log_table_if_not_exists(engine)
        def to_naive(dt): return dt.naive() if hasattr(dt, "naive") else dt.replace(tzinfo=None) if dt else None
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO etl_extract_log
                (processlogid, source_connection, saved_count, starttime, endtime,
                 lastextractdatetime, success, status, errormessage)
                VALUES (:pid, :src, :cnt, :start, :end, :ldt, :success, :status, :msg)
            """), {
                "pid": pid, "src": src, "cnt": count,
                "start": to_naive(start), "end": to_naive(end),
                "ldt": last_dt, "success": success, "status": status, "msg": msg
            })
        logger.info(f"[LOG] {src}: {status} ({count} rows)")
    except Exception as e:
        logger.error(f"[LOG] Failed to write ETL log: {e}")
    finally:
        engine.dispose()


@retry_on_exception()
def get_last_extract_dt_from_log(src: str) -> Optional[datetime]:
    engine = get_postgres_engine()
    try:
        create_etl_log_table_if_not_exists(engine)
        with engine.connect() as conn:
            res = conn.execute(text("""
                SELECT MAX(lastextractdatetime)
                FROM etl_extract_log
                WHERE status = 'Completed' AND source_connection=:s AND saved_count>0
            """), {"s": src}).scalar()
        logger.info(f"Last extract datetime for {src}: {res}")
        return res
    except Exception as e:
        logger.warning(f"[{src}] Could not fetch last extract time: {e}")
        return None
    finally:
        engine.dispose()


# ---------------- DATA FETCH ---------------- #
@retry_on_exception()
def fetch_data_from_source(connection_id: str) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Fetch data from MSSQL in batches with safe type casting and memory optimization.

    Args:
        connection_id (str): Airflow MSSQL connection ID.

    Yields:
        List[Dict[str, Any]]: Cleaned and validated batches of records.
    """
    start_time = time.time()
    logger.info(f"[{connection_id}] 🚀 Starting data extraction...")

    # Memory pre-check
    check_memory(f"{connection_id} - initial check")

    # Get MSSQL connection and last extract timestamp
    connection = BaseHook.get_connection(connection_id)
    conn_str = build_mssql_conn_str(connection)
    last_extract_dt = get_last_extract_dt_from_log(connection_id)

        # If no previous extract, get minimum CreationDate from source
    if not last_extract_dt:
        logger.info(f"[{connection_id}] Using min CreationDate from source: {last_extract_dt}")


    # SQL Query (fully aligned)
    query = """
        SELECT
            /*# IHS.dbo.ODP_Master.ODP_Date AS odp_date,*/

            DATEADD(DAY, 
            - CASE WHEN DATEPART(HOUR, CAST(IHS.dbo.ODP_Master.ODP_Actual_Clock_In AS DATETIMEOFFSET)) < 5 THEN 1 ELSE 0 END,
            CAST(CAST(IHS.dbo.ODP_Master.ODP_Actual_Clock_In AS DATETIMEOFFSET) AS DATE)
           ) AS odp_date,

            IHS.dbo.ODP_Master.ODP_Key AS odp_key,
            /*IHS.dbo.ODP_Master.ODP_Shift AS odp_shift_key,*/
            /*# CASE WHEN [ODP_Shift] = 1 THEN 'Day' ELSE 'Night' END AS shift,*/

            CASE 
                WHEN CAST(IHS.dbo.ODP_Master.ODP_Actual_Clock_In AS TIME) BETWEEN '07:00:00' AND '16:00:00' 
                THEN 'Day' 
                ELSE 'Night' 
            END as shift,

            IHS.dbo.ODP_Detail.ODPD_Key AS odpd_key,
            /*---Employeeee------------*/
            Employee_Master_1.EM_RFID AS em_rfid,
            IHS.dbo.ODP_Master.ODP_EM_Key AS odp_em_key,
            Employee_Master_1.EM_Department AS em_department,
            Employee_Master_1.EM_FirstName AS em_firstname,
            Employee_Master_1.EM_LastName AS em_lastname,
            Employee_Master_1.EM_SSN AS em_ssn,
            Employee_Master_1.EM_City AS em_city,
            Employee_Master_1.EM_LCD_Name AS odpd_em_lcd_name,

            IHS.dbo.ODP_Master.[ODP_Actual_Clock_In] AS odp_actual_clock_in,
            IHS.dbo.ODP_Master.[ODP_Actual_Clock_Out] AS odp_actual_clock_out,
            IHS.dbo.ODP_Master.[ODP_Shift_Clock_In] AS odp_shift_clock_in,
            IHS.dbo.ODP_Master.[ODP_Shift_Clock_Out] AS odp_shift_clock_out,
            IHS.dbo.ODP_Master.[ODP_First_Hanger_Time] AS odp_first_hanger_time,
            IHS.dbo.ODP_Master.[ODP_Last_Hanger_Time] AS odp_last_hanger_time,
            IHS.dbo.ODP_Master.[ODP_Current_Station] AS odp_current_station,
            IHS.dbo.ODP_Master.[ODP_Lump_Sum_Payment] AS odp_lump_sum_payment,
            IHS.dbo.ODP_Master.[ODP_Make_Up_Pay_Rate] AS odp_make_up_pay_rate,
            IHS.dbo.ODP_Master.[ODP_Last_Hanger_Start_Time] AS odp_last_hanger_start_time,
            IHS.dbo.ODP_Detail.ODPD_WC_Key AS odpd_wc_key,
            Work_Codes_1.WC_Description AS ODPI_WC_Description,
	
            IHS.dbo.ODP_Detail.ODPD_Workstation AS odpd_workstation,
            IHS.dbo.ODP_Detail.ODPD_Quantity AS odpd_quantity,

            IHS.dbo.ODP_Detail.ODPD_OC_Key AS odpd_oc_key,
            Operation_Codes_1.OC_Description AS oc_description,
            CASE WHEN [OC_Description] = 'Loading/Panel Segregation' THEN IHS.dbo.ODP_Detail.ODPD_Quantity ELSE 0 END AS loading_qty,
            CASE WHEN [OC_Description] IN ('Garment Insert in Poly Bag & Close') THEN IHS.dbo.ODP_Detail.ODPD_Quantity ELSE 0 END AS unloading_qty,
            Colour_Master_1.CM_Short_Description AS cm_short_description,
            Size_Master_1.SM_Short_Description AS sm_short_description,
            IHS.dbo.ODP_Detail.ODPD_Standard AS oc_standard_time,
            IHS.dbo.ODP_Detail.ODPD_Piece_Rate AS oc_piece_rate,
            IHS.dbo.ODP_Detail.ODPD_Actual_Time AS odpd_actual_time,


            Colour_Master_1.CM_Description AS cm_description,
            Size_Master_1.SM_Description AS sm_description,
            IHS.dbo.ODP_Detail.ODPD_PA_Key AS odpd_pa_key,
            Pay_Categories_1.PA_CurrencyValue AS odpd_pa_currencyvalue,
            Pay_Categories_1.PA_CategoryType AS odpd_pa_categorytype,
            Pay_Categories_1.PA_Description AS odpd_pa_description,
            IHS.dbo.ODP_Detail.ODPD_Is_Overtime AS odpd_is_overtime,
            IHS.dbo.ODP_Detail.ODPD_Start_Time AS odpd_start_time,

            Style_Master_1.ST_ID AS st_id,
            Style_Master_1.ST_Description AS st_description,
            IHS.dbo.ODP_Detail.ODPD_Lot_Number AS odpd_lot_number,
			
            Primary_Codes_1.PC_Description AS odpd_pc_description,
            Style_Master_1.ST_Collection AS odpd_st_collection,
            IHS.dbo.ODP_Detail.ODPD_ST_Key AS odpd_st_key,
            Machine_Codes_1.MC_Type AS odpd_mc_type,
            Style_Operations_1.STOP_Order AS odpd_stop_order,
            Style_Operations_1.STOP_Number AS odpd_stop_number,
            Operation_Codes_1.OC_Piece_Rate_Additional AS odpd_oc_piece_rate_additional,


            Style_Master_1.ST_Fabric AS odpd_st_fabric,
            IHS.dbo.ODP_Detail.ODPD_STPO_Key AS odpd_stpo_key,
            Style_Planned_Orders_1.STPO_ID AS ODPI_STPO_Number,
            Style_Master_1.ST_Trim AS odpd_st_trim,
            IHS.dbo.ODP_Detail.ODPD_Normal_Pay_Factor AS odpd_normal_pay_factor,
            IHS.dbo.ODP_Detail.ODPD_Overtime_Factor AS odpd_overtime_factor,
            li.Line_Number AS odpd_line_number,
            IHS.dbo.ODP_Master.[modified_at] AS created_at
        FROM
            lnk_svr.IHS_SHARED.dbo.Style_Operations_Master AS Style_Operations_Master_1
            INNER JOIN lnk_svr.IHS_SHARED.dbo.Style_Operations AS Style_Operations_1 ON Style_Operations_Master_1.STOPM_Key = Style_Operations_1.STOP_STOPM_Key
            INNER JOIN lnk_svr.IHS_SHARED.dbo.Style_Master AS Style_Master_1 ON Style_Operations_Master_1.STOPM_Key = Style_Master_1.ST_STOPM_Key
            RIGHT OUTER JOIN IHS.dbo.ODP_Master
            INNER JOIN IHS.dbo.ODP_Detail ON IHS.dbo.ODP_Master.ODP_Key = IHS.dbo.ODP_Detail.ODPD_ODP_Key
            LEFT OUTER JOIN lnk_svr.IHS_SHARED.dbo.Style_Planned_Orders AS Style_Planned_Orders_1 ON IHS.dbo.ODP_Detail.ODPD_STPO_Key = Style_Planned_Orders_1.STPO_Key ON Style_Master_1.ST_Key = IHS.dbo.ODP_Detail.ODPD_ST_Key
            AND Style_Operations_1.STOP_OC_Key = IHS.dbo.ODP_Detail.ODPD_OC_Key
            LEFT OUTER JOIN lnk_svr.IHS_SHARED.dbo.Work_Codes AS Work_Codes_1 ON IHS.dbo.ODP_Detail.ODPD_WC_Key = Work_Codes_1.WC_Key
            LEFT OUTER JOIN lnk_svr.IHS_SHARED.dbo.Primary_Codes AS Primary_Codes_1
            INNER JOIN lnk_svr.IHS_SHARED.dbo.Operation_Codes AS Operation_Codes_1 ON Primary_Codes_1.PC_Key = Operation_Codes_1.OC_PC_Key ON IHS.dbo.ODP_Detail.ODPD_OC_Key = Operation_Codes_1.OC_Key
            LEFT OUTER JOIN lnk_svr.IHS_SHARED.dbo.Employee_Master AS Employee_Master_1 ON IHS.dbo.ODP_Master.ODP_EM_Key = Employee_Master_1.EM_Key
            LEFT OUTER JOIN lnk_svr.IHS_SHARED.dbo.Pay_Categories AS Pay_Categories_1 ON IHS.dbo.ODP_Detail.ODPD_PA_Key = Pay_Categories_1.PA_Key
            LEFT OUTER JOIN lnk_svr.IHS_SHARED.dbo.Machine_Codes AS Machine_Codes_1 ON Primary_Codes_1.PC_MC_Key = Machine_Codes_1.MC_Key
            LEFT OUTER JOIN lnk_svr.IHS_SHARED.dbo.Size_Master AS Size_Master_1 ON IHS.dbo.ODP_Detail.ODPD_SM_Key = Size_Master_1.SM_Key
            LEFT OUTER JOIN lnk_svr.IHS_SHARED.dbo.Colour_Master AS Colour_Master_1 ON IHS.dbo.ODP_Detail.ODPD_CM_Key = Colour_Master_1.CM_Key
            LEFT OUTER JOIN (SELECT TOP (1) Line_Number FROM IHS.dbo.Line_Information ORDER BY Date_Of_Configuration DESC) AS li ON 1 = 1
        WHERE IHS.dbo.ODP_Master.modified_at > ?
        ORDER BY ODP_Last_Hanger_Time ASC;
    """
#  WHERE ODP_Last_Hanger_Time > ?
    with pyodbc.connect(conn_str, timeout=30) as c:
        cur = c.cursor()
        cur.execute(query, [last_extract_dt])
        total, batch_no = 0, 0

        while True:
            rows = cur.fetchmany(BATCH_SIZE)
            if not rows:
                break

            cols = [d[0].lower() for d in cur.description]
            batch = []

            for r in rows:
                d = dict(zip(cols, r))
                batch.append({
                    "odp_date": d.get("odp_date"),
                    "odp_key": str(d.get("odp_key")),
                    "shift": d.get("shift"),
                    "odp_em_key": sanitize_numeric(d.get("odp_em_key")),
                    "em_rfid": str(d.get("em_rfid")) if d.get("em_rfid") else None,
                    "em_department": str(d.get("em_department")) if d.get("em_department") else None,
                    "em_firstname": str(d.get("em_firstname")) if d.get("em_firstname") else None,
                    "em_lastname": str(d.get("em_lastname")) if d.get("em_lastname") else None,
                    "odp_actual_clock_in": d.get("odp_actual_clock_in"),
                    "odp_actual_clock_out": d.get("odp_actual_clock_out"),
                    "odp_shift_clock_in": d.get("odp_shift_clock_in"),
                    "odp_shift_clock_out": d.get("odp_shift_clock_out"),
                    "odp_first_hanger_time": d.get("odp_first_hanger_time"),
                    "odp_last_hanger_time": d.get("odp_last_hanger_time"),
                    "odp_current_station": str(d.get("odp_current_station")) if d.get("odp_current_station") else None,
                    "odp_lump_sum_payment": sanitize_float(d.get("odp_lump_sum_payment")),
                    "odp_make_up_pay_rate": sanitize_float(d.get("odp_make_up_pay_rate")),
                    "odp_last_hanger_start_time": d.get("odp_last_hanger_start_time"),
                    "odpd_key": str(d.get("odpd_key")),
                    "odpd_workstation": str(d.get("odpd_workstation")) if d.get("odpd_workstation") else None,
                    "odpd_wc_key": sanitize_numeric(d.get("odpd_wc_key")),
                    "odpd_quantity": sanitize_numeric(d.get("odpd_quantity")),
                    "odpd_st_key": sanitize_numeric(d.get("odpd_st_key")),
                    "st_id": str(d.get("st_id")) if d.get("st_id") else None,
                    "st_description": str(d.get("st_description")) if d.get("st_description") else None,
                    "odpd_lot_number": str(d.get("odpd_lot_number")) if d.get("odpd_lot_number") else None,
                    "odpd_oc_key": sanitize_numeric(d.get("odpd_oc_key")),
                    "oc_description": str(d.get("oc_description")) if d.get("oc_description") else None,
                    "loading_qty": sanitize_numeric(d.get("loading_qty")),
                    "unloading_qty": sanitize_numeric(d.get("unloading_qty")),
                    "oc_piece_rate": sanitize_float(d.get("oc_piece_rate")),
                    "oc_standard_time": sanitize_float(d.get("oc_standard_time")),
                    "odpd_standard": sanitize_float(d.get("odpd_standard")),
                    "odpd_actual_time": sanitize_float(d.get("odpd_actual_time")),
                    "odpd_pa_key": sanitize_numeric(d.get("odpd_pa_key")),
                    "odpd_pay_rate": sanitize_float(d.get("odpd_pay_rate")),
                    "odpd_piece_rate": sanitize_float(d.get("odpd_piece_rate")),
                    "odpd_start_time": d.get("odpd_start_time"),
                    "odpd_cm_key": sanitize_numeric(d.get("odpd_cm_key")),
                    "cm_description": str(d.get("cm_description")) if d.get("cm_description") else None,
                    "odpd_sm_key": sanitize_numeric(d.get("odpd_sm_key")),
                    "sm_description": str(d.get("sm_description")) if d.get("sm_description") else None,
                    "odpd_normal_pay_factor": sanitize_float(d.get("odpd_normal_pay_factor")),
                    "odpd_is_overtime": bool(d.get("odpd_is_overtime")) if d.get("odpd_is_overtime") is not None else None,
                    "odpd_overtime_factor": sanitize_float(d.get("odpd_overtime_factor")),
                    "odpd_edited_by": str(d.get("odpd_edited_by")) if d.get("odpd_edited_by") else None,
                    "odpd_edited_date": d.get("odpd_edited_date"),
                    "odpd_actual_time_from_reader": sanitize_float(d.get("odpd_actual_time_from_reader")),
                    "odpd_stpo_key": sanitize_numeric(d.get("odpd_stpo_key")),
                    "created_at": d.get("created_at"),
                    "source_connection": connection_id
                })

            yield batch
            total += len(batch)
            batch_no += 1
            logger.info(f"[{connection_id}] ✅ Batch {batch_no}: {len(batch)} rows fetched")
            check_memory(f"{connection_id} - after batch {batch_no}")

        logger.info(f"[{connection_id}] 🎯 Extraction complete → {total} rows in {time.time() - start_time:.2f}s")


# ---------------- UPSERT ---------------- #
@retry_on_exception()
def upsert_to_postgres(connection_id: str) -> str:
    start_t = pendulum.now(PKT)
    engine = get_postgres_engine()
    create_table_if_not_exists(engine)
    upserted, last_dt = 0, None
    try:
        for batch in fetch_data_from_source(connection_id) or []:
            if not batch:
                continue
            with engine.begin() as conn:
                stmt = insert(HangerLaneData).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["source_connection", "odp_key", "odpd_key"],
                    set_={col.name: stmt.excluded[col.name]
                          for col in HangerLaneData.__table__.columns
                        #   if col.name in ("odp_actual_clock_out", "odp_shift_clock_out","odp_last_hanger_time", "odp_last_hanger_start_time", "odpd_quantity", "loading_qty", "unloading_qty", "odpd_standard", "odpd_actual_time", "odpd_start_time", "odpd_edited_by", "odpd_edited_date")}
                          if col.name not in ("source_connection", "odp_key", "odpd_key")}

                )
                conn.execute(stmt)
                upserted += len(batch)
                latest = max((b.get("odp_last_hanger_time") for b in batch if b.get("odp_last_hanger_time")), default=None)
                if latest and (not last_dt or latest > last_dt):
                    last_dt = latest
            check_memory(f"{connection_id} - upsert batch")
        end_t = pendulum.now(PKT)
        insert_etl_log(str(uuid.uuid4()), connection_id, upserted, start_t, end_t, last_dt, True, "Completed", None)
        msg = f"[{connection_id}] ✅ Upserted {upserted} rows"
        logger.info(msg)
        return msg
    except Exception as e:
        end_t = pendulum.now(PKT)
        insert_etl_log(str(uuid.uuid4()), connection_id, upserted, start_t, end_t, last_dt, False, "Failed", str(e))
        logger.error(f"[{connection_id}] ❌ Upsert failed: {e}")
        return f"Failed upsert for {connection_id}: {e}"
    finally:
        engine.dispose()
        perform_memory_cleanup(connection_id)
