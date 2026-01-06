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
from scripts.create_target_qcr_table import (
    QualityControlRepair,
    create_etl_log_qcr_table_if_not_exists,
    create_qcr_table_if_not_exists
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
        create_etl_log_qcr_table_if_not_exists(engine)
        def to_naive(dt): return dt.naive() if hasattr(dt, "naive") else dt.replace(tzinfo=None) if dt else None
        with engine.begin() as conn:
            conn.execute(text("""
                INSERT INTO etl_qcr_extract_log
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
        create_qcr_table_if_not_exists(engine)
        with engine.connect() as conn:
            res = conn.execute(text("""
                SELECT MAX(lastextractdatetime)
                FROM etl_qcr_extract_log
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
            CASE
                WHEN DATEPART(HOUR, QC_Rework_1.QCR_Defect_DateTime) >= 8
                    THEN CONVERT(DATE, QC_Rework_1.QCR_Defect_DateTime)
                ELSE DATEADD(DAY, -1, CONVERT(DATE, QC_Rework_1.QCR_Defect_DateTime))
            END AS qcr_date,
            CASE 
                WHEN CAST(QC_Rework_1.QCR_Defect_DateTime AS TIME) BETWEEN '07:00:00' AND '16:00:00' 
                THEN 'Day' 
                ELSE 'Night' 
            END as shift,
            QC_Rework_1.QCR_Key AS qcr_key,
            QC_Rework_1.QCR_Defect_EM_Key AS qcr_defect_em_key,
            EM_Master_Defect.EM_FirstName AS defect_em_firstname,
            EM_Master_Defect.EM_LastName AS defect_em_lastname,
            QC_Rework_1.QCR_Defect_Quantity AS qcr_defect_quantity,
            QC_Rework_1.QCR_Defect_ST_Key AS qcr_defect_st_key,
            Style_Master_1.ST_ID AS st_id,
            Style_Master_1.ST_Description AS st_description,
            Style_Master_1.ST_Collection AS st_collection,
            QC_Rework_1.QCR_Defect_OC_Key AS qcr_defect_oc_key,
            Operation_Codes_1.OC_Description AS oc_description,
            Primary_Codes_1.PC_Description AS pc_description,
            Style_Operations_1.STOP_Number AS st_po_number,
            QC_Rework_1.QCR_Defect_CM_Key AS qcr_defect_cm_key,
            Colour_Master_1.CM_Short_Description AS cm_short_description,
            Colour_Master_1.CM_Description AS cm_description,
            QC_Rework_1.QCR_Defect_SM_Key AS qcr_defect_sm_key,
            Size_Master_1.SM_Short_Description AS sm_short_description,
            Size_Master_1.SM_Description AS sm_description,
            QC_Rework_1.QCR_QCSC_Key AS qcr_qcsc_key,
            QC_Sub_Codes_1.QCSC_Description AS qcsc_description,
            QC_Sub_Codes_1.QCSC_Is_Rework AS qcsc_is_rework,

            QC_Rework_1.QCR_From_QC_Station AS qcr_station,
            QC_Rework_1.QCR_QC_DateTime AS qcr_defect_datetime,
            QC_Rework_1.QCR_Repair_EM_Key AS qcr_repair_em_key,
            EM_Master_Rework.EM_FirstName AS em_repair_firstname,
            EM_Master_Rework.EM_LastName AS em_repair_lastname,
            QC_Rework_1.QCR_Repair_DateTime AS qcr_repair_datetime,
            QC_Rework_1.QCR_Repair_Quantity AS qcr_repair_quantity,

            EM_Master_QC.EM_Department AS qcr_oc_em_department,
            EM_Master_QC.EM_SSN AS qcr_qc_em_ssn,
            EM_Master_QC.EM_City AS qcr_qc_em_city,
            EM_Master_Defect.EM_Department AS qcr_defect_em_department,
            EM_Master_Defect.EM_SSN AS qcr_defect_em_ssn,
            EM_Master_Defect.EM_City AS qcr_defect_em_city,
            EM_Master_Rework.EM_Department AS qcr_repair_em_department,
            EM_Master_Rework.EM_SSN AS qcr_repair_em_ssn,
            EM_Master_Rework.EM_City AS qcr_repair_em_city,

            EM_Master_Defect.EM_LCD_Name AS qcr_defect_em_lcd_name,
            EM_Master_QC.EM_LCD_Name AS qcr_qc_em_lcd_name,
            EM_Master_Rework.EM_LCD_Name AS qcr_repair_em_lcd_name,

            COALESCE( EM_Master_Defect.EM_Department,'ina-db-6r') AS source_line,   
            QC_Rework_1.QCR_STPO_Key AS qcr_stpo_key

        FROM
        IHS_SHARED.dbo.Style_Master AS Style_Master_1
        INNER JOIN IHS_SHARED.dbo.Style_Operations_Master AS Style_Operations_Master_1 
            ON Style_Master_1.ST_STOPM_Key = Style_Operations_Master_1.STOPM_Key
        INNER JOIN IHS_SHARED.dbo.Style_Operations AS Style_Operations_1 
            ON Style_Operations_Master_1.STOPM_Key = Style_Operations_1.STOP_STOPM_Key
        RIGHT OUTER JOIN IHS_SHARED.dbo.QC_Rework AS QC_Rework_1 
            ON Style_Operations_1.STOP_OC_Key = QC_Rework_1.QCR_Defect_OC_Key
        AND Style_Master_1.ST_Key = QC_Rework_1.QCR_Defect_ST_Key
        LEFT OUTER JOIN IHS_SHARED.dbo.Size_Master AS Size_Master_1 
            ON Size_Master_1.SM_Key = QC_Rework_1.QCR_Defect_SM_Key
        LEFT OUTER JOIN IHS_SHARED.dbo.Colour_Master AS Colour_Master_1 
            ON QC_Rework_1.QCR_Defect_CM_Key = Colour_Master_1.CM_Key
        LEFT OUTER JOIN IHS_SHARED.dbo.Operation_Codes AS Operation_Codes_1 
            ON QC_Rework_1.QCR_Defect_OC_Key = Operation_Codes_1.OC_Key
        LEFT OUTER JOIN .IHS_SHARED.dbo.Primary_Codes AS Primary_Codes_1 
            ON Primary_Codes_1.PC_Key = Operation_Codes_1.OC_PC_Key
        LEFT OUTER JOIN IHS_SHARED.dbo.QC_Sub_Codes AS QC_Sub_Codes_1 
            ON QC_Rework_1.QCR_QCSC_Key = QC_Sub_Codes_1.QCSC_Key
        LEFT OUTER JOIN IHS_SHARED.dbo.Employee_Master_ERP AS EM_Master_Defect 
            ON RIGHT(QC_Rework_1.QCR_Defect_EM_Key,6) = RIGHT(EM_Master_Defect.EM_Key,6)
        LEFT OUTER JOIN IHS_SHARED.dbo.Employee_Master_ERP AS EM_Master_Rework 
            ON RIGHT(QC_Rework_1.QCR_Repair_EM_Key,6) = RIGHT(EM_Master_Rework.EM_Key,6)
        LEFT OUTER JOIN IHS_SHARED.dbo.Employee_Master AS EM_Master_QC 
            ON QC_Rework_1.QCR_Sent_To_Rework_By_EM_Key = EM_Master_QC.EM_Key
        WHERE QCR_Defect_DateTime > ?
        ORDER BY QCR_Defect_DateTime ASC;
        """

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
                    "qcr_key": str(d.get("qcr_key")),
                    "qcr_stpo_key": sanitize_numeric(d.get("qcr_stpo_key")),
                    "qcr_defect_datetime": d.get("qcr_defect_datetime"),
                    "qcr_date": d.get("qcr_date"),
                    "shift": d.get("shift"),
                    "qcr_defect_em_key": sanitize_numeric(d.get("qcr_defect_em_key")),
                    "defect_em_firstname": d.get("defect_em_firstname"),
                    "defect_em_lastname": d.get("defect_em_lastname"),
                    "defect_em_rfid": d.get("defect_em_rfid"),
                    "qcr_defect_st_key": sanitize_numeric(d.get("qcr_defect_st_key")),
                    "qcr_defect_oc_key": sanitize_numeric(d.get("qcr_defect_oc_key")),
                    "oc_description": d.get("oc_description"),
                    "qcr_sent_to_rework_by_em_key": sanitize_numeric(d.get("qcr_sent_to_rework_by_em_key")),
                    "qcr_defect_quantity": sanitize_numeric(d.get("qcr_defect_quantity")),
                    "qcr_from_qc_station": d.get("qcr_from_qc_station"),
                    "qcr_hm_id": d.get("qcr_hm_id"),
                    "qcr_qc_datetime": d.get("qcr_qc_datetime"),
                    "qcr_repair_em_key": sanitize_numeric(d.get("qcr_repair_em_key")),
                    "em_repair_firstname": d.get("em_repair_firstname"),
                    "em_repair_lastname": d.get("em_repair_lastname"),
                    "em_repair_rfid": d.get("em_repair_rfid"),
                    "qcr_repair_datetime": d.get("qcr_repair_datetime"),
                    "qcr_repair_quantity": sanitize_numeric(d.get("qcr_repair_quantity")),
                    "qcr_defect_cm_key": sanitize_numeric(d.get("qcr_defect_cm_key")),
                    "cm_description": d.get("cm_description"),
                    "qcr_defect_sm_key": sanitize_numeric(d.get("qcr_defect_sm_key")),
                    "sm_description": d.get("sm_description"),
                    "qcr_qcsc_key": str(d.get("qcr_qcsc_key")),
                    "qcsc_description": d.get("qcsc_description"),
                    "st_id": d.get("st_id"),
                    "st_description": d.get("st_description"),
                    "stpo_st_key": sanitize_numeric(d.get("stpo_st_key")),
                    "stpo_id": d.get("stpo_id"),
                    "stpo_ci_name": d.get("stpo_ci_name"),
                    'created_at': datetime.now(PKT),
                    'source_connection': d.get("source_line"),

                })

            yield batch
            total += len(batch)
            batch_no += 1
            logger.info(f"[{connection_id}] ✅ Batch {batch_no}: {len(batch)} rows fetched")
            check_memory(f"{connection_id} - after batch {batch_no}")

        logger.info(f"[{connection_id}] 🎯 Extraction complete → {total} rows in {time.time() - start_time:.2f}s")


# ---------------- UPSERT ---------------- #
@retry_on_exception()
def qcr_upsert_to_postgres(connection_id: str) -> str:
    start_t = pendulum.now(PKT)
    engine = get_postgres_engine()
    create_qcr_table_if_not_exists(engine)
    upserted, last_dt = 0, None
    try:
        for batch in fetch_data_from_source(connection_id) or []:
            if not batch:
                continue

            # Deduplicate batch by unique key (qcr_key, source_connection, qcr_date)
            # Keep the most recent record by qcr_defect_datetime
            unique_key = lambda r: (r["qcr_key"], r["source_connection"], r["qcr_date"])
            batch = sorted(batch, key=lambda r: r.get("qcr_defect_datetime") or datetime.min.replace(tzinfo=PKT), reverse=True)
            deduplicated_batch = []
            seen_keys = set()
            for record in batch:
                key = unique_key(record)
                if key not in seen_keys:
                    deduplicated_batch.append(record)
                    seen_keys.add(key)
            batch = deduplicated_batch

            if not batch:
                continue

            with engine.begin() as conn:
                stmt = insert(QualityControlRepair).values(batch)
                stmt = stmt.on_conflict_do_update(
                    index_elements=["qcr_key","source_connection","qcr_date"],
                    set_={col.name: stmt.excluded[col.name]
                          for col in QualityControlRepair.__table__.columns
                          if col.name not in ("qcr_key","source_connection","qcr_date")}

                )
                conn.execute(stmt)
                upserted += len(batch)
                latest = max((b.get("qcr_defect_datetime") for b in batch if b.get("qcr_defect_datetime")), default=None)
                if latest and (not last_dt or latest > last_dt):
                    last_dt = latest
            check_memory(f"{connection_id} - upsert batch")
        end_t = pendulum.now(PKT)
        if upserted > 0:
            insert_etl_log(str(uuid.uuid4()), connection_id, upserted, start_t, end_t, last_dt, True, "Completed", None)
        else:
            insert_etl_log(str(uuid.uuid4()), connection_id, upserted, start_t, end_t, None, True, "No New Data", "No new records to upsert")    

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
