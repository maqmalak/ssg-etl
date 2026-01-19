"""
Optimized Hanger Line Data ETL Utilities
---------------------------------------
Handles extraction from PostgreSQL and upsert to PostgreSQL with:
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
def get_target_postgres_engine():
    """Create Target PostgreSQL engine with Airflow connection or fallback."""
    from urllib.parse import quote_plus
    try:
        c = BaseHook.get_connection("pg-ssg")
        uri = f"postgresql://{c.login}:{quote_plus(c.password or '')}@{c.host}:{c.port}/{c.schema}"
        logger.info(f"[PG] Target Connected via Airflow: {c.host}/{c.schema}")
    except Exception as e:
        logger.warning(f"[PG] Target Airflow conn failed ({e}), using fallback")
        uri = f"postgresql://postgres:{quote_plus('P@kistan12')}@172.16.7.6:5432/ssg"

    return create_engine(uri, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600, echo=False)


def get_source_postgres_engine():
    """Create Source PostgreSQL engine with Airflow connection or fallback."""
    from urllib.parse import quote_plus
    try:
        c = BaseHook.get_connection("INA-7A")
        uri = f"postgresql://{c.login}:{quote_plus(c.password or '')}@{c.host}:{c.port}/{c.schema}"
        logger.info(f"[PG] Source Connected via Airflow: {c.host}/{c.schema}")
    except Exception as e:
        logger.warning(f"[PG] Source Airflow conn failed ({e}), using fallback")
        uri = f"postgresql://postgres:{quote_plus('P@kistan12')}@172.16.7.6:5432/ssg"

    return create_engine(uri, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600, echo=False)

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
    engine = get_target_postgres_engine()
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
    engine = get_target_postgres_engine()
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
    Fetch data from Source connection Postgres in batches with safe type casting and memory optimization.

    Args:
        connection_id (str): Airflow Postgress connection ID.

    Yields:
        List[Dict[str, Any]]: Cleaned and validated batches of records.
    """
    start_time = time.time()
    logger.info(f"[{connection_id}] 🚀 Starting data extraction...")

    # Memory pre-check
    check_memory(f"{connection_id} - initial check")

    # Get source postgres connection and last extract timestamp
    conn_str = get_source_postgres_engine()
    # conn_str = build_mssql_conn_str(connection)
    last_extract_dt = get_last_extract_dt_from_log(connection_id)

        # If no previous extract, get minimum CreationDate from source
    if not last_extract_dt:
        logger.info(f"[{connection_id}] Using min CreationDate from source: {last_extract_dt}")


    # SQL Query (fully aligned)
    query = """
        SELECT
            pqtm.pqtm_key AS qcr_key, 
            CASE
                WHEN EXTRACT(HOUR FROM pqtm.pqtm_date_back) >= 8
                    THEN pqtm.pqtm_date_back::date
                ELSE (pqtm.pqtm_date_back - INTERVAL '1 day')::date
            END AS qcr_date, 
            CASE
                WHEN pqtm.pqtm_date_back::time BETWEEN '07:00:00'::time AND '16:00:00'::time
                    THEN 'Day'
                ELSE 'Night'
            END AS shift, 
            pqtm_bsm_code_back AS qcr_station, 
            
            CASE
                WHEN LEFT(pqtm.pqtm_bsm_code_back, 2) = '10' THEN 'line-30'
                WHEN LEFT(pqtm.pqtm_bsm_code_back, 2) = '11' THEN 'line-21'
                WHEN LEFT(pqtm.pqtm_bsm_code_back, 2) = '12' THEN 'line-32'
                ELSE pqtm.pqtm_bsm_code_back
            END AS source_connection, 
            pqtm.pqtm_date_back qcr_defect_datetime, 
            pqtm.pqtm_complete_time qcr_repair_datetime, 
            pqtr.pqtr_key, 
            pqtr.pqtr_hei_key_receive, 
            hei_receive.hei_code AS qcr_defect_em_key, 
            hei_receive.hei_name AS defect_em_firstname, 
            pqtr.pqtr_hei_key_repair, 
            pqtr.pqtr_hei_key_recheck, 
            pqtr.pqtr_bindquantity, 
            pqta.pqta_bqci_name, 
            pqta.pqta_quantity AS qcr_defect_quantity, 
            pqta.pqta_decisionresult qcsc_description, 
            pqta.pqta_bqci_code, 
            pwb.pwb_code AS item_id, 
            pwb.pwb_mixcode AS st_po_number, 
            pwb.pwb_psi_key, 
            pwb.pwb_psi_code AS st_id, 
            pwb.pwb_psi_name AS st_description, 
            pwb.pwb_pci_key, 
            pwb.pwb_pci_code AS qcr_defect_cm_key, 
            pwb.pwb_pci_name AS cm_description, 
            pwb.pwb_psz_key, 
            pwb.pwb_psz_code AS qcr_defect_sm_key, 
            pwb.pwb_psz_name AS sm_description, 
            pqtr.pqtr_poi_key_receive, 
            poi.poi_code AS qcr_defect_oc_key, 
            poi.poi_name AS oc_description, 
            hei_repair.hei_code AS qcr_repair_em_key, 
            hei_repair.hei_name AS em_repair_firstname
        FROM
            pmr_quality_tracking_master AS pqtm
            INNER JOIN
                pmr_quality_tracking_result AS pqtr 
                ON  pqtm.pqtm_key = pqtr.pqtr_pqtm_key
            INNER JOIN pmr_quality_tracking_answer AS pqta
                ON  pqtm.pqtm_key = pqta.pqta_pqtm_key AND
                    pqtr.pqtr_key = pqta.pqta_pqtr_key
            INNER JOIN
                hr_employee_info AS hei_receive
                ON  pqtr.pqtr_hei_key_receive = hei_receive.hei_key
            INNER JOIN
                pm_work_bill AS pwb
                ON  pqtm.pqtm_pwb_key = pwb.pwb_key
            INNER JOIN
                pdm_operation_info AS poi
                ON  pqtr.pqtr_poi_key_receive = poi.poi_key
            LEFT JOIN
                hr_employee_info AS hei_repair
                ON 
                pqtr.pqtr_hei_key_repair = hei_repair.hei_key
        WHERE
            pqtm.pqtm_date_back::date >= ?
        ORDER BY
            pqtm.pqtm_date_back DESC;
        """
#         conn = get_database_connection()
#         cursor = conn.cursor()
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
                    "qcr_defect_st_key": sanitize_numeric(d.get("qcr_defect_st_key")),
                    "qcr_defect_oc_key": sanitize_numeric(d.get("qcr_defect_oc_key")),
                    "oc_description": d.get("oc_description"),
                    "qcr_defect_quantity": sanitize_numeric(d.get("qcr_defect_quantity")),
                    "qcr_from_qc_station": d.get("qcr_from_qc_station"),
                    "qcr_qc_datetime": d.get("qcr_qc_datetime"),
                    "qcr_repair_em_key": sanitize_numeric(d.get("qcr_repair_em_key")),
                    "em_repair_firstname": d.get("em_repair_firstname"),
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
    engine = get_target_postgres_engine()
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
