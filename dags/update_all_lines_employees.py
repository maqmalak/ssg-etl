"""
ETL DAG: SilverStr → All Hanger Lines – Employee Master Upsert
Works perfectly when Line_Desc = 'line-21', 'line-22', etc.
"""
from __future__ import annotations

import logging
import gc
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pendulum
import pyodbc
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from scripts.constans.db_sources import TARGET_HANGER_LANE

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
PKT = pendulum.timezone("Asia/Karachi")
logger = logging.getLogger(__name__)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime.now(PKT) - timedelta(minutes=30),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
    "catchup": False,
}

SOURCE_CONN_ID = "SilverStr"
CHUNK_SIZE = 1000


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def build_conn_str(conn) -> str:
    return (
        f"DRIVER={{FreeTDS}};"
        f"SERVER={conn.host};PORT=1433;"
        f"DATABASE={conn.schema};"
        f"UID={conn.login};PWD={conn.password};"
        f"TDS_Version=7.0;Connect Timeout=60;Login Timeout=60;Charset=UTF8;"
    )


def chunked(items: List[Any], size: int):
    for i in range(0, len(items), size):
        yield items[i:i + size]


def safe_id(name: str) -> str:
    return "".join(c if c.isalnum() else "_" for c in name)


# ------------------------------------------------------------------
# DAG
# ------------------------------------------------------------------
@dag(
    dag_id="update_all_lines_employees",
    default_args=default_args,
    schedule="*/30 8-23,0-1 * * 1-6",
    tags=["ssg", "hangerline", "employee"],
    max_active_runs=1,
    catchup=False,
)
def update_all_lines_employees():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    def check_source():
        conn = BaseHook.get_connection(SOURCE_CONN_ID)
        with pyodbc.connect(build_conn_str(conn), timeout=30) as cnxn:
            cnxn.execute("SELECT 1")
        logger.info("Source '%s' reachable", SOURCE_CONN_ID)

    @task
    def check_target(conn_id: str):
        conn = BaseHook.get_connection(conn_id)
        with pyodbc.connect(build_conn_str(conn), timeout=30) as cnxn:
            cnxn.execute("SELECT 1")
        logger.info("Target '%s' reachable", conn_id)

    @task
    def fetch_employees(line_conn_id: str) -> List[Dict[str, Any]]:
        """
        IMPORTANT: Your table stores Line_Desc = 'line-21' to 'line-29', etc.
        So we pass the connection ID directly as the filter value.
        """
        conn = BaseHook.get_connection(SOURCE_CONN_ID)
        conn_str = build_conn_str(conn)

        # Normalize: accept "line-21", to  line-29
        line_desc = (line_conn_id or "").strip()
        if (line_desc.startswith("'") and line_desc.endswith("'")) or \
           (line_desc.startswith('"') and line_desc.endswith('"')):
            line_desc = line_desc[1:-1]
            logger.debug("Stripped surrounding quotes from line_conn_id -> %s", line_desc)

        sql = """
            SELECT 
                INA_ID        AS EM_Key,
                Title         AS EM_FirstName,
                Latest_Line_ID AS EM_Department,
                ID            AS EM_SSN
            FROM dbo.hangerline_emp
            WHERE Line_Desc = CAST(? AS NVARCHAR(255)) and INA_ID IS NOT NULL
        """

        try:
            with pyodbc.connect(conn_str, timeout=60) as cnxn:
                cursor = cnxn.cursor()
                # pass single parameter as a 1-tuple to avoid pyodbc interpreting the string as iterable
                cursor.execute(sql, (line_desc,))
                # guard cursor.description in case no resultset
                if cursor.description:
                    cols = [col[0] for col in cursor.description]
                    rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
                else:
                    rows = []
        except Exception as e:
            logger.error("Failed fetching for Line_Desc='%s': %s", line_desc, e)
            raise

        logger.info("Fetched %d employees for Line_Desc='%s'", len(rows), line_desc)
        return rows

    @task
    def prepare_payload(rows: List[Dict]) -> List[Dict]:
        payload = []
        for r in rows:
            try:
                em_key = int(r["EM_Key"])
            except (TypeError, ValueError):
                logger.warning("Bad EM_Key, skipping: %s", r)
                continue

            payload.append({
                "em_key":       em_key,
                "em_firstname": r.get("EM_FirstName"),
                "department":   r.get("EM_Department"),
                "em_ssn":       r.get("EM_SSN"),
            })
 
        logger.info("Prepared %d records", len(payload))
        return payload

    @task
    def upsert_batch(payload: List[Dict], target_conn_id: str) -> Dict[str, int]:
        if not payload:
            return {"attempted": 0, "succeeded": 0}

        conn = BaseHook.get_connection(target_conn_id)
        merge_sql = """
            MERGE IHS_SHARED.dbo.Employee_Master_ERP AS T
            USING (VALUES {values})
                AS S (EM_Key, EM_FirstName, EM_Department, EM_SSN)
            ON T.EM_Key = S.EM_Key
            WHEN MATCHED THEN
                UPDATE SET
                    EM_FirstName   = S.EM_FirstName,
                    EM_Department  = S.EM_Department,
                    EM_SSN         = S.EM_SSN
            WHEN NOT MATCHED THEN
                INSERT (EM_Key, EM_FirstName, EM_Department, EM_SSN)
                VALUES (S.EM_Key, S.EM_FirstName, S.EM_Department, S.EM_SSN);
        """

        stats = {"attempted": 0, "succeeded": 0}
        with pyodbc.connect(build_conn_str(conn), autocommit=False) as cnxn:
            cur = cnxn.cursor()
            for chunk in chunked(payload, CHUNK_SIZE):
                placeholders = ["(?, ?, ?, ?)"] * len(chunk)
                params = []
                for r in chunk:
                    params.extend([r["em_key"], r["em_firstname"], r["department"], r["em_ssn"]])

                sql = merge_sql.format(values=", ".join(placeholders))

                try:
                    cur.execute(sql, params)
                    cnxn.commit()
                    n = len(chunk)
                    stats["attempted"] += n
                    stats["succeeded"] += n
                except Exception as e:
                    cnxn.rollback()
                    logger.warning("Batch failed, doing row-by-row: %s", e)
                    for r in chunk:
                        try:
                            cur.execute(merge_sql.format(values="(?, ?, ?, ?)"),
                                        (r["em_key"], r["em_firstname"], r["department"], r["em_ssn"]))
                            cnxn.commit()
                            stats["succeeded"] += 1
                        except Exception as e2:
                            logger.error("Failed EM_Key %s: %s", r["em_key"], e2)
                        stats["attempted"] += 1
                gc.collect()

        logger.info("UPSERT DONE → %s | %s", target_conn_id, stats)
        return stats

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize():
        logger.info("All lines synced successfully!")
        return "SUCCESS"

    # ——————————————————————————————————
    # Orchestration
    # ——————————————————————————————————
    source_ok = check_source()
    leaf_tasks = []

    for line_conn_id in TARGET_HANGER_LANE:  # e.g. ['line-21' to 'line-29', ...]
        safe = safe_id(line_conn_id)

        with TaskGroup(group_id=f"line_{safe}", tooltip=f"Sync {line_conn_id}") as tg:
            tgt_ok = check_target(line_conn_id)
            data   = fetch_employees(line_conn_id)        # ← passes 'line-21' directly
            prep   = prepare_payload(data)
            upsert = upsert_batch(prep, line_conn_id)

            tgt_ok >> data >> prep >> upsert
            leaf_tasks.append(upsert)

        start >> source_ok >> tg

    done = finalize()
    for leaf in leaf_tasks:
        leaf >> done
    done >> end


# Run
dag = update_all_lines_employees()