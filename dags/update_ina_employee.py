from __future__ import annotations
import logging, gc, sys, os, pyodbc
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pendulum
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.baseoperator import chain

# Mocking INA_DB for a runnable example
INA_DB = ['ina-db']

# ------------------------------------------------------------------
# Config
# ------------------------------------------------------------------
PKT = pendulum.timezone("Asia/Karachi")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

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
CHUNK_SIZE = 100


# ------------------------------------------------------------------
# Helper Utilities
# ------------------------------------------------------------------
def build_conn_str(conn) -> str:
    """Builds a pyodbc connection string for FreeTDS/MSSQL."""
    return (
        f"DRIVER={{FreeTDS}};"
        f"SERVER={conn.host};PORT=1433;"
        f"DATABASE={conn.schema};UID={conn.login};PWD={conn.password};"
        "TDS_Version=7.0;Connect Timeout=60;Login Timeout=60;Charset=UTF8;"
    )


def chunked(items: List[Any], size: int):
    """Yield successive n-sized chunks."""
    for i in range(0, len(items), size):
        yield items[i:i + size]


def safe_id(name: str) -> str:
    """Converts a string to a safe Airflow ID."""
    return "".join(c if c.isalnum() else "_" for c in name)


# ------------------------------------------------------------------
# DAG
# ------------------------------------------------------------------
@dag(
    dag_id="update_ina_employee",
    default_args=default_args,
    schedule="*/30 8-23,0-1 * * 1-6",
    tags=["ssg", "hangerline", "employee", "ina"],
    max_active_runs=1,
)
def update_erp_ina_employee_corrected():

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ---------------- SOURCE CHECK ---------------- #
    @task(retries=2, retry_delay=timedelta(seconds=10))
    def check_source() -> str:
        """Check MSSQL Source availability."""
        conn = BaseHook.get_connection(SOURCE_CONN_ID)
        try:
            with pyodbc.connect(build_conn_str(conn), timeout=20) as cnxn:
                cnxn.execute("SELECT 1")
            msg = f"✅ Source '{SOURCE_CONN_ID}' reachable"
            logger.info(msg)
            return msg
        except Exception as e:
            logger.error("Source check failed for '%s': %s", SOURCE_CONN_ID, e)
            raise

    # ---------------- TARGET CHECK ---------------- #
    @task(retries=2, retry_delay=timedelta(seconds=10))
    def check_target(conn_id: str) -> str:
        """Check individual target DB availability."""
        conn = BaseHook.get_connection(conn_id)
        try:
            with pyodbc.connect(build_conn_str(conn), timeout=20) as cnxn:
                cnxn.execute("SELECT 1")
                msg = f"✅ Target '{conn_id}' reachable"
                logger.info(msg)
                return msg
        except Exception as e:
            logger.error("Target check failed for '%s': %s", conn_id, e)
            raise

    # ---------------- FETCH EMPLOYEES ---------------- #
    @task
    def fetch_employees(line_conn_id: str) -> List[Dict[str, Any]]:
        """Fetch employee data from MSSQL for specific line."""
        conn = BaseHook.get_connection(SOURCE_CONN_ID)
        conn_str = build_conn_str(conn)

        sql = """
            SELECT 
                INA_ID AS EM_Key,
                Title AS EM_FirstName,
                Latest_Line_ID AS EM_Department,
                ID AS EM_SSN
            FROM dbo.hangerline_emp
        """

        rows = []
        try:
            with pyodbc.connect(conn_str, timeout=60) as cnxn:
                cursor = cnxn.cursor()
                cursor.execute(sql)
                if cursor.description:
                    cols = [col[0] for col in cursor.description]
                    rows = [dict(zip(cols, row)) for row in cursor.fetchall()]
        except Exception as e:
            logger.error("Line %s: Failed fetching employees: %s", line_conn_id, e)
            raise

        logger.info("Line %s: fetched %d employees", line_conn_id, len(rows))
        return rows

    # ---------------- PREPARE PAYLOAD ---------------- #
    @task
    def prepare_payload(rows: List[Dict], line_conn_id: str) -> Dict[str, Any]:
        """Validates & prepares employee payload."""
        payload, bad = [], 0
        for r in rows:
            try:
                em_key = int(r["EM_Key"])
                payload.append({
                    "em_key": em_key,
                    "em_firstname": r.get("EM_FirstName"),
                    "department": r.get("EM_Department"),
                    "em_ssn": r.get("EM_SSN"),
                })
            except (TypeError, ValueError):
                bad += 1
                logger.warning("Line %s: Bad EM_Key: %s", line_conn_id, r.get("EM_Key"))

        logger.info("Line %s: %d valid, %d invalid", line_conn_id, len(payload), bad)
        return {"payload": payload, "fetched": len(rows), "bad": bad}


    # ---------------- UPSERT TO TARGET (WITH CHUNKS) ---------------- #
    @task
    def upsert_batch(prep: Dict[str, Any], target_conn_id: str) -> Dict[str, Any]:
        """
        Upserts data in chunks using conditional UPDATE/INSERT logic with autocommit.
        This ensures that the upsert is handled efficiently and logs each chunk processed.
        """
        payload = prep["payload"]
        stats = {"attempted": 0, "succeeded": 0, "failed": 0, "target": target_conn_id}

        if not payload:
            logger.info("Line %s: No payload to upsert.", target_conn_id)
            return {**stats, **prep}

        conn = BaseHook.get_connection(target_conn_id)
        
        # SQL to check for existence and perform UPDATE or INSERT
        upsert_sql = """
            IF EXISTS (SELECT 1 FROM lnk_svr.IHS_SHARED.dbo.Employee_Master WHERE EM_Key = ?)
                UPDATE lnk_svr.IHS_SHARED.dbo.Employee_Master
                SET EM_FirstName = ?, EM_Department = ?, EM_SSN = ?
                WHERE EM_Key = ?
            ELSE
                INSERT INTO lnk_svr.IHS_SHARED.dbo.Employee_Master (EM_Key, EM_FirstName, EM_Department, EM_SSN)
                VALUES (?, ?, ?, ?)
        """

        # Process the payload in chunks
        try:
            with pyodbc.connect(build_conn_str(conn), autocommit=True) as cnxn:
                cur = cnxn.cursor()
                
                # Chunk the payload into smaller chunks of size CHUNK_SIZE
                for chunk in chunked(payload, CHUNK_SIZE):
                    for record in chunk:
                        stats["attempted"] += 1
                        
                        # Parameters for the conditional upsert
                        params = [
                            record["em_key"],          # Check condition EM_Key
                            record["em_firstname"],    # Update EM_FirstName
                            record["department"],      # Update EM_Department
                            record["em_ssn"],          # Update EM_SSN
                            record["em_key"],          # Update WHERE EM_Key
                            record["em_key"],          # Insert EM_Key
                            record["em_firstname"],    # Insert EM_FirstName
                            record["department"],      # Insert EM_Department
                            record["em_ssn"]           # Insert EM_SSN
                        ]
                        
                        try:
                            cur.execute(upsert_sql, params)
                            stats["succeeded"] += 1
                        except Exception as e:
                            stats["failed"] += 1
                            logger.error("Line %s: Failed to upsert employee EM_Key %s. Error: %s", target_conn_id, record["em_key"], e)

                    # Commit after processing each chunk
                    cnxn.commit()
                    logger.info("Line %s: Processed chunk of %d records.", target_conn_id, len(chunk))
                    gc.collect()

        except Exception as e_conn:
            logger.error("Line %s: Connection or overall upsert failed: %s", target_conn_id, e_conn)
            raise

        logger.info("Line %s: Upsert stats: %s", target_conn_id, stats)
        return {**stats, **prep}

    # ---------------- SUMMARIZE ---------------- #
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def analyze_and_summarize(ti=None):
        """Aggregate XCom stats across all lines."""
        all_stats = [
            ti.xcom_pull(task_ids=f"line_{safe_id(line)}.upsert_batch", key="return_value")
            for line in INA_DB
        ]
        all_stats = [s for s in all_stats if s]

        if not all_stats:
            logger.warning("No stats collected.")
            return "No data processed."

        totals = {
            "fetched": sum(s.get("fetched", 0) for s in all_stats),
            "bad": sum(s.get("bad", 0) for s in all_stats),
            "attempted": sum(s.get("attempted", 0) for s in all_stats),
            "succeeded": sum(s.get("succeeded", 0) for s in all_stats),
            "failed": sum(s.get("failed", 0) for s in all_stats),
        }

        report = [
            f"📊 ETL Summary @ {datetime.now(PKT).strftime('%Y-%m-%d %H:%M:%S %Z')}",
            f"Total Lines: {len(INA_DB)}",
            f"Records Fetched: {totals['fetched']}",
            f"Invalid Records: {totals['bad']}",
            f"Upsert Attempted: {totals['attempted']}",
            f"Upsert Succeeded: {totals['succeeded']}",
            f"Upsert Failed: {totals['failed']}",
            "",
            "--- Detailed Line Breakdown ---",
            "| Line ID | Fetched | Invalid | Attempted | Succeeded | Failed |",
            "| :--- | :---: | :---: | :---: | :---: | :---: |"
        ]
        
        for s in all_stats:
            line_id = s.get("target", "N/A")
            fetched = s.get("fetched", 0)
            bad = s.get("bad", 0)
            attempted = s.get("attempted", 0)
            succeeded = s.get("succeeded", 0)
            failed = s.get("failed", 0)
            report.append(f"| {line_id} | {fetched} | {bad} | {attempted} | {succeeded} | {failed} |")

        summary = "\n".join(report)
        logger.info("Final Summary:\n%s", summary)
        
        # Push the final summary to XCom for external consumption (e.g., email)
        ti.xcom_push(key='final_etl_summary', value=summary)
        
        return summary

    # ------------------------------------------------------------------
    # Orchestration
    # ------------------------------------------------------------------
    src_ok = check_source()
    leaf_tasks = []

    for line_conn_id in INA_DB:
        safe = safe_id(line_conn_id)
        with TaskGroup(group_id=f"line_{safe}", tooltip=f"Process {line_conn_id}") as tg:
            tgt_ok = check_target(line_conn_id)
            data = fetch_employees(line_conn_id)
            prep = prepare_payload(data, line_conn_id)
            upsert = upsert_batch(prep, line_conn_id)
            chain(tgt_ok, data, prep, upsert)
            leaf_tasks.append(upsert)
        start >> src_ok >> tg

    summary = analyze_and_summarize()
    for leaf in leaf_tasks:
        leaf >> summary
    summary >> end


dag = update_erp_ina_employee_corrected()
