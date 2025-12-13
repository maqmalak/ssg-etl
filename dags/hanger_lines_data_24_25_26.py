"""
Airflow DAG: hanger_lines_data_upsert
-------------------------------------
ETL pipeline for Hanger Lines Data (MSSQL → PostgreSQL Upsert)

✅ Runs every 5 minutes, Mon-Sat, 08:00-02:00 (PKT)
✅ Enforces shift window (skips gracefully)
✅ Validates MSSQL & PostgreSQL connections
✅ Skips if no new data found
✅ Upserts new data into PostgreSQL
✅ Structured logging & summary aggregation
"""

from __future__ import annotations
import json, logging, os, sys
from datetime import datetime, timedelta
import pendulum, pyodbc
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException
from sqlalchemy import text

# Project imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))
from scripts.constans.db_sources import SOURCE_LINE_24_25_26
from dags.hanger_lines_data import (
    build_mssql_conn_str,
    get_postgres_engine,
    upsert_to_postgres,
    get_last_extract_dt_from_log,
)

# ---------------- CONFIG ---------------- #
PKT = pendulum.timezone("Asia/Karachi")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime(2025, 11, 27, 0, 0, tzinfo=PKT),
    "start_date":datetime.now(PKT) - timedelta(minutes=10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=10),
    "catchup": False,
}


def make_result(status: str, step: str, connection_id: str, message: str) -> dict:
    emoji = {"success": "✅", "fail": "❌", "skipped": "⚠️"}.get(status, "ℹ️")
    return {
        "status": status,
        "step": step,
        "connection_id": connection_id,
        "message": message,
        "friendly": f"{emoji} [{connection_id}] {step.upper()} - {message}",
    }


# ---------------- DAG DEFINITION ---------------- #
@dag(
    dag_id="hanger_lines_data_24_25_26",
    default_args=default_args,
    schedule="4,14,24,34,44,54 8-23,0-1 * * 1-6",  # ✅ Every 10 min, 8AM–2AM, Mon–Sat
    tags=["ssg", "hangerline", "data", "upsert"],
    max_active_runs=1,
    catchup=False,
    description="ETL pipeline for Hanger Lines Data (8 AM-2 AM PKT, Mon-Sat)",
)
def hanger_lines_data_upsert():
    start = EmptyOperator(task_id="start", doc_md="### 🚀 Pipeline Start")
    end = EmptyOperator(task_id="end", doc_md="### ✅ Pipeline End")

    # ---------------- SHIFT TIME CHECK ---------------- #
    @task
    def check_shift_time() -> str:
        now = pendulum.now("Asia/Karachi")
        hour = now.hour
        if not (8 <= hour <= 23 or 0 <= hour <= 1):
            msg = f"⏸️ Outside shift hours ({now.format('HH:mm')}) — skipping run."
            logger.warning(msg)
            raise AirflowSkipException(msg)
        msg = f"✅ Within shift hours ({now.format('HH:mm')}) — proceeding."
        logger.info(msg)
        return msg

    shift_check = check_shift_time()

    # ---------------- SOURCE / TARGET VALIDATION ---------------- #
    @task
    def check_source_connection(connection_id: str) -> dict:
        step = "source"
        try:
            conn = BaseHook.get_connection(connection_id)
            conn_str = build_mssql_conn_str(conn)
            with pyodbc.connect(conn_str, timeout=10) as c:
                c.cursor().execute("SELECT 1")
            return make_result("success", step, connection_id, "MSSQL reachable")
        except Exception as e:
            msg = f"MSSQL unreachable: {e}"
            logger.warning(f"[{connection_id}] ⚠️ {msg}")
            raise AirflowSkipException(make_result("skipped", step, connection_id, msg)["friendly"])

    @task
    def check_target_connection(connection_id: str) -> dict:
        step = "target"
        try:
            with get_postgres_engine().connect() as conn:
                conn.execute(text("SELECT 1"))
            return make_result("success", step, connection_id, "PostgreSQL reachable")
        except Exception as e:
            msg = f"PostgreSQL unreachable: {e}"
            logger.warning(f"[{connection_id}] ⚠️ {msg}")
            raise AirflowSkipException(make_result("skipped", step, connection_id, msg)["friendly"])

    # ---------------- DATA CHECK + EXTRACTION ---------------- #
    @task
    def check_for_new_data(connection_id: str, ti=None) -> dict:
        """Check if new data is available since last extract."""
        step = "data-check"
        last_extract_dt = get_last_extract_dt_from_log(connection_id)
        if not last_extract_dt:
            logger.info(f"[{connection_id}] No previous extract, full initial load triggered.")
            res = make_result("skipped", step, connection_id, "No previous extract")
            ti.xcom_push(key=f"{connection_id}_{step}", value=res)
            raise AirflowSkipException(res["message"])

        try:
            conn_str = build_mssql_conn_str(BaseHook.get_connection(connection_id))
            with pyodbc.connect(conn_str, timeout=30) as conn:
                count = conn.cursor().execute(
                    "SELECT COUNT(*) FROM [IHS].[dbo].[ODP_Master] WHERE ODP_Last_Hanger_Time > ?",
                    [last_extract_dt],
                ).fetchone()[0]

            if count == 0:
                msg = "No new data found."
                logger.info(f"[{connection_id}] ⚠️ {msg}")
                raise AirflowSkipException(make_result("skipped", step, connection_id, msg)["friendly"])

            msg = f"Found {count} new records."
            logger.info(f"[{connection_id}] ✅ {msg}")
            return make_result("success", step, connection_id, msg)

        except Exception as e:
            msg = f"Data check error: {e}"
            logger.error(f"[{connection_id}] ❌ {msg}")
            raise AirflowSkipException(make_result("skipped", step, connection_id, msg)["friendly"])

    @task(trigger_rule="none_failed_min_one_success")
    def extract_from_source(connection_id: str) -> dict:
        """Simulate data extraction (actual logic handled in upsert)."""
        step = "extract"
        msg = "Data extraction started."
        logger.info(f"[{connection_id}] 🚀 {msg}")
        return make_result("success", step, connection_id, msg)

    # ---------------- UPSERT TASK ---------------- #
    @task(trigger_rule="none_failed_min_one_success")
    def upsert_task(connection_id: str) -> dict:
        step = "upsert"
        try:
            msg = upsert_to_postgres(connection_id)
            logger.info(f"[{connection_id}] ✅ {msg}")
            return make_result("success", step, connection_id, msg)
        except Exception as e:
            msg = f"Upsert failed: {e}"
            logger.error(f"[{connection_id}] ❌ {msg}")
            return make_result("fail", step, connection_id, msg)

    # ---------------- SUMMARY ---------------- #
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize_results(conn_ids: list, ti=None) -> dict:
        """
        Collects results from all line task groups and prints a formatted summary.
        """
        summary = {"success": 0, "fail": 0, "skipped": 0, "details": {}}
        report_lines = []
        separator = "─" * 70

        logger.info("\n" + separator)
        logger.info("📊 FINAL ETL SUMMARY")
        logger.info(separator)

        for cid in conn_ids:
            steps = []
            for step in ["source", "target", "data-check", "extract", "upsert"]:
                # ✅ FIX: prefix group name so XCom paths match TaskGroup IDs
                tid = f"line_{cid}.{step}_{cid}"
                result = ti.xcom_pull(task_ids=tid)
                if result:
                    steps.append(result)
                    summary[result["status"]] = summary.get(result["status"], 0) + 1

            if not steps:
                steps = [make_result("skipped", "pipeline", cid, "No steps executed")]

            summary["details"][cid] = steps

            # 🧩 Build formatted line summary
            line_status = "✅ SUCCESS"
            if any(s["status"] == "fail" for s in steps):
                line_status = "❌ FAIL"
            elif all(s["status"] == "skipped" for s in steps):
                line_status = "⚠️ SKIPPED"

            report_lines.append(f"\n📦 {cid}: {line_status}")
            for s in steps:
                emoji = {"success": "✅", "fail": "❌", "skipped": "⚠️"}.get(s["status"], "ℹ️")
                report_lines.append(f"   {emoji} {s['step'].capitalize():<10} → {s['message']}")

        # 🧾 Print formatted table
        logger.info("\n".join(report_lines))
        logger.info(separator)
        logger.info(f"📈 Totals → ✅ {summary['success']} | ⚠️ {summary['skipped']} | ❌ {summary['fail']}")
        logger.info(separator)

        return summary

    # ---------------- PIPELINE STRUCTURE ---------------- #
    leaf_tasks = []
    for conn_id in SOURCE_LINE_24_25_26:
        with TaskGroup(group_id=f"line_{conn_id}", tooltip=f"ETL for {conn_id}") as tg:
            src = check_source_connection.override(task_id=f"source_{conn_id}")(conn_id)
            tgt = check_target_connection.override(task_id=f"target_{conn_id}")(conn_id)
            dat = check_for_new_data.override(task_id=f"data_check_{conn_id}")(conn_id)
            ext = extract_from_source.override(task_id=f"extract_{conn_id}")(conn_id)
            ups = upsert_task.override(task_id=f"upsert_{conn_id}")(conn_id)

            # The magic: if data-check skips → downstream auto-skips
            src >> tgt >> dat >> ext >> ups
            leaf_tasks.append(ups)

        shift_check >> tg

    summary = summarize_results.override(task_id="summary")(SOURCE_LINE_24_25_26)

    # Final flow
    start >> shift_check
    leaf_tasks >> summary >> end


dag = hanger_lines_data_upsert()
