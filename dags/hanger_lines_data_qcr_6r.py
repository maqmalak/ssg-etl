from __future__ import annotations

import logging, json, sys, os
from datetime import datetime, timedelta

sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pendulum, pyodbc
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowSkipException
from pendulum import timezone
from sqlalchemy import text

# from scripts.constans.db_sources import SOURCE_HANGER_LANE
SOURCE_HANGER_LANE = [
    'ina-db-6r'
    # 'line-21',
    # 'line-22',
    # 'line-23',
    # 'line-24',
    # 'line-25',
    # 'line-26',
    # 'line-27'
    # 'line-28',
    # 'line-29'
]


from dags.hanger_lines_qcr_6r import (
    build_mssql_conn_str,
    get_postgres_engine,
    get_last_extract_dt_from_log,
    qcr_upsert_to_postgres
)

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

PKT = timezone("Asia/Karachi")

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime(2025, 1, 1, 0, 0, tzinfo=PKT),
    "start_date":datetime.now(PKT) - timedelta(minutes=10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30),
    "catchup": False,
}

def make_result(status: str, step: str, connection_id: str, message: str) -> dict:
    """Standardize results with emoji + friendly text."""
    if status == "success":
        friendly = f"✅ {step.upper()} SUCCESS - {message}"
    elif status == "fail":
        friendly = f"❌ {step.upper()} FAIL - {message}"
    else:
        friendly = f"⚠️ {step.upper()} SKIPPED - {message}"
    return {
        "status": status,
        "step": step,
        "connection_id": connection_id,
        "message": message,
        "friendly": friendly,
    }

@dag(
    dag_id="hanger_lines_data_qcr_6r",
    default_args=default_args,
    # schedule=timedelta(minutes=5),
    schedule="*/9 8-23,0-1 * * 1-6",  # ✅ Every 9 min, 8AM–2AM, Mon–Sat
    tags=["ssg", "hangerline", "data", "qcr"],
    max_active_runs=1,
    description="ETL pipeline for Hanger lines data from MSSQL to PostgreSQL",
)
def hanger_lines_data_qcr():
    start = EmptyOperator(task_id="start", doc_md="### 🚀 Pipeline Start")
    end = EmptyOperator(task_id="end", doc_md="### ✅ Pipeline End")

    leaf_tasks = []

    # ---------------- TASKS ---------------- #

    @task
    def check_source_connection(connection_id: str, ti=None) -> dict:
        step = "source"
        try:
            conn_str = build_mssql_conn_str(BaseHook.get_connection(connection_id))
            with pyodbc.connect(conn_str, timeout=30) as conn:
                if conn.cursor().execute("SELECT 1").fetchone():
                    res = make_result("success", step, connection_id, "Source reachable")
                    ti.xcom_push(key=f"{connection_id}_{step}", value=res)
                    return res
            res = make_result("skipped", step, connection_id, "Source check failed")
            ti.xcom_push(key=f"{connection_id}_{step}", value=res)
            raise AirflowSkipException(res["message"])
        except Exception as e:
            res = make_result("skipped", step, connection_id, f"Source error: {e}")
            ti.xcom_push(key=f"{connection_id}_{step}", value=res)
            raise AirflowSkipException(res["message"])

    @task
    def check_target_connection(connection_id: str, ti=None) -> dict:
        step = "target"
        try:
            with get_postgres_engine().connect() as conn:
                if conn.execute(text("SELECT 1")).fetchone():
                    res = make_result("success", step, connection_id, "Target reachable")
                    ti.xcom_push(key=f"{connection_id}_{step}", value=res)
                    return res
            res = make_result("skipped", step, connection_id, "Target check failed")
            ti.xcom_push(key=f"{connection_id}_{step}", value=res)
            raise AirflowSkipException(res["message"])
        except Exception as e:
            res = make_result("skipped", step, connection_id, f"Target error: {e}")
            ti.xcom_push(key=f"{connection_id}_{step}", value=res)
            raise AirflowSkipException(res["message"])

    @task
    def check_for_new_data(connection_id: str, ti=None) -> dict:
        step = "data-check"
        last_extract_dt = get_last_extract_dt_from_log(connection_id)
        if not last_extract_dt:
            res = make_result("skipped", step, connection_id, "No previous extract")
            ti.xcom_push(key=f"{connection_id}_{step}", value=res)
            raise AirflowSkipException(res["message"])

        try:
            conn_str = build_mssql_conn_str(BaseHook.get_connection(connection_id))
            with pyodbc.connect(conn_str, timeout=30) as conn:
                count = conn.cursor().execute(
                    "SELECT COUNT(*) FROM IHS_SHARED.dbo.QC_Rework WHERE QCR_Defect_DateTime > ?",
                    [last_extract_dt],
                ).fetchone()[0]
                if count == 0:
                    res = make_result("skipped", step, connection_id, "No new data")
                    ti.xcom_push(key=f"{connection_id}_{step}", value=res)
                    raise AirflowSkipException(res["message"])
                res = make_result("success", step, connection_id, f"Found {count} new records")
                ti.xcom_push(key=f"{connection_id}_{step}", value=res)
                return res
        except Exception as e:
            res = make_result("skipped", step, connection_id, f"Data check error: {e}")
            ti.xcom_push(key=f"{connection_id}_{step}", value=res)
            raise AirflowSkipException(res["message"])

    @task
    def extract_from_source(connection_id: str, ti=None) -> dict:
        step = "extract"
        res = make_result("success", step, connection_id, "Extraction started")
        ti.xcom_push(key=f"{connection_id}_{step}", value=res)
        return res

    # ---------------- UPSERT TASK ---------------- #
    @task(trigger_rule="none_failed_min_one_success")
    def upsert_task(connection_id: str) -> dict:
        step = "upsert"
        try:
            msg = qcr_upsert_to_postgres(connection_id)
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

    # ---------------- DYNAMIC BUILD with GROUPS ---------------- #
    for conn_id in SOURCE_HANGER_LANE:
        with TaskGroup(group_id=f"line_{conn_id}", tooltip=f"Pipeline for {conn_id}") as tg:
            src = check_source_connection.override(task_id=f"source_check_{conn_id}")(conn_id)
            tgt = check_target_connection.override(task_id=f"target_check_{conn_id}")(conn_id)
            dat = check_for_new_data.override(task_id=f"data_check_{conn_id}")(conn_id)
            ext = extract_from_source.override(task_id=f"extract_{conn_id}")(conn_id)
            # sav = save_data_to_postgres_task.override(task_id=f"save_{conn_id}")(conn_id)
            ups = upsert_task.override(task_id=f"upsert_{conn_id}")(conn_id)
            src >> tgt >> dat >> ext >> ups
            leaf_tasks.append(ups)

        start >> tg >> end

    summary = summarize_results.override(task_id="summary")(SOURCE_HANGER_LANE)
    leaf_tasks >> summary >> end

dag = hanger_lines_data_qcr()
