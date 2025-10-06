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
from sqlalchemy import text

from scripts.constans.db_sources import SOURCE_LINE_21_22_23
from dags.hanger_lines import (
    build_mssql_conn_str,
    get_postgres_engine,
    get_last_extract_dt_from_log,
    save_to_postgres,
)

# ---------------- CONFIG ---------------- #
PKT = pendulum.timezone("Asia/Karachi")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime(2025, 10, 6, 0, 0, tzinfo=PKT),
    "start_date":datetime.now(PKT) - timedelta(minutes=10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=24),
    "catchup": False,
}

def make_result(status: str, step: str, connection_id: str, message: str) -> dict:
    """Standardize results with emoji + friendly text."""
    emoji = {"success": "✅", "fail": "❌", "skipped": "⚠️"}.get(status, "⚙️")
    return {
        "status": status,
        "step": step,
        "connection_id": connection_id,
        "message": message,
        "friendly": f"{emoji} {step.upper()} - {message}",
    }

# ---------------- DAG DEFINITION ---------------- #
@dag(
    dag_id="hanger_lines_data_21_22_23",
    default_args=default_args,
    schedule="2,12,22,32,42,52 8-23,0-1 * * 1-6",  # ✅ Every 10 min, 8AM–2AM, Mon–Sat
    tags=["ssg", "hangerline", "data", "21_22_23"],
    max_active_runs=1,
    description="ETL pipeline for Hanger lines data from MSSQL to PostgreSQL (shift 8AM–2AM)",
)
def hanger_lines_data_21_22_23():
    # ----- Base structure -----
    start = EmptyOperator(task_id="start", doc_md="### 🚀 Pipeline Start")
    end = EmptyOperator(task_id="end", doc_md="### ✅ Pipeline End")
    leaf_tasks = []

    # ---------------- TASKS ---------------- #

    @task
    def check_shift_time():
        """Skip DAG outside shift hours (08:00–02:59 PKT)."""
        now = pendulum.now("Asia/Karachi")
        hour = now.hour
        if not (hour >= 8 or hour <= 2):
            msg = f"⏸️ Skipping DAG run outside shift hours (now: {now.format('HH:mm')})"
            logger.warning(msg)
            raise AirflowSkipException(msg)
        ok = f"✅ Within shift hours ({now.format('HH:mm')})"
        logger.info(ok)
        return ok

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
            raise Exception("Source check failed")
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
            raise Exception("Target check failed")
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
                    "SELECT COUNT(*) FROM [IHS].[dbo].[ODP_Detail] WHERE created_at > ?",
                    [last_extract_dt],
                ).fetchone()[0]
                if count == 0:
                    res = make_result("skipped", step, connection_id, "No new data found")
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

    @task
    def save_data_to_postgres_task(connection_id: str, ti=None) -> dict:
        step = "save"
        try:
            msg = save_to_postgres(connection_id)
            res = make_result("success", step, connection_id, msg)
            ti.xcom_push(key=f"{connection_id}_{step}", value=res)
            return res
        except Exception as e:
            res = make_result("fail", step, connection_id, f"Save failed: {e}")
            ti.xcom_push(key=f"{connection_id}_{step}", value=res)
            return res

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize_results(conn_ids: list, ti=None) -> dict:
        summary = {"totals": {"success": 0, "fail": 0, "skipped": 0}, "details": {}}
        for cid in conn_ids:
            steps = []
            for step in ["source", "target", "data-check", "extract", "save"]:
                step_result = ti.xcom_pull(key=f"{cid}_{step}")
                if step_result:
                    steps.append(step_result)
            if not steps:
                steps = [make_result("skipped", "pipeline", cid, "No steps executed")]
            summary["details"][cid] = steps
            for s in steps:
                summary["totals"][s["status"]] += 1
        logger.info("=== ETL SUMMARY ===")
        for cid, steps in summary["details"].items():
            logger.info(f"📌 {cid}")
            for s in steps:
                logger.info(f"   {s['friendly']}")
        logger.info(f"TOTALS → {json.dumps(summary['totals'])}")
        logger.info("===================")
        ti.xcom_push(key="summary", value=summary)
        return summary

    # ---------------- BUILD PIPELINE ---------------- #
    check_time = check_shift_time()
    for conn_id in SOURCE_LINE_21_22_23:
        with TaskGroup(group_id=f"line_{conn_id}", tooltip=f"Pipeline for {conn_id}") as tg:
            src = check_source_connection.override(task_id=f"source_check_{conn_id}")(conn_id)
            tgt = check_target_connection.override(task_id=f"target_check_{conn_id}")(conn_id)
            dat = check_for_new_data.override(task_id=f"data_check_{conn_id}")(conn_id)
            ext = extract_from_source.override(task_id=f"extract_{conn_id}")(conn_id)
            sav = save_data_to_postgres_task.override(task_id=f"save_{conn_id}")(conn_id)
            src >> tgt >> dat >> ext >> sav
            leaf_tasks.append(sav)
        check_time >> tg >> end

    summary = summarize_results.override(task_id="summary")(SOURCE_LINE_21_22_23)
    leaf_tasks >> summary >> end


dag = hanger_lines_data_21_22_23()
