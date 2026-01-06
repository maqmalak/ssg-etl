"""
ETL DAG: SilverStr → All Hanger Lines – Employee Master Upsert (by EM_Key)
Clean, reliable, production-ready version with TaskGroups per line.
"""
from __future__ import annotations

import logging
import gc
import re
from datetime import datetime, timedelta
from typing import List, Dict, Any

import pendulum
import pyodbc
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

# Your project constant: list of target line connection IDs (e.g. ['line_21', 'line_22', ...])
TARGET_HANGER_LANE = [
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

# ------------------------------------------------------------------
# Configuration
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
CHUNK_SIZE = 50  # Reduced to avoid SQL Server 2100 parameter limit (500 * 4 = 2000 params)


# ------------------------------------------------------------------
# Helpers
# ------------------------------------------------------------------
def build_conn_str(conn) -> str:
    return (
        f"DRIVER={{FreeTDS}};"
        f"SERVER={conn.host};PORT=1433;"
        f"DATABASE={conn.schema};"
        f"UID={conn.login};PWD={conn.password};"
        f"TDS_Version=7.0;Connect Timeout=60;Login Timeout=60;"
    )


def chunked(items: List[Any], size: int):
    """Yield successive n-sized chunks from list."""
    for i in range(0, len(items), size):
        yield items[i:i + size]


def safe_id(name: str) -> str:
    return re.sub(r"[^A-Za-z0-9_]", "_", name)

# ---------------------------------------------------------------- #
# DB UTILITIES
# ---------------------------------------------------------------- #
def build_mssql_conn_str(conn):
    return (
        f"DRIVER={{FreeTDS}};SERVER={conn.host};PORT=1433;"
        f"DATABASE={conn.schema};UID={conn.login};PWD={conn.password};"
        "TDS_Version=7.0;Connect Timeout=60;Login Timeout=60;"
    )
# ------------------------------------------------------------------
# DAG
# ------------------------------------------------------------------
@dag(
    dag_id="update_all_lines_employees",
    default_args=default_args,
    schedule="*/60 8-23,0-1 * * 1-6",  # Every 60 mins during shift hours
    tags=["ssg", "hangerline", "employee", "master"],
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
        logger.info("Source DB '%s' is reachable", SOURCE_CONN_ID)

    @task
    def check_target(conn_id: str):
        conn = BaseHook.get_connection(conn_id)
        with pyodbc.connect(build_conn_str(conn), timeout=30) as cnxn:
            cnxn.execute("SELECT 1")
        logger.info("Target DB '%s' is reachable", conn_id)

    @task
    def fetch_employees(line_conn_id: str) -> List[Dict[str, Any]]:
        conn = BaseHook.get_connection(SOURCE_CONN_ID)
        conn_str = build_mssql_conn_str(conn)

        # Normalize: allow callers to pass quoted values like "'line-21'" or '"line-21"'
        line_conn_id_norm = line_conn_id.strip()
        if (line_conn_id_norm.startswith("'") and line_conn_id_norm.endswith("'")) or \
           (line_conn_id_norm.startswith('"') and line_conn_id_norm.endswith('"')):
            line_conn_id_norm = line_conn_id_norm[1:-1]
            logger.debug("Stripped surrounding quotes from line_conn_id -> %s", line_conn_id_norm)

        # Extract line description from connection ID if it's in 'line-XX' format
        # This handles cases where the database might store line numbers differently
        if line_conn_id_norm.startswith('line-'):
            line_desc = line_conn_id_norm[5:]  # Remove 'line-' prefix, keep only the number
        else:
            line_desc = line_conn_id_norm

        logger.info("Starting fetch from '%s' for line '%s' (mapped from conn_id: %s)", SOURCE_CONN_ID, line_desc, line_conn_id_norm)

        sql = """
            SELECT
                ID AS EM_Key,
                Title AS EM_FirstName,
                FatherName AS EM_LastName,
                Line_Desc AS EM_Department,
                EMP_ID AS EM_SSN,
                ActiveStatus,
                joindate AS EM_JoinDate,
                ResignDate AS EM_ResignDate,
                NIC as EM_NIC,
                Gender AS EM_Gender
            FROM dbo.hangerline_emp
            WHERE ID IS NOT NULL
        """

        with pyodbc.connect(conn_str, timeout=30) as cnxn:
            cursor = cnxn.cursor()
            try:
                # pass single parameter as 1-tuple
                cursor.execute(sql)
                logger.debug("Executed SQL: %s -- params: %s", sql.strip(), line_desc)
            except Exception as e:
                logger.error("Error executing query for line '%s' (mapped from conn_id: %s): %s", line_desc, line_conn_id_norm, e)
                raise

            if cursor.description is None:
                rows = []
            else:
                cols = [col[0] for col in cursor.description]
                rows = [dict(zip(cols, row)) for row in cursor.fetchall()]

        logger.info("Fetched %d employees for line: %s (mapped from conn_id: %s)", len(rows), line_desc, line_conn_id_norm)
        return rows




    @task
    def prepare_payload(rows: List[Dict]) -> List[Dict]:
        payload = []
        for r in rows:
            try:
                em_key = int(r["EM_Key"])
            except (TypeError, ValueError):
                logger.warning("Invalid EM_Key, skipping: %s", r)
                continue

            payload.append({
                "em_key": em_key,
                "em_firstname": r.get("EM_FirstName"),
                "em_lastname": r.get("EM_LastName"),
                "department": r.get("EM_Department"),
                "em_ssn": r.get("EM_SSN"),
                "em_nic": r.get("EM_NIC"),
                "em_joindate": r.get("EM_JoinDate"),
                "em_resigndate": r.get("EM_ResignDate"),
                "em_gender": r.get("EM_Gender"),
                "em_activestatus": r.get("ActiveStatus"),

            })
        logger.info("Prepared %d valid records for upsert", len(payload))
        return payload

    @task
    def upsert_batch(payload: List[Dict], target_conn_id: str) -> Dict[str, int]:
        if not payload:
            return {"attempted": 0, "succeeded": 0}

        conn = BaseHook.get_connection(target_conn_id)
        merge_sql = """
            MERGE IHS_SHARED.dbo.Employee_Master_ERP AS T
            USING (VALUES {values}) 
                AS S (EM_Key, EM_FirstName, EM_LastName, EM_Department, EM_SSN, EM_NIC, EM_DateHired, EM_TerminationDate, EM_Gender, EM_ActiveStatus)
            ON T.EM_Key = S.EM_Key
            WHEN MATCHED THEN
                UPDATE SET 
                    EM_FirstName = S.EM_FirstName,
                    EM_LastName = S.EM_LastName,
                    EM_Department = S.EM_Department,
                    EM_SSN = S.EM_SSN,
                    EM_ID = S.EM_NIC,
                    EM_DateHired = S.EM_DateHired,
                    EM_TerminationDate = S.EM_TerminationDate,
                    EM_Sex = S.EM_Gender,
                    EM_Resigned = CASE WHEN S.EM_ActiveStatus='1' THEN 0 ELSE 1 END

            WHEN NOT MATCHED THEN
                INSERT (EM_Key, EM_FirstName, EM_LastName, EM_Department, EM_SSN, EM_ID, EM_DateHired, EM_TerminationDate,EM_Resigned,EM_Sex)
                VALUES (S.EM_Key, S.EM_FirstName, S.EM_LastName, S.EM_Department, S.EM_SSN, S.EM_NIC, S.EM_DateHired, S.EM_TerminationDate, S.EM_ActiveStatus, S.EM_Gender);
        """

        stats = {"attempted": 0, "succeeded": 0}
        with pyodbc.connect(build_conn_str(conn), autocommit=False) as cnxn:
            cur = cnxn.cursor()
            for chunk in chunked(payload, CHUNK_SIZE):
                values = []
                params = []
                for rec in chunk:
                    values.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                    params.extend([
                        rec["em_key"],
                        rec["em_firstname"],
                        rec["em_lastname"],
                        rec["department"],
                        rec["em_ssn"],
                        rec["em_nic"],
                        rec["em_joindate"],
                        rec["em_resigndate"],
                        rec["em_gender"],
                        rec["em_activestatus"]
                    ])

                sql = merge_sql.format(values=", ".join(values))

                try:
                    cur.execute(sql, params)
                    cnxn.commit()
                    n = len(chunk)
                    stats["attempted"] += n
                    stats["succeeded"] += n
                    logger.info("Upserted %d rows → %s", n, target_conn_id)
                except Exception as e:
                    cnxn.rollback()
                    logger.error("Batch failed (%s), falling back to single rows", e)
                    # Fallback: row-by-row
                    for rec in chunk:
                        try:
                            single_sql = merge_sql.format(values="(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                            cur.execute(single_sql, (
                                rec["em_key"], rec["em_firstname"],
                                rec["em_lastname"],
                                rec["department"], rec["em_ssn"],
                                rec["em_nic"], rec["em_joindate"],
                                rec["em_resigndate"], rec["em_gender"],
                                rec["em_activestatus"],
                       
                                rec
                            ))
                            cnxn.commit()
                            stats["succeeded"] += 1
                        except Exception as e2:
                            logger.error("Failed EM_Key=%s: %s", rec["em_key"], e2)
                        stats["attempted"] += 1
                gc.collect()

        logger.info("Final: %s → attempted=%d succeeded=%d", target_conn_id,
                    stats["attempted"], stats["succeeded"])
        return stats

    @task
    def flatten_employee_lists(employee_lists: List[List[Dict]]) -> List[Dict]:
        """Flatten list of employee lists into single list"""
        all_employees = []
        for employee_list in employee_lists:
            all_employees.extend(employee_list)
        logger.info("Flattened %d employee lists into %d total employees",
                   len(employee_lists), len(all_employees))
        return all_employees

    @task
    def group_employees_by_department(payload: List[Dict]) -> Dict[str, List[Dict]]:
        """Group employees by EM_Department"""
        department_groups = {}
        for employee in payload:
            dept = employee.get("department") or "Unknown"
            if dept not in department_groups:
                department_groups[dept] = []
            department_groups[dept].append(employee)

        logger.info("Grouped employees into %d departments: %s",
                   len(department_groups), list(department_groups.keys()))
        return department_groups

    @task
    def process_departments_by_group(dept_groups: Dict[str, List[Dict]], target_conn_id: str):
        """Process all departments and return summaries"""
        department_summaries = []

        # Get database connection for upsert operations
        conn = BaseHook.get_connection(target_conn_id)
        merge_sql = """
            MERGE IHS_SHARED.dbo.Employee_Master_ERP AS T
            USING (VALUES {values})
                AS S (EM_Key, EM_FirstName, EM_LastName, EM_Department, EM_SSN, EM_NIC, EM_DateHired, EM_TerminationDate, EM_Gender, EM_ActiveStatus)
            ON T.EM_Key = S.EM_Key
            WHEN MATCHED THEN
                UPDATE SET
                    EM_FirstName = S.EM_FirstName,
                    EM_LastName = S.EM_LastName,
                    EM_Department = S.EM_Department,
                    EM_SSN = S.EM_SSN,
                    EM_ID = S.EM_NIC,
                    EM_DateHired = S.EM_DateHired,
                    EM_TerminationDate = S.EM_TerminationDate,
                    EM_Sex = S.EM_Gender,
                    EM_Resigned = S.EM_ActiveStatus

            WHEN NOT MATCHED THEN
                INSERT (EM_Key, EM_FirstName, EM_LastName, EM_Department, EM_SSN, EM_ID, EM_DateHired, EM_TerminationDate,EM_Resigned,EM_Sex)
                VALUES (S.EM_Key, S.EM_FirstName, S.EM_LastName, S.EM_Department, S.EM_SSN, S.EM_NIC, S.EM_DateHired, S.EM_TerminationDate, S.EM_ActiveStatus, S.EM_Gender);
        """

        for dept_name, dept_employees in dept_groups.items():
            if not dept_employees:
                continue

            # Perform upsert directly in this task
            stats = {"attempted": 0, "succeeded": 0}

            with pyodbc.connect(build_conn_str(conn), autocommit=False) as cnxn:
                cur = cnxn.cursor()
                for chunk in chunked(dept_employees, CHUNK_SIZE):
                    values = []
                    params = []
                    for rec in chunk:
                        values.append("(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                        params.extend([
                            rec["em_key"],
                            rec["em_firstname"],
                            rec["em_lastname"],
                            rec["department"],
                            rec["em_ssn"],
                            rec["em_nic"],
                            rec["em_joindate"],
                            rec["em_resigndate"],
                            rec["em_gender"],
                            rec["em_activestatus"]
                        ])

                    sql = merge_sql.format(values=", ".join(values))

                    try:
                        cur.execute(sql, params)
                        cnxn.commit()
                        n = len(chunk)
                        stats["attempted"] += n
                        stats["succeeded"] += n
                        logger.info("Upserted %d rows for department %s", n, dept_name)
                    except Exception as e:
                        cnxn.rollback()
                        logger.error("Batch failed for department %s: %s", dept_name, e)
                        # Fallback: row-by-row
                        for rec in chunk:
                            try:
                                single_sql = merge_sql.format(values="(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                                cur.execute(single_sql, (
                                    rec["em_key"], rec["em_firstname"],
                                    rec["em_lastname"],
                                    rec["department"], rec["em_ssn"],
                                    rec["em_nic"], rec["em_joindate"],
                                    rec["em_resigndate"], rec["em_gender"],
                                    rec["em_activestatus"]
                                ))
                                cnxn.commit()
                                stats["succeeded"] += 1
                            except Exception as e2:
                                logger.error("Failed EM_Key=%s in department %s: %s", rec["em_key"], dept_name, e2)
                            stats["attempted"] += 1

            # Create summary for this department
            summary = {
                "department": dept_name,
                "employee_count": len(dept_employees),
                "upsert_attempted": stats["attempted"],
                "upsert_succeeded": stats["succeeded"]
            }

            # Log summary
            logger.info("=== DEPARTMENT PROCESSING SUMMARY ===")
            logger.info("Department: %s", dept_name)
            logger.info("Employees in department: %d", len(dept_employees))
            logger.info("Employees upserted - attempted: %d, succeeded: %d",
                       summary["upsert_attempted"], summary["upsert_succeeded"])
            logger.info("Processing status: %s",
                       "SUCCESS" if summary["upsert_succeeded"] > 0 else "NO DATA PROCESSED")
            logger.info("=" * 50)

            department_summaries.append(summary)

        return department_summaries

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def finalize(department_summaries: List[Dict[str, Any]]):
        logger.info("=== FINAL DEPARTMENT PROCESSING SUMMARY ===")
        total_employees = sum(summary.get("employee_count", 0) for summary in department_summaries)
        total_attempted = sum(summary.get("upsert_attempted", 0) for summary in department_summaries)
        total_succeeded = sum(summary.get("upsert_succeeded", 0) for summary in department_summaries)

        logger.info("TOTAL DEPARTMENTS PROCESSED: %d", len(department_summaries))
        logger.info("TOTAL EMPLOYEES PROCESSED: %d", total_employees)
        logger.info("TOTAL EMPLOYEES UPSERTED - attempted: %d, succeeded: %d", total_attempted, total_succeeded)

        for summary in department_summaries:
            logger.info("  %s: %d employees, %d/%d upserted",
                       summary.get("department", "Unknown"),
                       summary.get("employee_count", 0),
                       summary.get("upsert_succeeded", 0),
                       summary.get("upsert_attempted", 0))

        logger.info("All departments processed successfully.")
        return "COMPLETED"

    # ——————————————————————————————————————————————————————————————
    # Orchestration: Process all lines, then group by department
    # ——————————————————————————————————————————————————————————————
    source_check_task = check_source()

    # First, process all lines to collect all employees
    all_employees = []
    for line_conn_id in TARGET_HANGER_LANE:
        safe = safe_id(line_conn_id)

        with TaskGroup(group_id=f"line_{safe}", tooltip=f"Fetch from {line_conn_id}") as tg:
            tgt_check = check_target(line_conn_id)
            raw_data = fetch_employees(line_conn_id)
            clean_data = prepare_payload(raw_data)

            tgt_check >> raw_data >> clean_data
            all_employees.append(clean_data)

        # Connect to common start + source check
        start >> source_check_task >> tg

    # Flatten employee lists and group by department
    flattened_employees = flatten_employee_lists(all_employees)
    dept_groups = group_employees_by_department(flattened_employees)
    department_summaries = process_departments_by_group(dept_groups, TARGET_HANGER_LANE[0])

    # Finalize when all departments done
    finalize_task = finalize(department_summaries)
    finalize_task >> end


# Instantiate DAG
dag = update_all_lines_employees()
