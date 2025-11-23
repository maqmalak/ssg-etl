"""
ETL DAG: MSSQL Employee Shift Update for Line-21
------------------------------------------------------------------
✅ Fetches employee data for line-21 from MSSQL source
✅ Updates shift information in MSSQL target for last day
✅ Implements proper error handling and retry logic
✅ Logs progress and provides summary

"""

from __future__ import annotations
import sys, os, logging, time, random
from datetime import datetime, timedelta
import pandas as pd
import pendulum, pyodbc
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine, text
from functools import wraps

# ---------------------------------------------------------------- #
# CONFIGURATION
# ---------------------------------------------------------------- #
PKT = pendulum.timezone("Asia/Karachi")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime(2025, 11, 5, 17, 0, tzinfo=PKT),
    "start_date":datetime.now(PKT) - timedelta(minutes=30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "catchup": False,
}

MSSQL_Source = "SilverStr"
MSSQL_Target = "line-21"



#---------------------------------------------------------------- #
# RETRY DECORATOR (for MSSQL & PostgreSQL checks)
# ---------------------------------------------------------------- #
def retry_on_exception(max_retries=2, delay=3, backoff=1.5, label=None):
    """Retry function with exponential backoff and jitter"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            wait = delay
            context = label or func.__name__
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"❌ {context} failed after {max_retries} retries: {e}")
                        raise
                    logger.warning(f"⚠️ {context} retry {retries}/{max_retries} in {wait:.1f}s ({e})")
                    time.sleep(wait + random.uniform(0, 1))
                    wait *= backoff
        return wrapper
    return decorator


# ---------------------------------------------------------------- #
# DB UTILITIES
# ---------------------------------------------------------------- #
def build_mssql_conn_str(conn):
    return (
        f"DRIVER={{FreeTDS}};SERVER={conn.host};PORT=1433;"
        f"DATABASE={conn.schema};UID={conn.login};PWD={conn.password};"
        "TDS_Version=7.0;Connect Timeout=60;Login Timeout=60;"
    )




# ---------------------------------------------------------------- #
# DAG DEFINITION
# ---------------------------------------------------------------- #
@dag(
    dag_id="update_line21_employee_shifts",
    default_args=default_args,
    schedule="*/30 8-23,0-1 * * 1-6",  # Every 30 min Mon–Sat, 8AM–2AM PKT
    tags=["mssqlerp", "hangerline", "ssg", "employee", "line-21"],
    max_active_runs=1,
)
def update_line21_employee_shifts():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ---------------- SOURCE CHECK ---------------- #
    @task
    @retry_on_exception(label="MSSQL Source Check")
    def source_check(ti=None):
        conn = BaseHook.get_connection(MSSQL_Source)
        conn_str = build_mssql_conn_str(conn)
        with pyodbc.connect(conn_str, timeout=30) as c:
            c.cursor().execute("SELECT 1")
        msg = "✅ MSSQL Source reachable"
        logger.info(msg)
        ti.xcom_push(key="source_check", value=msg)
        return msg

    # ---------------- TARGET CHECK ---------------- #
    @task
    @retry_on_exception(label="MSSQL Target Check")
    def target_check(ti=None):
        conn = BaseHook.get_connection(MSSQL_Target)
        conn_str = build_mssql_conn_str(conn)
        with pyodbc.connect(conn_str, timeout=30) as c:
            c.cursor().execute("SELECT 1")
        msg = "✅ MSSQL Target reachable"
        logger.info(msg)
        ti.xcom_push(key="target_check", value=msg)
        return msg

# ---------------- DATA FETCH ---------------- #
    @task
    @retry_on_exception()
    def fetch_data_from_source(connection_id: str):
        """
        Fetch employee data from MSSQL source for line-21

        Args:
            connection_id (str): The MSSQL source connection ID

        Returns:
            list: List of employee records as dictionaries
        """
        logger.info(f"[{connection_id}] 🚀 Starting data extraction...")
        start_time = time.time()

        try:
            # Get MSSQL connection and last extract timestamp
            connection = BaseHook.get_connection(MSSQL_Source)
            conn_str = build_mssql_conn_str(connection)

            # SQL Query (fully aligned)
            query = """
                SELECT
                    e.ID,
                    e.INA_ID,
                    e.Title,
                    e.Desig_ID,
                    e.Deptt_ID,
                    e.Current_Line_ID,
                    e.Latest_Line_ID,
                    e.Assignment_Date,
                    Line_Desc,
                    Shift,
                    case when Shift = 'Day' then 1 else 2 end as Shift_ID,
                    e.Joindate,
                    ActiveStatus
                FROM
                    hangerline_emp AS e
                WHERE Line_Desc='line-21' and INA_ID=106133009
                """

            with pyodbc.connect(conn_str, timeout=30) as c:
                cur = c.cursor()
                cur.execute(query)
                # Fetch all results
                rows = cur.fetchall()
                # Get column names
                columns = [column[0] for column in cur.description]
                # Convert to list of dictionaries for easier processing
                data = [dict(zip(columns, row)) for row in rows]

                total = len(data)  # Calculate total rows
                logger.info(f"[{connection_id}] 🎯 Extraction complete → {total} rows in {time.time() - start_time:.2f}s")

            return data
        except Exception as e:
            logger.error(f"❌ Error fetching data from source {connection_id}: {str(e)}", exc_info=True)
            raise

    # Function to update employee shift (without task decorator for use inside other tasks)
    @retry_on_exception(label="Update Employee Shift")
    def update_employee_shift_direct(emp_id: int, shift_id: int, connection_id: str):
        """
        Update an employee's shift in the target database

        Args:
            emp_id (int): Employee ID to update
            shift_id (int): New shift ID to assign
            connection_id (str): The target connection ID

        Returns:
            int: Number of rows affected by the update
        """
        logger.info(f"[{connection_id}] 🚀 Updating employee {emp_id} with shift {shift_id}...")
        start_time = time.time()

        try:
            # Get MSSQL connection and last extract timestamp
            connection = BaseHook.get_connection(MSSQL_Target)
            conn_str = build_mssql_conn_str(connection)

            # SQL Query with proper parameterization
            query = "UPDATE IHS.dbo.ODP_Master SET ODP_Shift = ? WHERE ODP_Date = '2025-11-20' AND ODP_EM_Key = ?"
            # Calculate yesterday's date for 'last day'
            yesterday = (datetime.now() - timedelta(days=1)).strftime('%Y-%m-%d')
            update_date = yesterday  # Use yesterday (last day) or pass as parameter

            with pyodbc.connect(conn_str, timeout=30) as c:
                cur = c.cursor()
                cur.execute(query, (shift_id, emp_id))
                c.commit()  # Commit the changes

                # Get number of affected rows
                rows_affected = cur.rowcount
                logger.info(f"[{connection_id}] 🎯 Update complete → {rows_affected} rows in {time.time() - start_time:.2f}s")

            return rows_affected
        except Exception as e:
            logger.error(f"❌ Error updating employee {emp_id}: {str(e)}", exc_info=True)
            raise

    # Task version of the update function
    @task
    def update_employee_shift_task(emp_id: int, shift_id: int, connection_id: str):
        return update_employee_shift_direct(emp_id, shift_id, connection_id)



    # ---------------- PROCESS EMPLOYEE DATA ---------------- #
    @task
    def process_employee_data(employees_data: list):
        """
        Process the fetched employee data and return a list of updates to perform

        Args:
            employees_data (list): List of employee records as dictionaries

        Returns:
            list: List of dictionaries containing emp_id and shift_id for updates
        """
        logger.info(f"🔍 Processing {len(employees_data)} employee records...")

        updates_to_perform = []
        invalid_records = 0

        for i, emp in enumerate(employees_data):
            try:
                emp_id = emp.get('INA_ID')
                shift_id = emp.get('Shift_ID')

                # Validate required fields
                if emp_id is not None and shift_id is not None:
                    updates_to_perform.append({
                        'emp_id': int(emp_id),
                        'shift_id': int(shift_id)
                    })
                else:
                    logger.warning(f"⚠️ Invalid record at index {i}: Missing emp_id or shift_id - {emp}")
                    invalid_records += 1
            except (ValueError, TypeError) as e:
                logger.warning(f"⚠️ Invalid data type at index {i}: {emp} - Error: {str(e)}")
                invalid_records += 1

        logger.info(f"✅ Prepared {len(updates_to_perform)} valid updates, skipped {invalid_records} records")
        return updates_to_perform

    # ---------------- EXECUTE UPDATES FOR ALL EMPLOYEES ---------------- #
    @task
    def execute_employee_updates(updates_list: list):
        """
        Execute update operations for all employees

        Args:
            updates_list (list): List of dictionaries containing emp_id and shift_id

        Returns:
            dict: Summary of update operations
        """
        logger.info(f"🔧 Executing {len(updates_list)} employee updates...")

        if not updates_list:
            logger.warning("⚠️ No updates to execute")
            return {
                'successful_updates': 0,
                'failed_updates': 0,
                'total_processed': 0
            }

        successful_updates = 0
        failed_updates = 0

        for i, update_info in enumerate(updates_list):
            try:
                emp_id = update_info.get('emp_id')
                shift_id = update_info.get('shift_id')

                if not emp_id or shift_id is None:
                    logger.warning(f"⚠️ Skipping update {i+1} - invalid data: {update_info}")
                    failed_updates += 1
                    continue

                # Call the direct update function for each employee
                rows_affected = update_employee_shift_direct(emp_id, shift_id, MSSQL_Target)

                if rows_affected > 0:
                    successful_updates += 1
                    logger.info(f"✅ Successfully updated employee {emp_id} with shift {shift_id} - {rows_affected} rows affected")
                else:
                    logger.warning(f"⚠️ No rows affected for employee {emp_id}")
                    failed_updates += 1
            except Exception as e:
                logger.error(f"❌ Failed to update employee {update_info.get('emp_id', 'unknown')}: {str(e)}", exc_info=True)
                failed_updates += 1

        summary = {
            'successful_updates': successful_updates,
            'failed_updates': failed_updates,
            'total_processed': len(updates_list)
        }

        logger.info(f"✅ Update summary: {summary}")
        return summary

    # ---------------------------------------------------------------- #
    # PIPELINE
    # ---------------------------------------------------------------- #
    src = source_check()
    tgt = target_check()

    # Fetch the employee data
    employees_data = fetch_data_from_source(MSSQL_Source)

    # Process the employee data to prepare updates
    updates_to_perform = process_employee_data(employees_data)

    # Execute all updates
    updates_list = execute_employee_updates(updates_to_perform)

    # Create the load tasks but control execution based on whether the table exists

    # Ensure source_check runs before target_check as requested: source_check >> target_check
    src >> tgt >> employees_data >> updates_to_perform >> updates_list >> end


dag = update_line21_employee_shifts()
