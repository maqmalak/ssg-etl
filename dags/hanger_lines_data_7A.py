"""
DAG for running hanger line data transformation daily.
This DAG checks if there's data to process, and if so, executes the transformation.
"""

# Standard library imports
import logging
import os
import sys
from datetime import datetime, timedelta
from typing import Optional

# Third-party imports
from pendulum import timezone

# Airflow imports
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.trigger_rule import TriggerRule

# Database imports
from sqlalchemy import create_engine, text
import psycopg2

# Project imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import Spark transformation functions
try:
    from sparkFiles.hangerline_transform_spark_7A import (
        create_spark_session,
        transform_data,
        check_for_recent_data,
        get_postgres_source_connection,
        get_target_postgres_connection
    )
    print("Successfully imported functions from hangerline_transform_spark.py")
except ImportError as e:
    print(f"Error importing functions from hangerline_transform_spark.py: {e}")
    import traceback
    traceback.print_exc()


# ---------------- DB CONNECTIONS ---------------- #
def get_postgres_engine(connection_id: str):
    """Create PostgreSQL engine with Airflow connection or fallback."""
    from urllib.parse import quote_plus
    try:
        c = BaseHook.get_connection(connection_id)
        uri = f"postgresql://{c.login}:{quote_plus(c.password or '')}@{c.host}:{c.port}/{c.schema}"
        logger.info(f"[PG] Connected via Airflow: {c.host}/{c.schema}")
    except Exception as e:
        logger.warning(f"[PG] Airflow conn failed ({e}), using fallback")
    return create_engine(uri, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600, echo=False)


# ---------------- ETL LOGGING ---------------- #
def make_result(status: str, step: str, connection_id: str, message: str) -> dict:
    emoji = {"success": "✅", "fail": "❌", "skipped": "⚠️"}.get(status, "ℹ️")
    return {
        "status": status,
        "step": step,
        "connection_id": connection_id,
        "message": message,
        "friendly": f"{emoji} [{connection_id}] {step.upper()} - {message}",
    }


def check_source_connection(connection_id: str) -> dict:
    step = "source"
    try:
        # Get connection parameters
        conn_params = get_postgres_source_connection()
        # Create SQLAlchemy engine from connection parameters
        from urllib.parse import quote_plus
        uri = f"postgresql://{conn_params['user']}:{quote_plus(conn_params['password'])}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
        engine = create_engine(uri, pool_pre_ping=True)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return make_result("success", step, connection_id, "PostgreSQL reachable")
    except Exception as e:
        msg = f"PostgreSQL unreachable: {e}"
        logger.warning(f"[{connection_id}] ⚠️ {msg}")
        raise AirflowSkipException(make_result("skipped", step, connection_id, msg)["friendly"])


def check_target_connection(connection_id: str) -> dict:
    step = "target"
    try:
        # Get connection parameters
        conn_params = get_target_postgres_connection()
        # Create SQLAlchemy engine from connection parameters
        from urllib.parse import quote_plus
        uri = f"postgresql://{conn_params['user']}:{quote_plus(conn_params['password'])}@{conn_params['host']}:{conn_params['port']}/{conn_params['database']}"
        engine = create_engine(uri, pool_pre_ping=True)
        # Test connection
        with engine.connect() as conn:
            conn.execute(text("SELECT 1"))
        engine.dispose()
        return make_result("success", step, connection_id, "PostgreSQL reachable")
    except Exception as e:
        msg = f"PostgreSQL unreachable: {e}"
        logger.warning(f"[{connection_id}] ⚠️ {msg}")
        raise AirflowSkipException(make_result("skipped", step, connection_id, msg)["friendly"])


# ---------------- ETL LOGGING ---------------- #
def insert_etl_log(pid: str, src: str, count: int, start: datetime, end: datetime,
                   last_dt: Optional[datetime], success: bool, status: str, msg: Optional[str]):
    """Insert ETL run log."""
    engine = get_postgres_engine("pg-ssg")
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

def get_last_extract_dt_from_log(src: str) -> Optional[datetime]:
    """Get last extract datetime from ETL log for incremental processing."""
    engine = get_postgres_engine("pg-ssg")
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


# Import table creation function
from scripts.create_target_pg_hl_table import create_etl_log_table_if_not_exists


# Timezone configuration
PKT = timezone("Asia/Karachi")

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 12, 25, tzinfo=PKT),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

       

def check_for_data(**context):
    """
    Check if there's data in the pmr_production_data table to process using Spark cluster.
    Uses the Spark-enabled check_for_recent_data() function from hangerline_transform_spark_7A.py
    Gets last_extract_dt from ETL log to determine what data to check for.
    Returns 'has_data' if there's data, 'no_data' otherwise.
    """
    logger.info("Starting check_for_data task with Spark cluster")

    try:
        # Check if the function was imported successfully
        if 'check_for_recent_data' not in globals():
            logger.error("check_for_recent_data function was not imported successfully")
            # Fallback: proceed with transformation anyway
            logger.info("Proceeding with transformation despite import error")
            return 'has_data'

        # Check if create_spark_session is available
        if 'create_spark_session' not in globals():
            logger.error("create_spark_session function was not imported successfully")
            logger.info("Proceeding with transformation despite import error")
            return 'has_data'

        # Get last extract datetime from ETL log
        connection_id = "INA-7A"
        last_extract_dt = get_last_extract_dt_from_log(connection_id)

        if last_extract_dt:
            logger.info(f"[{connection_id}] Last extract datetime: {last_extract_dt}")
        else:
            logger.info(f"[{connection_id}] No previous extract found, will check for any recent data")

        # Call the Spark-enabled function from hangerline_transform_spark_7A.py
        # This will create a Spark session, submit to cluster, and check for data
        logger.info("Calling check_for_recent_data with Spark session...")
        count = check_for_recent_data(spark=None, last_extract_dt=last_extract_dt)

        if count > 0:
            logger.info(f"✓ Found {count:,} records in pmr_production_data since last extract (via Spark cluster)")
            logger.info("Proceeding with transformation.")
            return 'has_data'
        else:
            logger.info("⚠ No new data found in pmr_production_data table since last extract")
            # Still proceed with transformation for debugging
            logger.info("Proceeding with transformation anyway")
            return 'has_data'

    except Exception as e:
        logger.error(f"Error in check_for_data task: {e}")
        import traceback
        traceback.print_exc()
        # On error, proceed with transformation for debugging
        logger.info("Proceeding with transformation despite error")
        return 'has_data'

def log_start(**context):
    """
    Log the start of the DAG execution.
    """
    logger.info("Starting hanger_line_daily_transform DAG execution")
    logger.info(f"Execution date: {context['execution_date']}")
    logger.info(f"Run ID: {context['run_id']}")
    return "DAG execution started"

def log_end(**context):
    """
    Log the end of the DAG execution.
    """
    logger.info("Completed hanger_line_daily_transform DAG execution")
    logger.info(f"Execution date: {context['execution_date']}")
    logger.info(f"Run ID: {context['run_id']}")
    return "DAG execution completed"

def execute_transformation(**context):
    """
    Execute the hanger line data transformation using imported functions.
    Gets last_extract_dt from ETL log and passes it to transformation for incremental processing.
    Returns detailed transformation results as XCom.
    """
    logger.info("Starting hanger line data transformation")

    try:
        # Check if the functions were imported successfully
        if 'create_spark_session' not in globals() or 'transform_data' not in globals():
            raise RuntimeError("Required functions were not imported successfully. Check import errors above.")

        # Get last extract datetime from ETL log for incremental processing
        connection_id = "INA-7A"
        last_extract_dt = get_last_extract_dt_from_log(connection_id)

        if last_extract_dt:
            logger.info(f"[{connection_id}] Using incremental mode - processing data since {last_extract_dt}")
        else:
            logger.info(f"[{connection_id}] No previous extract found - using default filtering")

        # Execute transformation with last_extract_dt for incremental processing
        logger.info("Executing data transformation...")
        spark = create_spark_session()
        results = transform_data(spark, last_extract_dt=last_extract_dt)  # Pass last_extract_dt for incremental processing

        # Return the complete results dictionary for XCom
        if results and isinstance(results, dict):
            logger.info(f"Transformation completed: {results.get('message', 'Unknown status')}")
            return results
        else:
            logger.warning("Transformation returned invalid results")
            return {"success": False, "message": "Invalid transformation results"}

    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        # Return error details as XCom instead of raising exception
        return {
            "success": False,
            "message": f"Transformation failed: {str(e)}",
            "tables_processed": []
        }

# Define the DAG
dag = DAG(
    'hanger_lines_data_7A',
    default_args=default_args,
    description="ETL pipeline for Hanger Lines Data 7A (8 AM-2 AM PKT, Mon-Sat)",
    # schedule='0 2 * * *',  # Run daily at 2:00 AM PKT
    schedule="8,18,28,38,48,58 8-23,0-1 * * 1-6",  # ✅ Every 10 min, 8AM–2AM, Mon–Sat
    tags=["ssg", "hangerline", "data", "upsert","7A"],
    catchup=False,
    max_active_runs=1
)

# Source connection check task
source_check_task = PythonOperator(
    task_id='source_check_INA-7A',
    python_callable=check_source_connection,
    op_args=['INA-7A'],
    dag=dag
)

# Target connection check task
target_check_task = PythonOperator(
    task_id='target_check_pg-ssg',
    python_callable=check_target_connection,
    op_args=['pg-ssg'],
    dag=dag
)

# Start task
start_task = PythonOperator(
    task_id='start',
    python_callable=log_start,
    dag=dag
)

# Check for data task
check_data_task = BranchPythonOperator(
    task_id='check_for_data',
    python_callable=check_for_data,
    dag=dag
)

# Has data label
has_data_label = EmptyOperator(
    task_id='has_data',
    dag=dag
)

# Transform task - uses imported functions
transform_task = PythonOperator(
    task_id='transform_data',
    python_callable=execute_transformation,
    dag=dag
)

# No data label
no_data_label = EmptyOperator(
    task_id='no_data',
    dag=dag
)

# Skip task
skip_task = EmptyOperator(
    task_id='skip_transformation',
    dag=dag
)

# Summary task that returns XCom results
def summarize_transformation_results(**context):
    """
    Pull and summarize the transformation XCom results and log ETL status
    """
    ti = context['task_instance']
    results = ti.xcom_pull(task_ids='transform_data')

    # Get DAG run information for ETL logging
    dag_run = context['dag_run']
    process_id = str(dag_run.run_id)
    source_connection = "INA-7A"  # Source connection for this DAG
    start_time = dag_run.start_date
    end_time = datetime.now(PKT)

    if results and isinstance(results, dict):
        logger.info("=== TRANSFORMATION SUMMARY ===")
        success = results.get('success', False)
        data_loaded = results.get('data_loaded', 0)
        message = results.get('message', 'No message')

        logger.info(f"✅ Success: {success}")
        logger.info(f"📊 Data Loaded: {data_loaded:,}")
        logger.info(f"🔄 Data Filtered: {results.get('data_filtered', 0):,}")

        # Log details for each table
        tables_processed = results.get('tables_processed', [])
        logger.info("📋 Tables Processed:")
        for table_info in tables_processed:
            table_name = table_info.get('table', 'Unknown')
            record_count = table_info.get('records', 0)
            # Format count with thousand separator if numeric, otherwise use as-is
            count_str = f"{record_count:,}" if isinstance(record_count, (int, float)) else str(record_count)
            logger.info(f"   • {table_name}: {count_str}")

        logger.info(f"💬 Message: {message}")
        logger.info("=" * 50)

        # Extract max ppd_complete_time from transformation results
        max_complete_time = results.get('max_ppd_complete_time')

        # Insert ETL log entry with the actual max timestamp from processed data
        try:
            if success:
                insert_etl_log(
                    pid=process_id,
                    src=source_connection,
                    count=data_loaded,
                    start=start_time,
                    end=end_time,
                    last_dt=max_complete_time,  # Use actual max timestamp from processed data
                    success=True,
                    status="Completed",
                    msg=message
                )
                if max_complete_time:
                    logger.info(f"📅 ETL log updated with lastextractdatetime: {max_complete_time}")
            else:
                insert_etl_log(
                    pid=process_id,
                    src=source_connection,
                    count=0,
                    start=start_time,
                    end=end_time,
                    last_dt=None,
                    success=False,
                    status="Failed",
                    msg=message
                )
        except Exception as log_error:
            logger.error(f"Failed to insert ETL log: {log_error}")

        return results  # Return the complete results for downstream tasks
    else:
        logger.warning("No transformation results found in XCom")
        # Log failed ETL run
        try:
            insert_etl_log(
                pid=process_id,
                src=source_connection,
                count=0,
                start=start_time,
                end=end_time,
                last_dt=None,
                success=False,
                status="Failed",
                msg="No transformation results available"
            )
        except Exception as log_error:
            logger.error(f"Failed to insert ETL log: {log_error}")

        return {"error": "No transformation results available"}

summarize_task = PythonOperator(
    task_id='save_completion_status',
    python_callable=summarize_transformation_results,
    dag=dag
)

# End task
end_task = PythonOperator(
    task_id='end',
    python_callable=log_end,
    dag=dag
)

# Set task dependencies
start_task >> [source_check_task, target_check_task]
[source_check_task, target_check_task] >> check_data_task
check_data_task >> has_data_label
check_data_task >> no_data_label
has_data_label >> transform_task >> summarize_task >> end_task
no_data_label >> skip_task >> end_task
