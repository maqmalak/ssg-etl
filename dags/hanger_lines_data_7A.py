"""
DAG for running hanger line data transformation daily.
This DAG checks if there's data to process, and if so, executes the transformation.
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from pendulum import timezone
import psycopg2
import logging
import sys
from airflow.hooks.base import BaseHook

# Add the sparkFiles directory to the Python path
# sparkfiles_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'sparkFiles')
# scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scripts')
# sys.path.append(os.path.abspath(sparkfiles_path))
# sys.path.append(os.path.abspath(scripts_path))
import sys
import os
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import functions from hangerline_transform_spark.py (working Spark implementation with optimized upsert)
try:
    from sparkFiles.hangerline_transform_spark_7A import (
        create_spark_session,
        transform_data,
        check_for_recent_data
    )
    print("Successfully imported functions from hangerline_transform_spark.py")
except ImportError as e:
    print(f"Error importing functions from hangerline_transform_spark.py: {e}")
    import traceback
    traceback.print_exc()


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
        
        # Call the Spark-enabled function from hangerline_transform_spark_7A.py
        # This will create a Spark session, submit to cluster, and check for data
        logger.info("Calling check_for_recent_data with Spark session...")
        count = check_for_recent_data(spark=None, days=30)  # spark=None means it will create its own session
        
        if count > 0:
            logger.info(f"✓ Found {count:,} recent records in pmr_production_data (via Spark cluster)")
            logger.info("Proceeding with transformation.")
            return 'has_data'
        else:
            logger.info("⚠ No recent data found in pmr_production_data table")
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
    Returns detailed transformation results as XCom.
    """
    logger.info("Starting hanger line data transformation")

    try:
        # Check if the functions were imported successfully
        if 'create_spark_session' not in globals() or 'transform_data' not in globals():
            raise RuntimeError("Required functions were not imported successfully. Check import errors above.")

        # Execute transformation - note transform_data now returns a summary dict, not boolean
        logger.info("Executing data transformation...")
        spark = create_spark_session()
        results = transform_data(spark)  # transform_data handles its own Spark session now

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
    Pull and summarize the transformation XCom results
    """
    ti = context['task_instance']
    results = ti.xcom_pull(task_ids='transform_data')

    if results and isinstance(results, dict):
        logger.info("=== TRANSFORMATION SUMMARY ===")
        logger.info(f"✅ Success: {results.get('success', False)}")
        logger.info(f"📊 Data Loaded: {results.get('data_loaded', 0):,}")
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
            # # Remove :, formatting - works for both int and string
            # logger.info(f"   • {table_name}: {record_count} records")


        logger.info(f"💬 Message: {results.get('message', 'No message')}")
        logger.info("=" * 50)

        return results  # Return the complete results for downstream tasks
    else:
        logger.warning("No transformation results found in XCom")
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
start_task >> check_data_task
check_data_task >> has_data_label
check_data_task >> no_data_label
has_data_label >> transform_task >> summarize_task >> end_task
no_data_label >> skip_task >> end_task