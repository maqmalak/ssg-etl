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
import os
import logging
import sys

# Add the scripts directory to the Python path
scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scripts')
sys.path.append(os.path.abspath(scripts_path))

# Import functions from hanger_line_transform.py
try:
    from hanger_line_transform import (
        get_connection_params_fallback,
        create_spark_session,
        transform_data,
        save_with_update_insert
    )
    print("Successfully imported functions from hanger_line_transform.py")
except ImportError as e:
    print(f"Error importing functions from hanger_line_transform.py: {e}")

# Timezone configuration
PKT = timezone("Asia/Karachi")

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 8, 22, tzinfo=PKT),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),
}

def check_for_data(**context):
    """
    Check if there's data in the operator_daily_performance table to process.
    Returns 'has_data' if there's data, 'no_data' otherwise.
    """
    logger.info("Starting check_for_data task")
    
    try:
        # Get connection parameters from environment variables
        host = os.getenv("POSTGRES_HOST", "172.16.7.6")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "ssg")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "postgres")
        
        logger.info(f"Connecting to PostgreSQL database: {database} on {host}:{port} as user {user}")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password
        )
        
        cursor = conn.cursor()
        
        # Check if there's data in the operator_daily_performance table
        logger.info("Executing query to count records in operator_daily_performance table")
        cursor.execute("SELECT COUNT(*) FROM operator_daily_performance;")
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        logger.info(f"Found {count} records in operator_daily_performance table")
        
        if count > 0:
            logger.info(f"Found {count} records in operator_daily_performance table. Proceeding with transformation.")
            return 'has_data'
        else:
            logger.info("No data found in operator_daily_performance table. Skipping transformation.")
            return 'no_data'
            
    except Exception as e:
        logger.error(f"Error checking for data: {e}")
        # In case of error, it's safer to skip the transformation
        return 'no_data'

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
    """
    logger.info("Starting hanger line data transformation")
    
    try:
        # Create Spark session
        logger.info("Creating Spark session...")
        spark = create_spark_session()
        logger.info("Spark session created successfully")
        
        # Execute transformation
        logger.info("Executing data transformation...")
        success = transform_data(spark)
        
        if success:
            logger.info("Data transformation completed successfully")
            return "Transformation completed successfully"
        else:
            logger.warning("Data transformation completed with issues")
            return "Transformation completed with issues"
            
    except Exception as e:
        logger.error(f"Error during data transformation: {e}")
        raise

# Define the DAG
dag = DAG(
    'hanger_line_daily_transform',
    default_args=default_args,
    description='Daily transformation of hanger line data',
    schedule_interval='0 2 * * *',  # Run daily at 2:00 AM PKT
    catchup=False,
    tags=['ssg', 'hanger_line', 'transformation'],
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

# Save task
save_task = PythonOperator(
    task_id='save_completion_status',
    python_callable=lambda **context: logger.info("Transformation process completed and saved"),
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
has_data_label >> transform_task >> save_task >> end_task
no_data_label >> skip_task >> end_task