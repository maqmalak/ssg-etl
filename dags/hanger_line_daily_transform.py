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
    from sparkFiles.hangerline_transform_upsert import (
        create_spark_session,
        transform_data
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
    'start_date': datetime(2025, 11, 20, tzinfo=PKT),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=1),
}

def check_for_data(**context):
    """
    Check if there's data in the operator_daily_performance table to process.
    Returns 'has_data' if there's data, 'no_data' otherwise.
    """
    logger.info("Starting check_for_data task")
    
    try:
        # Get connection parameters from Airflow connection
        try:
            connection = BaseHook.get_connection("pg-ssg")
            host = connection.host
            port = connection.port if connection.port else 5432
            database = connection.schema
            user = connection.login
            password = connection.password
            
            logger.info(f"Using Airflow connection 'pg-ssg':")
            logger.info(f"  Host: {host}")
            logger.info(f"  Port: {port}")
            logger.info(f"  Database: {database}")
            logger.info(f"  User: {user}")
            logger.info(f"  Password length: {len(password) if password else 0}")
        except Exception as e:
            logger.warning(f"Could not get Airflow connection 'pg-ssg', using environment variables: {e}")
            # Fallback to environment variables
            host = os.getenv("POSTGRES_HOST", "172.16.7.6")
            port = os.getenv("POSTGRES_PORT", "5432")
            database = os.getenv("POSTGRES_DB", "ssg")
            user = os.getenv("POSTGRES_USER", "postgres")
            password = os.getenv("POSTGRES_PASSWORD", "P@kistan12")  # Use correct password
            
            logger.info(f"Environment variables check:")
            logger.info(f"  POSTGRES_HOST: {os.getenv('POSTGRES_HOST', 'Not set')}")
            logger.info(f"  POSTGRES_PORT: {os.getenv('POSTGRES_PORT', 'Not set')}")
            logger.info(f"  POSTGRES_DB: {os.getenv('POSTGRES_DB', 'Not set')}")
            logger.info(f"  POSTGRES_USER: {os.getenv('POSTGRES_USER', 'Not set')}")
            logger.info(f"  POSTGRES_PASSWORD: {'*' * len(os.getenv('POSTGRES_PASSWORD', '')) if os.getenv('POSTGRES_PASSWORD') else 'Not set'}")
            
            logger.info(f"Using connection parameters:")
            logger.info(f"  Host: {host}")
            logger.info(f"  Port: {port}")
            logger.info(f"  Database: {database}")
            logger.info(f"  User: {user}")
            logger.info(f"  Password length: {len(password) if password else 0}")
        
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
        
        # Check if there's recent data in the operator_daily_performance table (last 2 days)
        logger.info("Executing query to count recent records in operator_daily_performance table")
        cursor.execute("""
            SELECT COUNT(*) FROM operator_daily_performance 
            WHERE created_at >= CURRENT_DATE - INTERVAL '3 days'
        """)
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        logger.info(f"Found {count} recent records in operator_daily_performance table")
        
        if count > 0:
            logger.info(f"Found {count} recent records in operator_daily_performance table. Proceeding with transformation.")
            return 'has_data'
        else:
            # Also check if this might be the first run by checking other tables
            try:
                conn = psycopg2.connect(
                    host=host,
                    port=port,
                    database=database,
                    user=user,
                    password=password
                )
                cursor = conn.cursor()
                
                # Check if ETL log table exists and has recent data
                cursor.execute("""
                    SELECT COUNT(*) FROM etl_extract_log 
                    WHERE lastextractdatetime >= CURRENT_DATE - INTERVAL '2 days'
                """)
                log_count = cursor.fetchone()[0]
                logger.info(f"Found {log_count} recent records in etl_extract_log table")
                
                cursor.close()
                conn.close()
                
                if log_count > 0:
                    logger.info("Recent ETL process has run but no recent data found in operator_daily_performance table")
                    logger.info("This might indicate an issue with the data extraction process or no new data")
                    # Still return has_data to trigger transformation for debugging
                    logger.info("Proceeding with transformation to check what happens...")
                    return 'has_data'
                else:
                    logger.info("No recent ETL activity - this might be the first run or ETL is not running")
                    
            except Exception as e:
                logger.info(f"Could not check etl_extract_log table: {e}")
            
            logger.info("No recent data found in operator_daily_performance table. Skipping transformation.")
            return 'no_data'
            
    except Exception as e:
        logger.error(f"Error checking for data: {e}")
        # Even in case of error, let's try to proceed to see what happens
        logger.info("Error checking for data, but proceeding with transformation for debugging")
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
    'hanger_line_daily_transform',
    default_args=default_args,
    description='Daily transformation of hanger line data',
    schedule='0 2 * * *',  # Run daily at 2:00 AM PKT
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
            logger.info(f"   • {table_name}: {record_count:,} records")

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
