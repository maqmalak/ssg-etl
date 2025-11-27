# """
# Optimized hanger_line_daily_transform.py with ETL best practices
# """

# import time
# import logging
# from datetime import datetime, timedelta
# from airflow import DAG
# from airflow.operators.empty import EmptyOperator
# from airflow.operators.python import PythonOperator, BranchPythonOperator
# from airflow.hooks.base import BaseHook
# import psycopg2
# import os
# import sys
# from pendulum import timezone

# # Add the scripts directory to the Python path
# scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scripts')
# sys.path.append(os.path.abspath(scripts_path))

# from scripts.create_table_hourly import (
#     create_target_table_if_not_exists,
#     create_etl_log_odp_table_if_not_exists,
#     OdpDateOc,
#     OdpDateShift,
#     OdpDateEmployee
# )


# # Import functions from hanger_line_transform.py
# try:
#     from sparkFiles.hangerline_transform_optimized import (
#         create_spark_session_optimized,
#         transform_data_optimized
#     )
#     print("Successfully imported optimized functions from hanger_line_transform.py")
# except ImportError as e:
#     print(f"Error importing optimized functions from hanger_line_transform.py: {e}")

# # Timezone configuration
# PKT = timezone("Asia/Karachi")

# # Configure logging
# logger = logging.getLogger(__name__)
# logger.setLevel(logging.INFO)

# # ETL Configuration
# class ETLConfig:
#     def __init__(self):
#         # Original attributes for DAG functionality
#         self.data_freshness_threshold_hours = int(os.getenv('DATA_FRESHNESS_THRESHOLD', '24'))
#         self.min_records_threshold = int(os.getenv('MIN_RECORDS_THRESHOLD', '100'))
#         self.check_recent_days = int(os.getenv('CHECK_CURRENT_DAYS', '1'))
#         self.max_retry_attempts = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))
        
#         # Additional attributes for Spark functionality (matching sparkFiles/hangerline_transform_optimized.py)
#         self.spark_executor_memory = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')
#         self.spark_driver_memory = os.getenv('SPARK_DRIVER_MEMORY', '4g')
#         self.spark_shuffle_partitions = int(os.getenv('SPARK_SHUFFLE_PARTITIONS', '200'))
#         self.max_partition_bytes = os.getenv('SPARK_MAX_PARTITION_BYTES', '134217728')  # 128MB
#         self.enable_adaptive_query_execution = os.getenv('SPARK_ADAPTIVE_ENABLED', 'true').lower() == 'true'
    
#     def validate(self):
#         """Validate configuration values"""
#         if self.data_freshness_threshold_hours <= 0:
#             raise ValueError("Data freshness threshold must be positive")
#         if self.min_records_threshold < 0:
#             raise ValueError("Minimum records threshold cannot be negative")
#         if self.check_recent_days <= 0:
#             raise ValueError("Check recent days must be positive")
#         if self.spark_shuffle_partitions <= 0:
#             raise ValueError("Spark shuffle partitions must be positive")

# # Default arguments for the DAG
# default_args = {
#     'owner': 'airflow',
#     'depends_on_past': False,
#     'start_date': datetime(2025, 9, 15, tzinfo=PKT),
#     'retries': 2,
#     'retry_delay': timedelta(minutes=5),
#     'execution_timeout': timedelta(hours=3),
#     'retry_exponential_backoff': True,
# }

# def log_etl_metrics(start_time, records_processed=0, status="completed"):
#     """
#     Log comprehensive ETL metrics
#     """
#     end_time = time.time()
#     duration = end_time - start_time
    
#     metrics = {
#         'execution_time_seconds': round(duration, 2),
#         'records_processed': records_processed,
#         'status': status,
#         'throughput_rps': round(records_processed / duration, 2) if duration > 0 and records_processed > 0 else 0,
#         'timestamp': datetime.now().isoformat()
#     }
    
#     logger.info(f"ETL Metrics: {metrics}")
#     return metrics

# def get_database_connection():
#     """
#     Get database connection with proper error handling
#     """
#     try:
#         # Get connection parameters from Airflow connection
#         try:
#             connection = BaseHook.get_connection("pg-ssg")
#             host = connection.host
#             port = connection.port if connection.port else 5432
#             database = connection.schema
#             user = connection.login
#             password = connection.password
            
#             logger.info(f"Using Airflow connection 'pg-ssg'")
#         except Exception as e:
#             logger.warning(f"Could not get Airflow connection 'pg-ssg', using environment variables: {e}")
#             # Fallback to environment variables
#             host = os.getenv("POSTGRES_HOST", "172.16.7.6")
#             port = os.getenv("POSTGRES_PORT", "5432")
#             database = os.getenv("POSTGRES_DB", "ssg")
#             user = os.getenv("POSTGRES_USER", "postgres")
#             password = os.getenv("POSTGRES_PASSWORD", "P@kistan12")
        
#         # Connect to PostgreSQL
#         conn = psycopg2.connect(
#             host=host,
#             port=port,
#             database=database,
#             user=user,
#             password=password,
#             connect_timeout=30  # Add connection timeout
#         )
        
#         return conn
        
#     except Exception as e:
#         logger.error(f"Database connection failed: {e}")
#         raise

# # Create tables if they don't exist - this will be handled within DAG tasks, not at module level



# def check_for_data_enhanced(**context):
#     """
#     Enhanced data check with quality metrics and proper validation
#     """
#     start_time = time.time()
#     logger.info("Starting enhanced data quality check")
    
#     try:
#         config = ETLConfig()
#         config.validate()
        
#         # Get database connection
#         conn = get_database_connection()
#         cursor = conn.cursor()
        
#         try:
#             # Enhanced data quality checks
#             logger.info("Executing comprehensive data quality checks")
#             cursor.execute("""
#                 SELECT 
#                     COUNT(*) as total_records,
#                     COUNT(DISTINCT "source_connection") as line_count,
#                     MAX("created_at") as latest_record,
#                     MIN("created_at") as oldest_record,
#                     COUNT(DISTINCT "ODP_Date") as date_count
#                 FROM operator_daily_performance 
#                 WHERE "ODP_Date" = (
#                            DATE_TRUNC('day', NOW() - INTERVAL '8 hours') - INTERVAL '%(days)s days'
#                     )::DATE
#             """, {'days': config.check_recent_days})
            
#             result = cursor.fetchone()
            
#             if result and result[0] is not None:
#                 metrics = {
#                     'total_records': result[0],
#                     'line_count': result[1],
#                     'latest_record': result[2],
#                     'oldest_record': result[3],
#                     'date_count': result[4],
#                     'data_freshness_hours': (
#                         (datetime.now() - result[2]).total_seconds() / 3600 
#                         if result[2] else None
#                     ) if result[2] else None
#                 }
                
#                 logger.info(f"Data quality metrics: {metrics}")
                
#                 # Quality validation
#                 if metrics['total_records'] >= config.min_records_threshold:
#                     if metrics['data_freshness_hours'] and metrics['data_freshness_hours'] <= config.data_freshness_threshold_hours:
#                         logger.info("Data quality acceptable, proceeding with transformation")
#                         log_etl_metrics(start_time, metrics['total_records'], "data_check_passed")
#                         return 'has_data'
#                     else:
#                         logger.warning(f"Data is stale (freshness: {metrics['data_freshness_hours']} hours)")
#                 else:
#                     logger.warning(f"Insufficient data volume (records: {metrics['total_records']})")
#             else:
#                 logger.info("No data found in the specified time range")
                
#         finally:
#             cursor.close()
#             conn.close()
        
#         logger.info("Data quality check completed - no suitable data found")
#         log_etl_metrics(start_time, 0, "data_check_no_data")
#         return 'no_data'
        
#     except Exception as e:
#         logger.error(f"Error in enhanced data check: {e}")
#         log_etl_metrics(start_time, 0, "data_check_failed")
#         # In case of error, proceed for debugging
#         return 'has_data'

# def log_start(**context):
#     """
#     Log the start of the DAG execution with comprehensive information
#     """
#     logger.info("Starting hanger_line_daily_transform DAG execution")
#     logger.info(f"Execution date: {context.get('execution_date', 'Unknown')}")
#     logger.info(f"Run ID: {context.get('run_id', 'Unknown')}")
#     logger.info(f"DAG run configuration: {context.get('dag_run', {}).conf if context.get('dag_run') else 'No config'}")
#     return "DAG execution started"

# def log_end(**context):
#     """
#     Log the end of the DAG execution with final status
#     """
#     logger.info("Completed hanger_line_daily_transform DAG execution")
#     logger.info(f"Execution date: {context.get('execution_date', 'Unknown')}")
#     logger.info(f"Run ID: {context.get('run_id', 'Unknown')}")
    
#     # Log task instance information
#     task_instance = context.get('task_instance')
#     if task_instance:
#         logger.info(f"Task duration: {task_instance.duration}")
#         logger.info(f"Task state: {task_instance.state}")
    
#     return "DAG execution completed"

# def execute_transformation_optimized(**context):
#     """
#     Execute the hanger line data transformation using optimized functions
#     """
#     start_time = time.time()
#     logger.info("Starting optimized hanger line data transformation")
    
#     try:
#         # Create optimized Spark session
#         logger.info("Creating optimized Spark session...")
#         config = ETLConfig()
#         spark = create_spark_session_optimized(config)
#         logger.info("Optimized Spark session created successfully")
        
#         # Execute transformation with metrics
#         logger.info("Executing optimized data transformation...")
#         success, records_processed = transform_data_optimized(spark)
        
#         if success:
#             logger.info("Data transformation completed successfully")
#             log_etl_metrics(start_time, records_processed, "transformation_success")
#             return f"Transformation completed successfully with {records_processed} records processed"
#         else:
#             logger.warning("Data transformation completed with issues")
#             log_etl_metrics(start_time, records_processed, "transformation_partial")
#             return f"Transformation completed with issues, {records_processed} records processed"
            
#     except Exception as e:
#         logger.error(f"Error during data transformation: {e}")
#         log_etl_metrics(start_time, 0, "transformation_failed")
#         raise
#     finally:
#         logger.info("Cleaning up resources...")
#         # Ensure Spark session is properly closed
#         try:
#             if 'spark' in locals():
#                 spark.stop()
#                 logger.info("Spark session stopped successfully")
#         except Exception as e:
#             logger.error(f"Error stopping Spark session: {e}")

# # Define the DAG
# dag = DAG(
#     dag_id='hanger_line_daily_transform_optimized',
#     default_args=default_args,
#     description='Daily transformation of hanger line data with ETL best practices',
#     schedule='0 2 * * *',  # Run daily at 2:00 AM PKT
#     catchup=False,
#     tags=['ssg', 'line', 'transformation', 'optimized'],
#     max_active_runs=1,
#     # Add SLA for monitoring
#     sla_miss_callback=lambda context: logger.error(f"SLA missed for DAG: {context}")
# )

# # Task definitions
# start_task = PythonOperator(
#     task_id='start',
#     python_callable=log_start,
#     dag=dag
# )

# check_data_task = BranchPythonOperator(
#     task_id='check_for_data',
#     python_callable=check_for_data_enhanced,
#     dag=dag
# )

# has_data_label = EmptyOperator(
#     task_id='has_data',
#     dag=dag
# )

# transform_task = PythonOperator(
#     task_id='transform_data',
#     python_callable=execute_transformation_optimized,
#     dag=dag
# )

# no_data_label = EmptyOperator(
#     task_id='no_data',
#     dag=dag
# )

# skip_task = EmptyOperator(
#     task_id='skip_transformation',
#     dag=dag
# )

# save_task = PythonOperator(
#     task_id='save_completion_status',
#     python_callable=lambda **context: logger.info("Transformation process completed and saved"),
#     dag=dag
# )

# end_task = PythonOperator(
#     task_id='end',
#     python_callable=log_end,
#     dag=dag
# )

# # Set task dependencies
# start_task >> check_data_task
# check_data_task >> has_data_label
# check_data_task >> no_data_label
# has_data_label >> transform_task >> save_task >> end_task
# no_data_label >> skip_task >> end_task