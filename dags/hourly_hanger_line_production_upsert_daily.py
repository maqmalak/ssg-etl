#!/usr/bin/env python3
"""
Daily DAG for processing historical hanger line data and upserting to production tables.
This DAG processes missing historical data from operator_daily_performance based on 
the last_extract_datetime in etl_extract_hourly_log table.
"""

import time
import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import psycopg2
import os
import sys
import pandas as pd
from pendulum import timezone

# Timezone configuration
PKT = timezone("Asia/Karachi")

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 9, 15, tzinfo=PKT),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),  # Longer timeout for historical processing
    'retry_exponential_backoff': True,
}

def get_database_connection():
    """
    Get database connection with proper error handling
    """
    try:
        # Get connection parameters from Airflow connection
        try:
            connection = BaseHook.get_connection("pg-ssg")
            host = connection.host
            port = connection.port if connection.port else 5432
            database = connection.schema
            user = connection.login
            password = connection.password
            
            logger.info(f"Using Airflow connection 'pg-ssg'")
        except Exception as e:
            logger.warning(f"Could not get Airflow connection 'pg-ssg', using environment variables: {e}")
            # Fallback to environment variables
            host = os.getenv("POSTGRES_HOST", "172.16.7.6")
            port = os.getenv("POSTGRES_PORT", "5432")
            database = os.getenv("POSTGRES_DB", "ssg")
            user = os.getenv("POSTGRES_USER", "postgres")
            password = os.getenv("POSTGRES_PASSWORD", "P@kistan12")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=host,
            port=port,
            database=database,
            user=user,
            password=password,
            connect_timeout=30  # Add connection timeout
        )
        
        return conn
        
    except Exception as e:
        logger.error(f"Database connection failed: {e}")
        raise

def get_last_extract_datetime():
    """
    Get the last extract datetime from etl_extract_hourly_log table
    
    Returns:
        datetime: Last extract datetime or None if not found
    """
    try:
        # Create connection
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Get the maximum lastextractdatetime from the log table for successful extractions
        cursor.execute("""
            SELECT MAX(lastextractdatetime) 
            FROM etl_extract_hourly_log 
            WHERE success = TRUE
        """)
        
        result = cursor.fetchone()
        last_extract_dt = result[0] if result and result[0] else None
        
        cursor.close()
        conn.close()
        
        if last_extract_dt:
            logger.info(f"Last successful extract datetime: {last_extract_dt}")
        else:
            logger.info("No successful extracts found in log table")
            
        return last_extract_dt
        
    except Exception as e:
        logger.error(f"Error getting last extract datetime: {e}")
        return None

def fetch_historical_source_data(from_datetime=None, to_datetime=None):
    """
    Fetch historical data from operator_daily_performance table based on datetime range
    
    Args:
        from_datetime: Start datetime for data fetch (inclusive)
        to_datetime: End datetime for data fetch (exclusive)
        
    Returns:
        pandas.DataFrame: DataFrame containing the source data
    """
    start_time = time.time()
    logger.info(f"Fetching historical data from operator_daily_performance from {from_datetime} to {to_datetime}")
    
    try:
        conn = get_database_connection()
        
        # Select only needed columns instead of SELECT *
        needed_columns = [
            "ODP_Date", "Shift", "ODP_EM_Key", "EM_RFID", "EM_Department", "EM_FirstName", "EM_LastName",
            "ODP_Actual_Clock_In", "ODP_Actual_Clock_Out", "ODP_Shift_Clock_In", "ODP_Shift_Clock_Out",
            "ODP_First_Hanger_Time", "ODP_Last_Hanger_Time", "ODP_Current_Station", "ODP_Lump_Sum_Payment",
            "ODP_Make_Up_Pay_Rate", "ODP_Last_Hanger_Start_Time", "ODPD_Key", "ODPD_Workstation", "ODPD_WC_Key",
            "ODPD_Quantity", "ODPD_ST_Key", "ST_ID", "ST_Description", "ODPD_Lot_Number", "ODPD_OC_Key",
            "OC_Description", "Loading_Qty", "UnLoading_Qty", "OC_Piece_Rate", "OC_Standard_Time", "ODPD_Standard",
            "ODPD_Actual_Time", "ODPD_PA_Key", "ODPD_Pay_Rate", "ODPD_Piece_Rate", "ODPD_Start_Time", "ODPD_CM_Key",
            "CM_Description", "ODPD_SM_Key", "SM_Description", "ODPD_Normal_Pay_Factor", "ODPD_Is_Overtime",
            "ODPD_Overtime_Factor", "ODPD_Actual_Time_From_Reader", "ODPD_STPO_Key", "source_connection", "created_at"
        ]
        
        # Query to fetch historical data based on datetime range
        columns_str = ", ".join([f'"{col}"' for col in needed_columns])  # Quote column names
        query = f"""
            SELECT {columns_str} FROM operator_daily_performance
            WHERE "created_at" >= %s
        """
        params = [from_datetime]
        
        # Add end time filter if provided
        if to_datetime:
            query += " AND \"created_at\" < %s"
            params.append(to_datetime)
            
        query += " ORDER BY \"created_at\" ASC"
        
        # Read data into pandas DataFrame
        df = pd.read_sql_query(query, conn, params=params)
        
        conn.close()
        
        logger.info(f"Fetched {len(df)} records from operator_daily_performance in {time.time() - start_time:.2f} seconds")
        if not df.empty:
            logger.info(f"Column names in fetched data: {list(df.columns)}")
        else:
            logger.info("No data fetched from operator_daily_performance")
        return df
        
    except Exception as e:
        logger.error(f"Error fetching data from operator_daily_performance: {e}")
        return pd.DataFrame()

def process_historical_aggregations(**context):
    """
    Main function to fetch historical data, perform aggregations, and upsert results
    
    Args:
        **context: Airflow context dictionary
    """
    start_time = time.time()
    process_log_id = f"daily_historical_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("Starting historical hourly aggregation and upsert process")
    logger.info(f"Process log ID: {process_log_id}")
    
    try:
        # Get connection parameters directly
        connection_params = {
            "host": "172.16.7.6",
            "port": "5432",
            "database": "ssg",
            "user": "postgres",
            "password": "P@kistan12"
        }
        
        # Get last extract datetime
        last_extract_dt = get_last_extract_datetime()
        
        if last_extract_dt is None:
            logger.info("No last extract datetime found, using yesterday as default")
            last_extract_dt = datetime.now() - timedelta(days=15)
        
        # Calculate time range for historical processing
        # Process data from last extract to now (or a reasonable time window)
        from_datetime = last_extract_dt
        to_datetime = datetime.now()
        
        logger.info(f"Processing historical data from {from_datetime} to {to_datetime}")
        
        # Fetch historical source data
        source_data = fetch_historical_source_data(from_datetime=from_datetime, to_datetime=to_datetime)
        
        if source_data.empty:
            logger.info("No historical source data found, skipping aggregations")
            log_etl_metrics(start_time, 0, [], "no_source_data")
            
            # Log to etl_extract_hourly_log table even when no data
            log_etl_extraction(process_log_id, "pg-ssg", 0, start_time, "no_data", "No historical source data found")
            
            return "No historical source data found, skipping aggregations"
        
        # Import the aggregation functions here to avoid import issues at DAG parsing time
        sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'scripts')))
        from dags.hourly_hanger_line_production_upsert import perform_aggregations
        
        # Perform aggregations
        aggregated_results = perform_aggregations(source_data)
        
        # Log the results
        logger.info("Aggregation results:")
        total_records = 0
        tables_with_data = []
        for table_name, df in aggregated_results.items():
            logger.info(f"  {table_name}: {len(df)} records")
            total_records += len(df)
            if not df.empty:
                tables_with_data.append(table_name)
            
        # Import upsert utilities
        from dags.upsert_utils import upsert_data_via_postgres
        
        # Define key columns for each table (matching the actual primary keys from database)
        table_key_columns = {
            'odp_hourly_oc': [
                'hour_timestamp', 'ODP_Date', 'Shift', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 
                'ODPD_Lot_Number', 'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 
                'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'source_connection'
            ],
            'odp_hourly_shift': [
                'hour_timestamp', 'ODP_Date', 'Shift', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 
                'ODPD_Lot_Number', 'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 
                'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime', 
                'ODPD_STPO_Key', 'source_connection'
            ],
            'odp_hourly_employee': [
                'hour_timestamp', 'ODP_Date', 'Shift', 'ODP_EM_Key', 'EM_Description',
                'ODPD_Workstation', 'ODPD_WC_Key', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 
                'ODPD_Lot_Number', 'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 
                'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime', 
                'ODPD_STPO_Key', 'source_connection'
            ],
            'odp_hourly_summary': [
                'hour_timestamp', 'ODP_Date', 'Shift', 'station_id', 'station_description', 
                'operation_code', 'source_connection'
            ]
        }
        
        # Upsert each aggregated result
        results = []
        total_upserted = 0
        for table_name, aggregated_data in aggregated_results.items():
            if not aggregated_data.empty:
                key_columns = table_key_columns.get(table_name, [])
                # Convert DataFrame to list of dictionaries
                data = aggregated_data.to_dict('records')
                result = upsert_data_via_postgres(data, table_name, key_columns, connection_params)
                results.append(result)
                upserted_count = len(data)
                total_upserted += upserted_count
                logger.info(f"Upserted {upserted_count} records to {table_name}: {result}")
            else:
                logger.info(f"No data to upsert for {table_name}")
        
        logger.info("Historical hourly aggregation and upsert process completed successfully")
        
        # Log metrics and to etl_extract_hourly_log table
        log_etl_metrics(start_time, total_upserted, tables_with_data, "success")
        log_etl_extraction(process_log_id, "pg-ssg", total_upserted, start_time, "success")
        
        return "Historical hourly aggregation and upsert process completed successfully"
        
    except Exception as e:
        logger.error(f"Error in historical hourly aggregation process: {e}")
        
        # Log error to etl_extract_hourly_log table
        log_etl_extraction(process_log_id, "pg-ssg", 0, start_time, "error", str(e))
        log_etl_metrics(start_time, 0, [], "error")
        
        raise

def log_start(**context):
    """
    Log the start of the DAG execution
    """
    logger.info("Starting hourly_hanger_line_production_upsert_daily DAG execution")
    logger.info(f"Execution date: {context.get('execution_date', 'Unknown')}")
    logger.info(f"Run ID: {context.get('run_id', 'Unknown')}")
    return "DAG execution started"

def log_end(**context):
    """
    Log the end of the DAG execution
    """
    logger.info("Completed hourly_hanger_line_production_upsert_daily DAG execution")
    logger.info(f"Execution date: {context.get('execution_date', 'Unknown')}")
    logger.info(f"Run ID: {context.get('run_id', 'Unknown')}")
    return "DAG execution completed"


def log_etl_extraction(
    process_log_id: str,
    source_connection: str,
    extracted_count: int,
    start_time: float,
    status: str = "success",
    error_message: str = None
) -> bool:
    """
    Log ETL extraction details to etl_extract_hourly_log table
    
    Args:
        process_log_id: Unique identifier for this ETL process
        source_connection: Source connection name
        extracted_count: Number of records extracted
        start_time: Start time of the ETL process (for calculating duration)
        status: Status of the extraction (success, failed, etc.)
        error_message: Error message if any
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        # Calculate duration
        end_time = time.time()
        duration = end_time - start_time
        
        # Create connection
        conn = get_database_connection()
        cursor = conn.cursor()
        
        # Insert log entry
        insert_query = """
            INSERT INTO etl_extract_hourly_log (
                processlogid, source_connection, saved_count, starttime, endtime,
                lastextractdatetime, success, status, errormessage
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Current timestamps
        current_time = datetime.now()
        
        cursor.execute(insert_query, (
            process_log_id,
            source_connection,
            extracted_count,
            datetime.fromtimestamp(start_time),
            current_time,
            current_time,  # lastextractdatetime - using current time as last extract time
            status == "success",
            status,
            error_message
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully logged ETL extraction: {process_log_id} - {extracted_count} records - {status}")
        return True
        
    except Exception as e:
        logger.error(f"Error logging ETL extraction: {e}")
        return False


def log_etl_metrics(
    start_time: float, 
    records_processed: int, 
    tables_updated: list, 
    status: str = "completed"
) -> bool:
    """
    Log comprehensive ETL metrics to monitoring systems
    
    Args:
        start_time: Start time of the ETL process
        records_processed: Number of records processed
        tables_updated: List of tables that were updated
        status: Status of the ETL process
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        end_time = time.time()
        duration = end_time - start_time
        
        metrics = {
            'execution_time_seconds': round(duration, 2),
            'records_processed': records_processed,
            'tables_updated': tables_updated,
            'status': status,
            'throughput_rps': round(records_processed / duration, 2) if duration > 0 and records_processed > 0 else 0,
            'timestamp': datetime.now().isoformat()
        }
        
        logger.info(f"ETL Metrics: {metrics}")
        
        # Also log to etl_extract_hourly_log table
        process_log_id = f"daily_historical_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        log_etl_extraction(process_log_id, "pg-ssg", records_processed, start_time, status)
        
        return True
        
    except Exception as e:
        logger.error(f"Error logging ETL metrics: {e}")
        return False

# Define the DAG
dag = DAG(
    'hourly_hanger_line_production_upsert_daily',
    default_args=default_args,
    description='Daily processing of historical hanger line data and upsert to production tables based on etl_extract_hourly_log',
    schedule='0 2 * * *',  # Run daily at 2 AM
    catchup=False,
    tags=['ssg', 'line', 'production', 'upsert', 'daily', 'historical'],
    max_active_runs=1,
)

# Task definitions
start_task = PythonOperator(
    task_id='start',
    python_callable=log_start,
    dag=dag
)

# Main processing task
process_task = PythonOperator(
    task_id='process_historical_aggregations',
    python_callable=process_historical_aggregations,
    dag=dag
)

end_task = PythonOperator(
    task_id='end',
    python_callable=log_end,
    dag=dag
)

# Set task dependencies
start_task >> process_task >> end_task