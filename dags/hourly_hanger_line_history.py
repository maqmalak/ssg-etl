"""
Hourly DAG for aggregating hanger line data and upserting to production tables
"""

import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Generator
import logging
import subprocess
import sys
import os
import psutil
import gc

from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
import psycopg2
import os
import sys
import pandas as pd
from pendulum import timezone
from collections import defaultdict
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
# Constants for retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Add the scripts directory to the Python path
scripts_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'scripts')
sys.path.append(os.path.abspath(scripts_path))

# Import upsert utilities
try:
    from dags.upsert_utils import (
        upsert_data_via_postgres,
        create_connection_params_from_airflow
    )
    print("Successfully imported upsert utilities")
except ImportError as e:
    print(f"Error importing upsert utilities: {e}")

# Import table creation functions
try:
    from scripts.create_table_hourly import (
        create_hourly_table_if_not_exists,
        create_etl_hourly_log_odp_table_if_not_exists
    )
    print("Successfully imported table creation functions")
except ImportError as e:
    print(f"Error importing table creation functions: {e}")

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
    'execution_timeout': timedelta(hours=1),
    'retry_exponential_backoff': True,
}

def log_etl_metrics(start_time, records_processed=0, tables_updated=[], status="completed"):
    """
    Log comprehensive ETL metrics
    """
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
    return metrics

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Timezone configuration
PKT = timezone("Asia/Karachi")

# Constants for retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds

# Memory optimization constants
BATCH_SIZE = 1000
MAX_MEMORY_USAGE_PERCENT = 80.0  # Maximum memory usage percentage before triggering cleanup


def get_memory_usage() -> float:
    """Get current memory usage percentage."""
    return psutil.virtual_memory().percent


def log_memory_usage(operation: str) -> None:
    """Log current memory usage."""
    memory_percent = get_memory_usage()
    logger.info(f"[MEMORY] {operation} - Memory usage: {memory_percent:.2f}%")


def perform_memory_cleanup() -> None:
    """Perform garbage collection to free memory."""
    gc.collect()
    logger.info("[MEMORY] Garbage collection performed")


def check_memory_and_cleanup(operation: str) -> None:
    """Check memory usage and perform cleanup if needed."""
    log_memory_usage(operation)
    if get_memory_usage() > MAX_MEMORY_USAGE_PERCENT:
        logger.warning(f"[MEMORY] High memory usage detected during {operation}")
        perform_memory_cleanup()
        log_memory_usage(f"{operation} - After cleanup")


# Global engine cache to reuse connections
_engine_cache = {}

def get_postgres_engine(connection_name: str = "pg-ssg"):
    """
    Create and return a PostgreSQL engine using Airflow connection.
    Reuses existing engines when possible for better performance.
    
    Args:
        connection_name (str): Name of the Airflow connection to use
        
    Returns:
        sqlalchemy.engine.Engine: PostgreSQL engine instance
    """
    # Check if we already have an engine for this connection
    if connection_name in _engine_cache:
        return _engine_cache[connection_name]
    
    try:
        connection = BaseHook.get_connection(connection_name)
        # Properly encode the password to handle special characters like '@'
        from urllib.parse import quote_plus
        password = quote_plus(connection.password) if connection.password else ''
        uri = f"postgresql://{connection.login}:{password}@{connection.host}:{connection.port}/{connection.schema}"
        logger.info(f"Using Airflow connection: {connection.host}:{connection.port}/{connection.schema}")
    except Exception as e:
        logger.warning(f"Could not get {connection_name} connection, using default values: {e}")
        # Fallback to default values for testing
        # Properly encode the password to handle special characters like '@'
        from urllib.parse import quote_plus
        password = quote_plus("P@akistan12")
        uri = f"postgresql://postgres:{password}@172.16.7.6:5432/ssg"
        logger.info("Using fallback connection: 172.16.7.6:5432/ssg")
    
    # Use connection pooling for better performance with optimized settings
    engine = create_engine(
        uri,
        pool_size=10,  # Increased pool size for better concurrency
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_timeout=30,
        echo=False  # Disable SQL logging for performance
    )
    
    # Cache the engine for reuse
    _engine_cache[connection_name] = engine
    return engine

def dispose_postgres_engine(connection_name: str = "pg-ssg"):
    """
    Dispose of a PostgreSQL engine and remove it from the cache.
    
    Args:
        connection_name (str): Name of the Airflow connection
    """
    if connection_name in _engine_cache:
        _engine_cache[connection_name].dispose()
        del _engine_cache[connection_name]
        logger.info(f"Disposed PostgreSQL engine for connection: {connection_name}")


def retry_on_exception(max_retries: int = MAX_RETRIES, delay: int = RETRY_DELAY):
    """
    Decorator to retry a function on exception.
    
    Args:
        max_retries (int): Maximum number of retry attempts
        delay (int): Delay between retries in seconds
    """
    def decorator(func):
        def wrapper(*args, **kwargs):
            last_exception = None
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    last_exception = e
                    logger.warning(
                        f"Attempt {attempt + 1} failed for {func.__name__}: {str(e)}"
                    )
                    if attempt < max_retries - 1:
                        time.sleep(delay)
                    else:
                        logger.error(
                            f"All {max_retries} attempts failed for {func.__name__}"
                        )
                        raise last_exception
            return None
        return wrapper
    return decorator




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

def create_hourly_tables_if_not_exist():
    """
    Create production tables if they don't exist
    """
    try:
        # Import the table creation function
        from scripts.create_table_hourly import create_hourly_table_if_not_exists
        
        # Get connection parameters from Airflow connection
        try:
            connection = BaseHook.get_connection("pg-ssg")
            # Properly encode the password to handle special characters like '@'
            from urllib.parse import quote_plus
            password = quote_plus(connection.password) if connection.password else ''
            uri = f"postgresql://{connection.login}:{password}@{connection.host}:{connection.port}/{connection.schema}"
            logger.info(f"Using Airflow connection: {connection.host}:{connection.port}/{connection.schema}")
        except Exception as e:
            logger.warning(f"Could not get pg-ssg connection, using default values: {e}")
            # Fallback to default values for testing
            # Properly encode the password to handle special characters like '@'
            from urllib.parse import quote_plus
            password = quote_plus("P@kistan12")
            uri = f"postgresql://postgres:{password}@172.16.7.6:5432/ssg"
            logger.info("Using fallback connection: 172.16.7.6:5432/ssg")
        
        # Create engine and create tables
        engine = create_engine(uri)
        create_hourly_table_if_not_exists(engine)
        engine.dispose()
        
        logger.info("Production tables created/verified successfully")
        return "Production tables created/verified successfully"
        
    except Exception as e:
        logger.error(f"Error creating production tables: {e}")
        raise


@retry_on_exception()
def get_last_extract_dt_from_log(source_connection: str) -> Optional[datetime]:
    """
    Get the last extract datetime for a source connection from the ETL log.
    
    Args:
        source_connection (str): Source connection identifier
        
    Returns:
        Optional[datetime]: Last extract datetime or None if not found
    """
    engine = get_postgres_engine("pg-ssg")
    try:
        # Create the ETL log table if it doesn't exist
        create_etl_hourly_log_odp_table_if_not_exists(engine)
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT MAX(lastextractdatetime) FROM etl_qcr_extract_log WHERE status='Completed' and source_connection = :src"
                ),
                {"src": source_connection},
            ).scalar()
            logger.info(f"Last extract datetime for {source_connection}: {result}")
            return result
    except Exception as e:
        logger.error(f"Error fetching last extract datetime for {source_connection}: {e}")
        # If we can't access the log table, return None to trigger full extraction
        return None
    finally:
        dispose_postgres_engine("pg-ssg")


def insert_etl_log(
    processlogid: str,
    source_connection: str,
    saved_count: int,
    starttime: datetime,
    endtime: datetime,
    last_extract_dt: Optional[datetime],
    success: bool,
    status: str,
    errormessage: Optional[str],
) -> None:
    """
    Insert ETL process log into the database.
    
    Args:
        processlogid (str): Unique process ID
        source_connection (str): Source connection identifier
        saved_count (int): Number of records saved
        starttime (datetime): Process start time
        endtime (datetime): Process end time
        last_extract_dt (Optional[datetime]): Last extract datetime
        success (bool): Whether the process was successful
        status (str): Status message
        errormessage (Optional[str]): Error message if any
    """
    engine = get_postgres_engine("pg-ssg")
    try:
        # Create the ETL log table if it doesn't exist
        create_etl_hourly_log_odp_table_if_not_exists(engine)
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO etl_extract_hourly_log 
                    (processlogid, source_connection, saved_count, starttime, endtime, lastextractdatetime, success, status, errormessage)
                    VALUES (:processlogid, :source_connection, :saved_count, :starttime, :endtime, :lastextractdatetime, :success, :status, :errormessage)
                    """
                ),
                {
                    "processlogid": processlogid,
                    "source_connection": conn,
                    "saved_count": saved_count,
                    "starttime": starttime,
                    "endtime": endtime,
                    "lastextractdatetime": last_extract_dt,
                    "success": success,
                    "status": status,
                    "errormessage": errormessage,
                },
            )
        logger.info(f"Inserted ETL log for {conn}")
    except Exception as e:
        logger.error(f"Failed to insert ETL log for {conn}: {e}")
        # Don't raise the exception, just log it
    finally:
        dispose_postgres_engine("pg-ssg")


@retry_on_exception()
def fetch_recent_source_data(hours_back=1):
    """
    Fetch recently added data from the source table for the last N hours
    
    Args:
        hours_back: Number of hours back to fetch data
        
    Returns:
        pandas.DataFrame: DataFrame containing the source data
    """
    start_time = time.time()
    logger.info(f"Fetching recent data from operator_daily_performance for last {hours_back} hours")


    try:
        conn = get_database_connection()
        
        last_extract_dt = get_last_extract_dt_from_log(conn)
        # Select only needed columns instead of SELECT *
        needed_columns = [
            "ODP_Date", "Shift", "ODP_EM_Key", "EM_RFID", "EM_Department", "EM_FirstName", "EM_LastName",
            "ODP_Actual_Clock_In", "ODP_Actual_Clock_Out", "ODP_Shift_Clock_In", "ODP_Shift_Clock_Out",
            "ODP_First_Hanger_Time", "ODP_Last_Hanger_Time", "ODP_Lump_Sum_Payment",
            "ODP_Make_Up_Pay_Rate", "ODP_Last_Hanger_Start_Time", "ODPD_Key", "ODPD_Workstation", "ODPD_WC_Key",
            "ODPD_Quantity", "ODPD_ST_Key", "ST_ID", "ST_Description", "ODPD_Lot_Number", "ODPD_OC_Key",
            "OC_Description", "Loading_Qty", "UnLoading_Qty", "OC_Piece_Rate", "OC_Standard_Time", "ODPD_Standard",
            "ODPD_Actual_Time", "ODPD_PA_Key", "ODPD_Pay_Rate", "ODPD_Piece_Rate", "ODPD_Start_Time",
            "ODPD_CM_Key", "CM_Description", "ODPD_SM_Key", "SM_Description", "ODPD_Normal_Pay_Factor",
            "ODPD_Is_Overtime", "ODPD_Overtime_Factor",
            "ODPD_Actual_Time_From_Reader", "ODPD_STPO_Key", "source_connection", "created_at"
        ]
        
        # Query to fetch recent data (last hour) from source table
        columns_str = ", ".join([f'"{col}"' for col in needed_columns])  # Quote column names
        query = f"""
            SELECT {columns_str} FROM operator_daily_performance
            WHERE "created_at" >= NOW() - INTERVAL '{hours_back} hours'
        """
        
        # Read data into pandas DataFrame
        df = pd.read_sql_query(query, conn)
        
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

def perform_aggregations(df):
    """
    Perform the same aggregations as the Spark job
    
    Args:
        df: pandas DataFrame with source data
        
    Returns:
        dict: Dictionary containing the three aggregated DataFrames
    """
    start_time = time.time()
    logger.info("Performing aggregations on source data")
    
    try:
        if df.empty:
            logger.info("No data to aggregate, returning empty results")
            return {
                'odp_hourly_oc': pd.DataFrame(),
                'odp_hourly_shift': pd.DataFrame(),
                'odp_hourly_employee': pd.DataFrame()
            }
        
        # Log available columns for debugging
        logger.info(f"Available columns in source data: {list(df.columns)}")
        
        # Create a mapping of expected column names to actual column names in the DataFrame
        column_name_mapping = {
            # Source columns (from database table)
            'odp_date': ['ODP_Date', 'odp_date'],
            'shift': ['Shift', 'shift'],
            'odp_em_key': ['ODP_EM_Key', 'odp_em_key'],
            'em_rfid': ['EM_RFID', 'em_rfid'],
            'em_department': ['EM_Department', 'em_department'],
            'em_first_name': ['EM_FirstName', 'em_first_name'],
            'em_last_name': ['EM_LastName', 'em_last_name'],
            'odp_actual_clock_in': ['ODP_Actual_Clock_In', 'odp_actual_clock_in'],
            'odp_actual_clock_out': ['ODP_Actual_Clock_Out', 'odp_actual_clock_out'],
            'odpd_workstation': ['ODPD_Workstation', 'odpd_workstation'],
            'odpd_wc_key': ['ODPD_WC_Key', 'odpd_wc_key'],
            'odpd_quantity': ['ODPD_Quantity', 'odpd_quantity'],
            'odpd_st_key': ['ODPD_ST_Key', 'odpd_st_key'],
            'st_id': ['ST_ID', 'st_id'],
            'st_description': ['ST_Description', 'st_description'],
            'odpd_lot_number': ['ODPD_Lot_Number', 'odpd_lot_number'],
            'odpd_oc_key': ['ODPD_OC_Key', 'odpd_oc_key'],
            'oc_description': ['OC_Description', 'oc_description'],
            'loading_qty': ['Loading_Qty', 'loading_qty'],
            'unloading_qty': ['UnLoading_Qty', 'unloading_qty'],
            'oc_standard_time': ['OC_Standard_Time', 'oc_standard_time'],
            'odpd_actual_time': ['ODPD_Actual_Time', 'odpd_actual_time'],
            'odpd_cm_key': ['ODPD_CM_Key', 'odpd_cm_key'],
            'cm_description': ['CM_Description', 'cm_description'],
            'odpd_sm_key': ['ODPD_SM_Key', 'odpd_sm_key'],
            'sm_description': ['SM_Description', 'sm_description'],
            'odpd_is_overtime': ['ODPD_Is_Overtime', 'odpd_is_overtime'],
            'odpd_overtime_factor': ['ODPD_Overtime_Factor', 'odpd_overtime_factor'],
            'odpd_stpo_key': ['ODPD_STPO_Key', 'odpd_stpo_key'],
            'source_connection': ['source_connection'],
            'record_count': ['record_count']
        }
        
        # Find actual column names in the DataFrame
        actual_column_names = {}
        for expected_name, possible_names in column_name_mapping.items():
            for name in possible_names:
                if name in df.columns:
                    actual_column_names[expected_name] = name
                    break
            if expected_name not in actual_column_names:
                logger.warning(f"Column {expected_name} not found in DataFrame")
        
        logger.info(f"Actual column names mapping: {actual_column_names}")
        logger.info(f"DataFrame columns: {list(df.columns)}")
        
        # Transform 1: Group by Date and Operation Code details
        logger.info("Performing aggregation 1: by Date and Operation Code...")
        agg1_grouping_cols_expected = [
            "odp_date", "shift", "odpd_st_key", "st_id", "st_description",
            "odpd_lot_number", "odpd_oc_key", "oc_description",
            "oc_standard_time", "odpd_actual_time", "odpd_cm_key",
            "cm_description", "odpd_sm_key", "sm_description", "source_connection"
        ]
        
        # Map to actual column names
        actual_agg1_grouping_cols = [actual_column_names[col] for col in agg1_grouping_cols_expected if col in actual_column_names]
        
        # Define aggregation operations with actual column names
        agg_operations_1 = {}
        if 'odpd_quantity' in actual_column_names:
            agg_operations_1[actual_column_names['odpd_quantity']] = 'sum'
        if 'loading_qty' in actual_column_names:
            agg_operations_1[actual_column_names['loading_qty']] = 'sum'
        if 'unloading_qty' in actual_column_names:
            agg_operations_1[actual_column_names['unloading_qty']] = 'sum'
        # Use a column that we know exists for counting
        if 'id' in df.columns and 'id' not in actual_agg1_grouping_cols:
            agg_operations_1['id'] = 'count'
        else:
            # Find a column that's not in the grouping columns for counting
            for col in df.columns:
                if col not in actual_agg1_grouping_cols:
                    agg_operations_1[col] = 'count'
                    break
            else:
                # If all columns are in grouping columns, create a constant column for counting
                df = df.copy()
                df['record_count_temp'] = 1
                agg_operations_1['record_count_temp'] = 'sum'
        
        if actual_agg1_grouping_cols and agg_operations_1:
            aggregated_df1 = df.groupby(actual_agg1_grouping_cols).agg(agg_operations_1).reset_index()
            # Rename count column to record_count
            if 'record_count_temp' in aggregated_df1.columns:
                aggregated_df1 = aggregated_df1.rename(columns={'record_count_temp': 'record_count'})
            else:
                count_col = list(agg_operations_1.keys())[-1]  # Get the last aggregated column (the count)
                if count_col in aggregated_df1.columns:
                    aggregated_df1 = aggregated_df1.rename(columns={count_col: 'record_count'})
        else:
            logger.warning("No valid columns for aggregation 1")
            aggregated_df1 = pd.DataFrame()
        
        # Transform 2: Group by Date and Shift details
        logger.info("Performing aggregation 2: by Date and Shift...")
        agg2_grouping_cols_expected = [
            "odp_date", "shift", "odpd_st_key", "st_id", "st_description",
            "odpd_lot_number", "odpd_oc_key", "oc_description",
            "oc_standard_time", "odpd_actual_time", "odpd_cm_key",
            "cm_description", "odpd_sm_key", "sm_description", "odpd_stpo_key", "source_connection"
        ]
        
        # Map to actual column names
        actual_agg2_grouping_cols = [actual_column_names[col] for col in agg2_grouping_cols_expected if col in actual_column_names]
        
        # Define aggregation operations for aggregation 2
        agg_operations_2 = {}
        if 'odpd_quantity' in actual_column_names:
            agg_operations_2[actual_column_names['odpd_quantity']] = 'sum'
        if 'loading_qty' in actual_column_names:
            agg_operations_2[actual_column_names['loading_qty']] = 'sum'
        if 'unloading_qty' in actual_column_names:
            agg_operations_2[actual_column_names['unloading_qty']] = 'sum'
        if 'odpd_overtime_factor' in actual_column_names:
            agg_operations_2[actual_column_names['odpd_overtime_factor']] = 'mean'
        if 'odpd_is_overtime' in actual_column_names:
            agg_operations_2[actual_column_names['odpd_is_overtime']] = 'max'
        # Use a column for counting
        if 'id' in df.columns and 'id' not in actual_agg2_grouping_cols:
            agg_operations_2['id'] = 'count'
        else:
            # Find a column that's not in the grouping columns for counting
            for col in df.columns:
                if col not in actual_agg2_grouping_cols:
                    agg_operations_2[col] = 'count'
                    break
            else:
                # If all columns are in grouping columns, create a constant column for counting
                df = df.copy()
                df['record_count_temp'] = 1
                agg_operations_2['record_count_temp'] = 'sum'
        
        if actual_agg2_grouping_cols and agg_operations_2:
            aggregated_df2 = df.groupby(actual_agg2_grouping_cols).agg(agg_operations_2).reset_index()
            # Rename count column to record_count
            if 'record_count_temp' in aggregated_df2.columns:
                aggregated_df2 = aggregated_df2.rename(columns={'record_count_temp': 'record_count'})
            else:
                count_col = list(agg_operations_2.keys())[-1]  # Get the last aggregated column (the count)
                if count_col in aggregated_df2.columns:
                    aggregated_df2 = aggregated_df2.rename(columns={count_col: 'record_count'})
        else:
            logger.warning("No valid columns for aggregation 2")
            aggregated_df2 = pd.DataFrame()
        
        # Transform 3: Group by Date and Employee details
        logger.info("Performing aggregation 3: by Date and Employee...")
        agg3_grouping_cols_expected = [
            "odp_date", "shift",
            "odp_em_key", "em_rfid", "em_department", "em_first_name", "em_last_name",
            "odp_current_station", "odpd_workstation", "odpd_wc_key",
            "odpd_st_key", "st_id", "st_description", "odpd_lot_number",
            "odpd_oc_key", "oc_description",
            "odpd_cm_key", "cm_description", "odpd_sm_key", "sm_description",
            "odpd_is_overtime", "odpd_stpo_key",
            "source_connection"
        ]
        
        # Map to actual column names
        actual_agg3_grouping_cols = [actual_column_names[col] for col in agg3_grouping_cols_expected if col in actual_column_names]
        
        # Define aggregation operations for aggregation 3
        agg_operations_3 = {}
        if 'odpd_quantity' in actual_column_names and actual_column_names['odpd_quantity'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['odpd_quantity']] = 'sum'
        if 'loading_qty' in actual_column_names and actual_column_names['loading_qty'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['loading_qty']] = 'sum'
        if 'unloading_qty' in actual_column_names and actual_column_names['unloading_qty'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['unloading_qty']] = 'sum'
        if 'odpd_actual_time' in actual_column_names and actual_column_names['odpd_actual_time'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['odpd_actual_time']] = 'sum'
        if 'odpd_is_overtime' in actual_column_names and actual_column_names['odpd_is_overtime'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['odpd_is_overtime']] = 'max'
        if 'odpd_overtime_factor' in actual_column_names and actual_column_names['odpd_overtime_factor'] not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['odpd_overtime_factor']] = 'mean'
        # Use a column for counting
        if 'id' in df.columns and 'id' not in actual_agg3_grouping_cols:
            agg_operations_3['id'] = 'count'
        else:
            # Find a column that's not in the grouping columns for counting
            for col in df.columns:
                if col not in actual_agg3_grouping_cols:
                    agg_operations_3[col] = 'count'
                    break
            else:
                # If all columns are in grouping columns, create a constant column for counting
                df = df.copy()
                df['record_count_temp'] = 1
                agg_operations_3['record_count_temp'] = 'sum'
        if 'odp_actual_clock_in' in actual_column_names:
            agg_operations_3[actual_column_names['odp_actual_clock_in']] = 'min'
        if 'odp_actual_clock_out' in actual_column_names:
            agg_operations_3[actual_column_names['odp_actual_clock_out']] = 'max'
        
        if actual_agg3_grouping_cols and agg_operations_3:
            aggregated_df3 = df.groupby(actual_agg3_grouping_cols).agg(agg_operations_3).reset_index()
            # Rename count column to record_count
            if 'record_count_temp' in aggregated_df3.columns:
                aggregated_df3 = aggregated_df3.rename(columns={'record_count_temp': 'record_count'})
            else:
                count_col = list(agg_operations_3.keys())[-1]  # Get the last aggregated column (the count)
                if count_col in aggregated_df3.columns:
                    aggregated_df3 = aggregated_df3.rename(columns={count_col: 'record_count'})
        else:
            logger.warning("No valid columns for aggregation 3")
            aggregated_df3 = pd.DataFrame()
        
        # Transform 4: Hourly Summary Aggregation
        logger.info("Performing hourly summary aggregation...")
        
        # Group by hour, date, shift, station, and operation code
        hourly_summary_grouping_cols_expected = [
            "odp_date", "shift", "st_id", "st_description", "oc_description", "source_connection"
        ]
        
        # Map to actual column names
        actual_hourly_grouping_cols = [actual_column_names[col] for col in hourly_summary_grouping_cols_expected if col in actual_column_names]
        
        # Define aggregation operations for hourly summary
        hourly_agg_operations = {}
        if 'odpd_quantity' in actual_column_names:
            hourly_agg_operations[actual_column_names['odpd_quantity']] = 'sum'
        if 'loading_qty' in actual_column_names:
            hourly_agg_operations[actual_column_names['loading_qty']] = 'sum'
        if 'unloading_qty' in actual_column_names:
            hourly_agg_operations[actual_column_names['unloading_qty']] = 'sum'
        if 'odpd_actual_time' in actual_column_names:
            hourly_agg_operations[actual_column_names['odpd_actual_time']] = 'mean'
        # Count distinct employees
        if 'odp_em_key' in actual_column_names:
            hourly_agg_operations[actual_column_names['odp_em_key']] = 'nunique'
        # Add count of records
        # Find a column that's not in the grouping columns for counting
        for col in df.columns:
            if col not in actual_hourly_grouping_cols and col != 'hour_timestamp':
                hourly_agg_operations[col] = 'count'
                break
        else:
            # If all columns are in grouping columns, create a constant column for counting
            df_with_hour = df_with_hour.copy()
            df_with_hour['record_count_temp'] = 1
            hourly_agg_operations['record_count_temp'] = 'sum'
        
        if actual_hourly_grouping_cols and hourly_agg_operations:
            # First, we need to add hour_timestamp to the dataframe
            # We'll add it to the dataframe temporarily based on created_at
            df_with_hour = df.copy()
            # Extract hour from created_at (or use current time if created_at is not available)
            if 'created_at' in df_with_hour.columns:
                df_with_hour['hour_timestamp'] = pd.to_datetime(df_with_hour['created_at']).dt.floor('H')
            else:
                current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
                df_with_hour['hour_timestamp'] = current_hour
            
            # Include hour_timestamp in grouping
            actual_hourly_grouping_cols_with_hour = ['hour_timestamp'] + actual_hourly_grouping_cols
            
            aggregated_df4 = df_with_hour.groupby(actual_hourly_grouping_cols_with_hour).agg(hourly_agg_operations).reset_index()
            
            # Rename columns appropriately
            if actual_column_names.get('odp_em_key') in aggregated_df4.columns:
                aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['odp_em_key']: 'total_employees'})
            
            # Rename count column to record_count if it exists
            if 'record_count_temp' in aggregated_df4.columns:
                aggregated_df4 = aggregated_df4.rename(columns={'record_count_temp': 'record_count'})
            else:
                # Try to find the count column
                count_cols = [col for col in aggregated_df4.columns if col not in actual_hourly_grouping_cols_with_hour]
                if count_cols:
                    count_col = count_cols[-1]  # Get the last non-grouping column
                    aggregated_df4 = aggregated_df4.rename(columns={count_col: 'record_count'})
            
            # Rename other columns to match the database schema
            if actual_column_names.get('odpd_quantity') in aggregated_df4.columns:
                aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['odpd_quantity']: 'total_quantity'})
            if actual_column_names.get('loading_qty') in aggregated_df4.columns:
                aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['loading_qty']: 'total_loading_qty'})
            if actual_column_names.get('unloading_qty') in aggregated_df4.columns:
                aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['unloading_qty']: 'total_unloading_qty'})
            if actual_column_names.get('odpd_actual_time') in aggregated_df4.columns:
                aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['odpd_actual_time']: 'avg_actual_time'})
            if actual_column_names.get('st_id') in aggregated_df4.columns:
                aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['st_id']: 'station_id'})
            if actual_column_names.get('st_description') in aggregated_df4.columns:
                aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['st_description']: 'station_description'})
            if actual_column_names.get('oc_description') in aggregated_df4.columns:
                aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['oc_description']: 'operation_code'})
        else:
            logger.warning("No valid columns for hourly summary aggregation")
            aggregated_df4 = pd.DataFrame()
        
        # Add created_at timestamp to all DataFrames
        current_time = datetime.now()
        for df_agg in [aggregated_df1, aggregated_df2, aggregated_df3, aggregated_df4]:
            if not df_agg.empty:
                df_agg['created_at'] = current_time
        
        agg_results = {
            'odp_hourly_oc': aggregated_df1,
            'odp_hourly_shift': aggregated_df2,
            'odp_hourly_employee': aggregated_df3,
            'odp_hourly_summary': aggregated_df4
        }
        
        logger.info(f"Aggregations completed in {time.time() - start_time:.2f} seconds")
        return agg_results
        
    except Exception as e:
        logger.error(f"Error in aggregations: {e}")
        raise

def vacuum_analyze_table(table_name, connection_params):
    """
    Run VACUUM ANALYZE on a table to update statistics after heavy upserts
    
    Args:
        table_name: Name of the table to vacuum
        connection_params: Database connection parameters
    """
    try:
        # Create connection
        conn = psycopg2.connect(
            host=connection_params.get("host", "pg-ssg"),
            port=connection_params.get("port", "5432"),
            database=connection_params.get("database", "ssg"),
            user=connection_params.get("user", "postgres"),
            password=connection_params.get("password", "P@akistan12")
        )
        # Enable autocommit mode which is required for VACUUM
        conn.autocommit = True
        cursor = conn.cursor()
        
        # Run VACUUM ANALYZE
        cursor.execute(f"VACUUM ANALYZE {table_name};")
        
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully vacuumed and analyzed {table_name}")
        return True
        
    except Exception as e:
        logger.error(f"Error vacuuming table {table_name}: {e}")
        return False


def upsert_aggregated_table(table_name, key_columns, aggregated_data, **context):
    """
    Upsert aggregated data for a specific table
    
    Args:
        table_name: Name of the target table
        key_columns: List of columns that form the primary key
        aggregated_data: pandas DataFrame with aggregated data
    """
    start_time = time.time()
    logger.info(f"Starting upsert process for table: {table_name}")
    
    try:
        if aggregated_data.empty:
            logger.info(f"No data to upsert for {table_name}")
            log_etl_metrics(start_time, 0, [table_name], "no_data")
            return f"No data to upsert for {table_name}"
        
        # Convert DataFrame to list of dictionaries
        data = aggregated_data.to_dict('records')
        
        if not data:
            logger.info(f"No records to upsert for {table_name}")
            log_etl_metrics(start_time, 0, [table_name], "no_records")
            return f"No records to upsert for {table_name}"
        
        # Create a mapping from lowercase column names to database column names
        column_mapping = {
            'odp_date': 'ODP_Date',
            'shift': 'Shift',
            'odp_em_key': 'ODP_EM_Key',
            'em_rfid': 'EM_RFID',
            'em_department': 'EM_Department',
            'em_first_name': 'EM_FirstName',
            'em_last_name': 'EM_LastName',
            'odp_actual_clock_in': 'ODP_Actual_Clock_In',
            'odp_actual_clock_out': 'ODP_Actual_Clock_Out',
            'odp_shift_clock_in': 'ODP_Shift_Clock_In',
            'odp_shift_clock_out': 'ODP_Shift_Clock_Out',
            'odpd_workstation': 'ODPD_Workstation',
            'odpd_wc_key': 'ODPD_WC_Key',
            'odpd_quantity': 'ODPD_Quantity',
            'odpd_st_key': 'ODPD_ST_Key',
            'st_id': 'ST_ID',
            'st_description': 'ST_Description',
            'odpd_lot_number': 'ODPD_Lot_Number',
            'odpd_oc_key': 'ODPD_OC_Key',
            'oc_description': 'OC_Description',
            'loading_qty': 'Loading_Qty',
            'unloading_qty': 'UnLoading_Qty',
            'oc_standard_time': 'OC_Standard_Time',
            'odpd_actual_time': 'ODPD_Actual_Time',
            'odpd_cm_key': 'ODPD_CM_Key',
            'cm_description': 'CM_Description',
            'odpd_sm_key': 'ODPD_SM_Key',
            'sm_description': 'SM_Description',
            'odpd_is_overtime': 'ODPD_Is_Overtime',
            'odpd_overtime_factor': 'ODPD_Overtime_Factor',
            'odpd_stpo_key': 'ODPD_STPO_Key',
            'source_connection': 'source_connection',
            'record_count': 'record_count',
            'created_at': 'created_at',
            'hour_timestamp': 'hour_timestamp',
            'station_id': 'station_id',
            'station_description': 'station_description',
            'operation_code': 'operation_code',
            'total_quantity': 'total_quantity',
            'total_loading_qty': 'total_loading_qty',
            'total_unloading_qty': 'total_unloading_qty',
            'avg_actual_time': 'avg_actual_time',
            'total_employees': 'total_employees'
        }
        
        # Map column names in the data to match database schema
        mapped_data = []
        for record in data:
            mapped_record = {}
            for key, value in record.items():
                # Map the key to the database column name
                mapped_key = column_mapping.get(key.lower(), key)
                mapped_record[mapped_key] = value
            mapped_data.append(mapped_record)
        
        # Map key columns to match database schema
        mapped_key_columns = [column_mapping.get(col.lower(), col) for col in key_columns]
        
        # Get connection parameters
        connection_params = create_connection_params_from_airflow("pg-ssg")
        
        # Perform upsert with mapped data and key columns
        success = upsert_data_via_postgres(mapped_data, table_name, mapped_key_columns, connection_params)
        
        if success:
            logger.info(f"Successfully upserted {len(mapped_data)} records to {table_name}")
            log_etl_metrics(start_time, len(mapped_data), [table_name], "success")
            # Push row count to XCom for monitoring
            if context and 'ti' in context:
                context['ti'].xcom_push(key=f'{table_name}_upserted_count', value=len(mapped_data))
            # Run vacuum analyze after upsert for better performance
            vacuum_analyze_table(table_name, connection_params)
            return f"Successfully upserted {len(mapped_data)} records to {table_name}"
        else:
            logger.error(f"Failed to upsert data to {table_name}")
            log_etl_metrics(start_time, 0, [table_name], "failed")
            return f"Failed to upsert data to {table_name}"
            
    except Exception as e:
        logger.error(f"Error in upsert process for {table_name}: {e}")
        log_etl_metrics(start_time, 0, [table_name], "error")
        raise

def process_hourly_aggregations(**context):
    """
    Main function to fetch data, perform aggregations, and upsert results
    """
    start_time = time.time()
    logger.info("Starting hourly aggregation and upsert process")
    
    try:
        # Fetch recent source data (last 1 hour)
        source_data = fetch_recent_source_data(hours_back=1)
        
        if source_data.empty:
            logger.info("No recent source data found, skipping aggregations")
            log_etl_metrics(start_time, 0, [], "no_source_data")
            return "No recent source data found, skipping aggregations"
        
        # Perform aggregations
        aggregated_results = perform_aggregations(source_data)
        
        # Define key columns for each table (matching the primary keys)
        # Using the actual column names from the database schema
        table_key_columns = {
            'odp_hourly_oc': ['hour_timestamp',
                'ODP_Date', 'Shift', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 'ODPD_Lot_Number',
                'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 'CM_Description', 
                'ODPD_SM_Key', 'SM_Description', 'source_connection'
            ],
            'odp_hourly_shift': ['hour_timestamp',
                'ODP_Date', 'Shift', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 'ODPD_Lot_Number', 
                'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 'CM_Description', 
                'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime', 
                'ODPD_STPO_Key', 'source_connection'
            ],
            'odp_hourly_employee': ['hour_timestamp',
                'ODP_Date', 'Shift', 'ODP_EM_Key', 'EM_RFID', 'EM_Department', 'EM_FirstName', 'EM_LastName',
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
        for table_name, aggregated_data in aggregated_results.items():
            key_columns = table_key_columns.get(table_name, [])
            result = upsert_aggregated_table(table_name, key_columns, aggregated_data)
            results.append(result)
        
        logger.info("Hourly aggregation and upsert process completed successfully")
        log_etl_metrics(start_time, len(source_data), list(aggregated_results.keys()), "success")
        return "Hourly aggregation and upsert process completed successfully"
        
    except Exception as e:
        logger.error(f"Error in hourly aggregation process: {e}")
        log_etl_metrics(start_time, 0, [], "error")
        raise

def log_start(**context):
    """
    Log the start of the DAG execution
    """
    logger.info("Starting hourly_hanger_line_production_upsert DAG execution")
    logger.info(f"Execution date: {context.get('execution_date', 'Unknown')}")
    logger.info(f"Run ID: {context.get('run_id', 'Unknown')}")
    return "DAG execution started"

def log_end(**context):
    """
    Log the end of the DAG execution
    """
    logger.info("Completed hourly_hanger_line_production_upsert DAG execution")
    logger.info(f"Execution date: {context.get('execution_date', 'Unknown')}")
    logger.info(f"Run ID: {context.get('run_id', 'Unknown')}")
    return "DAG execution completed"

# Define the DAG
dag = DAG(
    'hourly_hanger_line_production_history_upsert',
    default_args=default_args,
    description='Hourly aggregation of hanger line data and upsert to production tables',
    schedule='0 * * * *',  # Run hourly at the top of each hour
    catchup=False,
    tags=['ssg', 'line', 'production', 'upsert', 'hourly'],
    max_active_runs=1,
)

# Task definitions
start_task = PythonOperator(
    task_id='start',
    python_callable=log_start,
    dag=dag
)

# Task to create production tables if they don't exist
create_tables_task = PythonOperator(
    task_id='create_production_tables',
    python_callable=create_hourly_tables_if_not_exist,
    dag=dag
)

# Main processing task
process_task = PythonOperator(
    task_id='process_hourly_aggregations',
    python_callable=process_hourly_aggregations,
    dag=dag
)

end_task = PythonOperator(
    task_id='end',
    python_callable=log_end,
    dag=dag
)

# Set task dependencies
start_task >> create_tables_task >> process_task >> end_task