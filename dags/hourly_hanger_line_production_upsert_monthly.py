"""
Hourly DAG for aggregating hanger line data and upserting to production tables
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
from sqlalchemy import create_engine
from collections import defaultdict


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
        process_log_id = f"hourly_hanger_line_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        log_etl_extraction(process_log_id, "pg-ssg", records_processed, start_time, status)
        
        return True
        
    except Exception as e:
        logger.error(f"Error logging ETL metrics: {e}")
        return False

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

def create_production_tables_if_not_exist():
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

def fetch_recent_source_data(days_back=30):
    """
    Fetch recently added data from the source table for the last N hours
    
    Args:
        days_back: Number of hours back to fetch data
        
    Returns:
        pandas.DataFrame: DataFrame containing the source data
    """
    start_time = time.time()
    logger.info(f"Fetching recent data from operator_daily_performance for last {days_back} hours")
    
    try:
        conn = get_database_connection()
        
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
            WHERE "created_at" >= NOW() - INTERVAL '{days_back} days'
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
                'odp_hourly_employee': pd.DataFrame(),
                'odp_hourly_summary': pd.DataFrame()
            }
        
        # Log available columns for debugging
        logger.info(f"Available columns in source data: {list(df.columns)}")
        
        # Create a mapping of expected column names to actual column names in the DataFrame
        column_name_mapping = {
            # Source columns (from database table)
            'odpd_key': ['ODPD_Key', 'odpd_key'],
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
        if 'odpd_key' in df.columns and 'odpd_key' not in actual_agg1_grouping_cols:
            agg_operations_1['odpd_key'] = 'count'
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
                # Safely find and rename the count column
                # Look for columns that were likely used for counting
                count_candidates = [col for col in aggregated_df1.columns 
                                  if col in agg_operations_1.keys() and 
                                     agg_operations_1[col] in ['count', 'sum', 'size']]
                if count_candidates:
                    # Use the last count candidate (usually the one we intended)
                    count_col = count_candidates[-1]
                    aggregated_df1 = aggregated_df1.rename(columns={count_col: 'record_count'})
                else:
                    # If we can't find a proper count column, add a default one
                    aggregated_df1['record_count'] = 1
                    logger.warning("Could not find count column in agg1, added default record_count=1")
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
        if 'odpd_key' in df.columns and 'odpd_key' not in actual_agg2_grouping_cols:
            agg_operations_2['odpd_key'] = 'count'
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
                # Safely find and rename the count column
                # Look for columns that were likely used for counting
                count_candidates = [col for col in aggregated_df2.columns 
                                  if col in agg_operations_2.keys() and 
                                     agg_operations_2[col] in ['count', 'sum', 'size']]
                if count_candidates:
                    # Use the last count candidate (usually the one we intended)
                    count_col = count_candidates[-1]
                    aggregated_df2 = aggregated_df2.rename(columns={count_col: 'record_count'})
                else:
                    # If we can't find a proper count column, add a default one
                    aggregated_df2['record_count'] = 1
                    logger.warning("Could not find count column in agg2, added default record_count=1")
        else:
            logger.warning("No valid columns for aggregation 2")
            aggregated_df2 = pd.DataFrame()
        
        # Transform 3: Group by Date and Employee details (CORRECTED VERSION)
        logger.info("Performing aggregation 3: by Date and Employee (CORRECTED)...")
        
        # Correct grouping columns that match the target table schema
        agg3_grouping_cols_expected = [
            "odp_date", "shift",
            "odp_em_key",  # This stays as a grouping column
            "odpd_workstation", "odpd_wc_key",
            "odpd_st_key", "st_id", "st_description", "odpd_lot_number",
            "odpd_oc_key", "oc_description",
            "odpd_cm_key", "cm_description", "odpd_sm_key", "sm_description",
            "odpd_is_overtime", "odpd_stpo_key",
            "source_connection"
        ]
        
        # Map to actual column names
        actual_agg3_grouping_cols = [actual_column_names[col] for col in agg3_grouping_cols_expected if col in actual_column_names]
        
        # Add EM_Description creation logic to the aggregation operations
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
        if 'odpd_key' in df.columns and 'odpd_key' not in actual_agg3_grouping_cols:
            agg_operations_3[actual_column_names['odpd_key']] = 'count'
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
            # FIX: Filter out problematic columns with all null values
            # Specifically, we know 'ODP_Current_Station' has all null values in our data
            filtered_agg3_grouping_cols = [col for col in actual_agg3_grouping_cols if col != 'ODP_Current_Station']
            
            # Create EM_Description column before filtering
            # Combine employee-related information into a single descriptive column
            employee_info_cols = ['ODP_EM_Key', 'EM_RFID', 'EM_Department', 'EM_FirstName', 'EM_LastName']
            employee_cols_present = [col for col in employee_info_cols if col in df.columns]
            
            if employee_cols_present:
                # Create EM_Description by combining available employee columns
                df['EM_Description'] = ''
                for i, col in enumerate(employee_cols_present):
                    if i == 0:
                        df['EM_Description'] = df[col].fillna('').astype(str)
                    else:
                        df['EM_Description'] = df['EM_Description'] + '-' + df[col].fillna('').astype(str)
            else:
                # If no employee columns available, use a default
                df['EM_Description'] = 'Unknown_Employee'
            
            # Filter out rows with null values in the grouping columns
            filtered_df3 = df.dropna(subset=filtered_agg3_grouping_cols)
            if len(filtered_df3) > 0:
                # Perform the aggregation
                aggregated_df3 = filtered_df3.groupby(filtered_agg3_grouping_cols).agg(agg_operations_3).reset_index()
                
                # Rename count column to record_count
                if 'record_count_temp' in aggregated_df3.columns:
                    aggregated_df3 = aggregated_df3.rename(columns={'record_count_temp': 'record_count'})
                else:
                    # Safely find and rename the count column
                    # Look for columns that were likely used for counting
                    count_candidates = [col for col in aggregated_df3.columns 
                                      if col in agg_operations_3.keys() and 
                                         agg_operations_3[col] in ['count', 'sum', 'size']]
                    if count_candidates:
                        # Use the last count candidate (usually the one we intended)
                        count_col = count_candidates[-1]
                        aggregated_df3 = aggregated_df3.rename(columns={count_col: 'record_count'})
                    else:
                        # If we can't find a proper count column, add a default one
                        aggregated_df3['record_count'] = 1
                        logger.warning("Could not find count column, added default record_count=1")
                
                # Transform data to match target table structure
        if not aggregated_df3.empty:
            logger.info("Transforming employee aggregation data to match target table structure...")
            
            # 1. Create EM_Description column by combining employee columns
            employee_id_cols = ['ODP_EM_Key', 'EM_RFID', 'EM_Department', 'EM_FirstName', 'EM_LastName']
            employee_cols_present = [col for col in employee_id_cols if col in aggregated_df3.columns]
            
            if employee_cols_present:
                # Create EM_Description by combining available employee columns
                desc_parts = []
                for col in employee_cols_present:
                    desc_parts.append(aggregated_df3[col].fillna('').astype(str))
                
                # Combine all parts with '-' separator
                aggregated_df3['EM_Description'] = desc_parts[0]  # Start with first column
                for part in desc_parts[1:]:
                    aggregated_df3['EM_Description'] = aggregated_df3['EM_Description'] + '-' + part
                
                # Drop individual employee columns since we now have EM_Description
                cols_to_drop = [col for col in employee_cols_present if col != 'ODP_EM_Key']  # Keep ODP_EM_Key
                aggregated_df3 = aggregated_df3.drop(columns=cols_to_drop, errors='ignore')
                logger.info(f"Dropped individual employee columns: {cols_to_drop}")
            else:
                # If no employee columns available, create a default EM_Description
                aggregated_df3['EM_Description'] = 'Unknown_Employee'
                logger.info("Created default EM_Description column")
            
            # 2. Handle quantity columns
            # If we have ODPD_Quantity but not Loading_Qty/UnLoading_Qty, 
            # distribute ODPD_Quantity appropriately or just drop it
            if 'ODPD_Quantity' in aggregated_df3.columns:
                if 'Loading_Qty' not in aggregated_df3.columns and 'UnLoading_Qty' not in aggregated_df3.columns:
                    # If Loading_Qty and UnLoading_Qty don't exist, we can't use ODPD_Quantity
                    # Just drop it since the target table doesn't expect it
                    aggregated_df3 = aggregated_df3.drop(columns=['ODPD_Quantity'], errors='ignore')
                    logger.info("Dropped ODPD_Quantity column (not needed for target table)")
                # If Loading_Qty and UnLoading_Qty do exist, ODPD_Quantity is redundant, so drop it
                elif 'Loading_Qty' in aggregated_df3.columns or 'UnLoading_Qty' in aggregated_df3.columns:
                    aggregated_df3 = aggregated_df3.drop(columns=['ODPD_Quantity'], errors='ignore')
                    logger.info("Dropped redundant ODPD_Quantity column")
            
            # 3. Ensure all required columns are present with proper names
            required_columns = {
                'ODP_EM_Key', 'EM_Description', 'ODPD_Workstation', 'ODPD_WC_Key', 
                'ODPD_ST_Key', 'ST_ID', 'ST_Description', 'ODPD_Lot_Number', 
                'ODPD_OC_Key', 'OC_Description', 'Loading_Qty', 'UnLoading_Qty', 
                'OC_Standard_Time', 'ODPD_Actual_Time', 'ODPD_CM_Key', 'CM_Description', 
                'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime', 'ODPD_Overtime_Factor', 
                'ODPD_STPO_Key', 'source_connection'
            }
            
            # Add missing required columns with default values
            for col in required_columns:
                if col not in aggregated_df3.columns:
                    # Add column with appropriate default based on data type
                    if col.endswith(('_Qty', '_Key', '_STPO_Key', '_CM_Key', '_SM_Key', '_OC_Key')):
                        aggregated_df3[col] = 0
                    elif col.endswith(('_Factor', '_Time', '_Standard_Time', '_Actual_Time')):
                        aggregated_df3[col] = 0.0
                    elif col.endswith(('_Is_Overtime',)):
                        aggregated_df3[col] = False
                    else:
                        aggregated_df3[col] = ''
                    logger.info(f"Added missing required column '{col}' with default values")
            
            # 4. Add timestamp columns if missing
            if 'hour_timestamp' not in aggregated_df3.columns:
                # Use current hour as default
                current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
                aggregated_df3['hour_timestamp'] = current_hour
                logger.info("Added missing hour_timestamp column with current hour")
            
            if 'created_at' not in aggregated_df3.columns:
                aggregated_df3['created_at'] = datetime.now()
                logger.info("Added missing created_at column with current timestamp")
            
            if 'record_count' not in aggregated_df3.columns:
                # Add record_count with proper integer values (default to 1 for each record)
                aggregated_df3['record_count'] = 1
                logger.info("Added missing record_count column with default integer value")
            
            # 5. Ensure only target table columns are included
            target_columns = {
                'hour_timestamp', 'ODP_Date', 'Shift', 'ODP_EM_Key', 'EM_Description',
                'ODPD_Workstation', 'ODPD_WC_Key', 'ODPD_ST_Key', 'ST_ID', 'ST_Description',
                'ODPD_Lot_Number', 'ODPD_OC_Key', 'OC_Description', 'Loading_Qty',
                'UnLoading_Qty', 'OC_Standard_Time', 'ODPD_Actual_Time', 'ODPD_CM_Key',
                'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime',
                'ODPD_Overtime_Factor', 'ODPD_STPO_Key', 'source_connection',
                'record_count', 'created_at'
            }
            
            # Remove any columns that are not in the target table
            extra_columns = set(aggregated_df3.columns) - target_columns
            if extra_columns:
                aggregated_df3 = aggregated_df3.drop(columns=list(extra_columns), errors='ignore')
                logger.info(f"Dropped extra columns not in target table: {extra_columns}")
            
            logger.info(f"Final employee aggregation data shape: {aggregated_df3.shape}")
            logger.info(f"Final columns: {list(aggregated_df3.columns)}")
        else:
            logger.info("No employee aggregation data to transform")
        
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
            df_with_hour = df.copy()
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
            
            # Filter out rows with null values in grouping columns
            filtered_df4 = df_with_hour.dropna(subset=actual_hourly_grouping_cols_with_hour)
            if len(filtered_df4) > 0:
                aggregated_df4 = filtered_df4.groupby(actual_hourly_grouping_cols_with_hour).agg(hourly_agg_operations).reset_index()
                
                # Rename columns appropriately
                if actual_column_names.get('odp_em_key') in aggregated_df4.columns:
                    aggregated_df4 = aggregated_df4.rename(columns={actual_column_names['odp_em_key']: 'total_employees'})
                
                # Rename count column to record_count if it exists
                if 'record_count_temp' in aggregated_df4.columns:
                    aggregated_df4 = aggregated_df4.rename(columns={'record_count_temp': 'record_count'})
                else:
                    # Safely find and rename the count column
                    # Look for columns that were likely used for counting
                    count_candidates = [col for col in aggregated_df4.columns 
                                      if col in hourly_agg_operations.keys() and 
                                         hourly_agg_operations[col] in ['count', 'sum', 'size']]
                    if count_candidates:
                        # Use the last count candidate (usually the one we intended)
                        count_col = count_candidates[-1]
                        aggregated_df4 = aggregated_df4.rename(columns={count_col: 'record_count'})
                    else:
                        # If we can't find a proper count column, add a default one
                        aggregated_df4['record_count'] = 1
                        logger.warning("Could not find count column in agg4, added default record_count=1")
                
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
                logger.warning("No valid data for hourly summary aggregation after filtering nulls")
                aggregated_df4 = pd.DataFrame()
        else:
            logger.warning("No valid columns for hourly summary aggregation")
            aggregated_df4 = pd.DataFrame()
        
        # Add created_at timestamp to all DataFrames and handle null values
        current_time = datetime.now()
        for df_agg in [aggregated_df1, aggregated_df2, aggregated_df3, aggregated_df4]:
            if not df_agg.empty:
                # Fill NaN values in record_count column with 0
                if 'record_count' in df_agg.columns:
                    df_agg['record_count'] = df_agg['record_count'].fillna(0)
                # Set created_at timestamp
                df_agg['created_at'] = current_time
                # Add hour_timestamp if not already present (should be added in hourly summary)
                if 'hour_timestamp' not in df_agg.columns:
                    # For non-hourly summary tables, we need to add hour_timestamp
                    # We'll use the current hour for these tables
                    current_hour = datetime.now().replace(minute=0, second=0, microsecond=0)
                    df_agg['hour_timestamp'] = current_hour
        
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
    process_log_id = f"hourly_hanger_line_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    
    logger.info("Starting hourly aggregation and upsert process")
    logger.info(f"Process log ID: {process_log_id}")
    
    try:
        # Fetch recent source data (last 1 hour)
        source_data = fetch_recent_source_data(days_back=30)
        
        if source_data.empty:
            logger.info("No recent source data found, skipping aggregations")
            log_etl_metrics(start_time, 0, [], "no_source_data")
            
            # Log to etl_extract_hourly_log table even when no data
            log_etl_extraction(process_log_id, "pg-ssg", 0, start_time, "no_data", "No recent source data found")
            
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
                'ODP_Date', 'Shift', 'ODP_EM_Key', 'EM_Description',
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
        total_records_upserted = 0
        tables_with_data = []
        
        for table_name, aggregated_data in aggregated_results.items():
            key_columns = table_key_columns.get(table_name, [])
            if not aggregated_data.empty:
                result = upsert_aggregated_table(table_name, key_columns, aggregated_data)
                results.append(result)
                record_count = len(aggregated_data)
                total_records_upserted += record_count
                tables_with_data.append(table_name)
                logger.info(f"Upserted {record_count} records to {table_name}")
            else:
                logger.info(f"No data to upsert for {table_name}")
        
        logger.info("Hourly aggregation and upsert process completed successfully")
        
        # Log metrics and to etl_extract_hourly_log table
        log_etl_metrics(start_time, total_records_upserted, tables_with_data, "success")
        log_etl_extraction(process_log_id, "pg-ssg", total_records_upserted, start_time, "success")
        
        return "Hourly aggregation and upsert process completed successfully"
        
    except Exception as e:
        logger.error(f"Error in hourly aggregation process: {e}")
        
        # Log error to etl_extract_hourly_log table
        log_etl_extraction(process_log_id, "pg-ssg", 0, start_time, "error", str(e))
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
    'hourly_hanger_line_production_upsert_monthly',
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
    python_callable=create_production_tables_if_not_exist,
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