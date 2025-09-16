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
    from scripts.create_target_production_table_pg import (
        create_target_table_if_not_exists
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
        from scripts.create_target_production_table_pg import create_target_table_if_not_exists
        
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
        create_target_table_if_not_exists(engine)
        engine.dispose()
        
        logger.info("Production tables created/verified successfully")
        return "Production tables created/verified successfully"
        
    except Exception as e:
        logger.error(f"Error creating production tables: {e}")
        raise

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
        
        # Select only needed columns instead of SELECT *
        needed_columns = [
            "ODP_Date", "Shift", "ODP_EM_Key", "EM_RFID", "EM_Department", "EM_FirstName", "EM_LastName",
            "ODP_Actual_Clock_In", "ODP_Actual_Clock_Out", "ODP_Shift_Clock_In", "ODP_Shift_Clock_Out",
            "ODP_First_Hanger_Time", "ODP_Last_Hanger_Time", "ODP_Current_Station", "ODP_Lump_Sum_Payment",
            "ODP_Make_Up_Pay_Rate", "ODP_Last_Hanger_Start_Time", "ODPD_Key", "ODPD_Workstation", "ODPD_WC_Key",
            "ODPD_Quantity", "ODPD_ST_Key", "ST_ID", "ST_Description", "ODPD_Lot_Number", "ODPD_OC_Key",
            "OC_Description", "Loading_Qty", "UnLoading_Qty", "OC_Piece_Rate", "OC_Standard_Time", "ODPD_Standard",
            "ODPD_Actual_Time", "ODPD_PA_Key", "ODPD_Pay_Rate", "ODPD_Piece_Rate", "ODPD_Start_Time",
            "ODPD_CM_Key", "CM_Description", "ODPD_SM_Key", "SM_Description", "ODPD_Normal_Pay_Factor",
            "ODPD_Is_Overtime", "ODPD_Overtime_Factor", "ODPD_Edited_By", "ODPD_Edited_Date",
            "ODPD_Actual_Time_From_Reader", "ODPD_STPO_Key", "source_connection", "created_at"
        ]
        
        # Query to fetch recent data (last hour) from source table
        columns_str = ", ".join(needed_columns)
        query = f"""
            SELECT {columns_str} FROM operator_daily_performance
            WHERE created_at >= NOW() - INTERVAL '{hours_back} hours'
        """
        
        # Read data into pandas DataFrame
        df = pd.read_sql_query(query, conn)
        
        conn.close()
        
        logger.info(f"Fetched {len(df)} records from operator_daily_performance in {time.time() - start_time:.2f} seconds")
        if not df.empty:
            logger.info(f"Column names in fetched data: {list(df.columns)}")
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
                'odp_date_oc': pd.DataFrame(),
                'odp_date_shift': pd.DataFrame(),
                'odp_date_employee': pd.DataFrame()
            }
        
        # Log available columns for debugging
        logger.info(f"Available columns in source data: {list(df.columns)}")
        
        # Create a mapping of expected column names to actual column names in the DataFrame
        column_name_mapping = {
            # Source columns (from database table)
            'odp_date': ['odp_date', 'odp_date', 'ODP_Date'],
            'shift': ['shift', 'shift', 'Shift'],
            'odp_em_key': ['odp_em_key', 'odp_em_key', 'ODP_EM_Key'],
            'em_rfid': ['em_rfid', 'em_rfid', 'EM_RFID'],
            'em_department': ['em_department', 'em_department', 'EM_Department'],
            'em_first_name': ['em_first_name', 'em_first_name', 'EM_FirstName'],
            'em_last_name': ['em_last_name', 'em_last_name', 'EM_LastName'],
            'odp_actual_clock_in': ['odp_actual_clock_in', 'odp_actual_clock_in', 'ODP_Actual_Clock_In'],
            'odp_actual_clock_out': ['odp_actual_clock_out', 'odp_actual_clock_out', 'ODP_Actual_Clock_Out'],
            'odp_current_station': ['odp_current_station', 'odp_current_station', 'ODP_Current_Station'],
            'odpd_workstation': ['odpd_workstation', 'odpd_workstation', 'ODPD_Workstation'],
            'odpd_wc_key': ['odpd_wc_key', 'odpd_wc_key', 'ODPD_WC_Key'],
            'odpd_quantity': ['odpd_quantity', 'odpd_quantity', 'ODPD_Quantity'],
            'odpd_st_key': ['odpd_st_key', 'odpd_st_key', 'ODPD_ST_Key'],
            'st_id': ['st_id', 'st_id', 'ST_ID'],
            'st_description': ['st_description', 'st_description', 'ST_Description'],
            'odpd_lot_number': ['odpd_lot_number', 'odpd_lot_number', 'ODPD_Lot_Number'],
            'odpd_oc_key': ['odpd_oc_key', 'odpd_oc_key', 'ODPD_OC_Key'],
            'oc_description': ['oc_description', 'oc_description', 'OC_Description'],
            'loading_qty': ['loading_qty', 'loading_qty', 'Loading_Qty'],
            'unloading_qty': ['unloading_qty', 'unloading_qty', 'UnLoading_Qty'],
            'oc_standard_time': ['oc_standard_time', 'oc_standard_time', 'OC_Standard_Time'],
            'odpd_actual_time': ['odpd_actual_time', 'odpd_actual_time', 'ODPD_Actual_Time'],
            'odpd_cm_key': ['odpd_cm_key', 'odpd_cm_key', 'ODPD_CM_Key'],
            'cm_description': ['cm_description', 'cm_description', 'CM_Description'],
            'odpd_sm_key': ['odpd_sm_key', 'odpd_sm_key', 'ODPD_SM_Key'],
            'sm_description': ['sm_description', 'sm_description', 'SM_Description'],
            'odpd_is_overtime': ['odpd_is_overtime', 'odpd_is_overtime', 'ODPD_Is_Overtime'],
            'odpd_overtime_factor': ['odpd_overtime_factor', 'odpd_overtime_factor', 'ODPD_Overtime_Factor'],
            'odpd_stpo_key': ['odpd_stpo_key', 'odpd_stpo_key', 'ODPD_STPO_Key'],
            'source_connection': ['source_connection', 'source_connection', 'source_connection'],
            'record_count': ['record_count', 'record_count', 'record_count']
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
        if 'id' in df.columns:
            agg_operations_1['id'] = 'count'
        elif len(df.columns) > 0:
            # Use the first column for counting if 'id' doesn't exist
            agg_operations_1[df.columns[0]] = 'count'
        
        if actual_agg1_grouping_cols and agg_operations_1:
            aggregated_df1 = df.groupby(actual_agg1_grouping_cols).agg(agg_operations_1).reset_index()
            # Rename count column to record_count
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
        if 'id' in df.columns:
            agg_operations_2['id'] = 'count'
        elif len(df.columns) > 0:
            agg_operations_2[df.columns[0]] = 'count'
        
        if actual_agg2_grouping_cols and agg_operations_2:
            aggregated_df2 = df.groupby(actual_agg2_grouping_cols).agg(agg_operations_2).reset_index()
            # Rename count column to record_count
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
            "odpd_oc_key", "oc_description", "oc_standard_time",
            "odpd_cm_key", "cm_description", "odpd_sm_key", "sm_description",
            "source_connection"
        ]
        
        # Map to actual column names
        actual_agg3_grouping_cols = [actual_column_names[col] for col in agg3_grouping_cols_expected if col in actual_column_names]
        
        # Define aggregation operations for aggregation 3
        agg_operations_3 = {}
        if 'odpd_quantity' in actual_column_names:
            agg_operations_3[actual_column_names['odpd_quantity']] = 'sum'
        if 'loading_qty' in actual_column_names:
            agg_operations_3[actual_column_names['loading_qty']] = 'sum'
        if 'unloading_qty' in actual_column_names:
            agg_operations_3[actual_column_names['unloading_qty']] = 'sum'
        if 'odpd_actual_time' in actual_column_names:
            agg_operations_3[actual_column_names['odpd_actual_time']] = 'sum'
        if 'odpd_is_overtime' in actual_column_names:
            agg_operations_3[actual_column_names['odpd_is_overtime']] = 'max'
        if 'odpd_overtime_factor' in actual_column_names:
            agg_operations_3[actual_column_names['odpd_overtime_factor']] = 'mean'
        # Use a column for counting
        if 'id' in df.columns:
            agg_operations_3['id'] = 'count'
        elif len(df.columns) > 0:
            agg_operations_3[df.columns[0]] = 'count'
        if 'odp_actual_clock_in' in actual_column_names:
            agg_operations_3[actual_column_names['odp_actual_clock_in']] = 'min'
        if 'odp_actual_clock_out' in actual_column_names:
            agg_operations_3[actual_column_names['odp_actual_clock_out']] = 'max'
        
        if actual_agg3_grouping_cols and agg_operations_3:
            aggregated_df3 = df.groupby(actual_agg3_grouping_cols).agg(agg_operations_3).reset_index()
            # Rename count column to record_count
            count_col = list(agg_operations_3.keys())[-1]  # Get the last aggregated column (the count)
            if count_col in aggregated_df3.columns:
                aggregated_df3 = aggregated_df3.rename(columns={count_col: 'record_count'})
        else:
            logger.warning("No valid columns for aggregation 3")
            aggregated_df3 = pd.DataFrame()
        
        # Add created_at timestamp to all DataFrames
        current_time = datetime.now()
        for df_agg in [aggregated_df1, aggregated_df2, aggregated_df3]:
            if not df_agg.empty:
                df_agg['created_at'] = current_time
        
        agg_results = {
            'odp_date_oc': aggregated_df1,
            'odp_date_shift': aggregated_df2,
            'odp_date_employee': aggregated_df3
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
        # Create connection with autocommit enabled for VACUUM
        conn = psycopg2.connect(
            host=connection_params.get("host", "pg-ssg"),
            port=connection_params.get("port", "5432"),
            database=connection_params.get("database", "ssg"),
            user=connection_params.get("user", "postgres"),
            password=connection_params.get("password", "P@akistan12"),
            autocommit=True  # Required for VACUUM
        )
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
            'odp_current_station': 'ODP_Current_Station',
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
            'created_at': 'created_at'
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
            'odp_date_oc': [
                'ODP_Date', 'Shift', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 'ODPD_Lot_Number',
                'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 'CM_Description', 
                'ODPD_SM_Key', 'SM_Description', 'source_connection'
            ],
            'odp_date_shift': [
                'ODP_Date', 'Shift', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 
                'ODPD_Lot_Number', 'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 
                'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime', 
                'ODPD_STPO_Key', 'source_connection'
            ],
            'odp_date_employee': [
                'ODP_Date', 'Shift', 'ODP_EM_Key', 'EM_RFID', 'EM_Department', 'EM_FirstName', 'EM_LastName',
                'ODP_Current_Station', 'ODPD_Workstation', 'ODPD_WC_Key', 'ODPD_ST_Key', 'ST_ID', 'ST_Description', 
                'ODPD_Lot_Number', 'ODPD_OC_Key', 'OC_Description', 'ODPD_CM_Key', 
                'CM_Description', 'ODPD_SM_Key', 'SM_Description', 'ODPD_Is_Overtime', 
                'ODPD_STPO_Key', 'source_connection'
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
    'hourly_hanger_line_production_upsert',
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