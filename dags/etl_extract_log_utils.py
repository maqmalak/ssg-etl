#!/usr/bin/env python3
"""
Utility functions for handling ETL extract logs and historical data processing.
"""

import logging
import psycopg2
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import Optional, Dict, Any

# For Airflow connections
try:
    from airflow.hooks.base_hook import BaseHook
except ImportError:
    BaseHook = None

import os

logger = logging.getLogger(__name__)

def get_last_extract_datetime(connection_params: Dict[str, str]) -> Optional[datetime]:
    """
    Get the last extract datetime from etl_extract_hourly_log table
    
    Args:
        connection_params: Database connection parameters
        
    Returns:
        datetime: Last extract datetime or None if not found
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

def update_etl_extract_log(
    connection_params: Dict[str, str],
    extract_datetime: datetime,
    records_processed: int,
    tables_updated: list,
    status: str = "success",
    error_message: str = None
) -> bool:
    """
    Update the ETL extract log with historical processing information
    
    Args:
        connection_params: Database connection parameters
        extract_datetime: Datetime of the extraction
        records_processed: Number of records processed
        tables_updated: List of tables that were updated
        status: Status of the extraction (success, failed, etc.)
        error_message: Error message if any
        
    Returns:
        bool: True if successful, False otherwise
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
        cursor = conn.cursor()
        
        # Insert log entry
        insert_query = """
            INSERT INTO etl_extract_hourly_log (
                processlogid, source_connection, extracted_count, starttime, endtime,
                lastextractdatetime, success, status, errormessage
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        # Generate a unique process log ID
        process_log_id = f"daily_historical_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        
        # Current timestamps
        current_time = datetime.now()
        
        cursor.execute(insert_query, (
            process_log_id,
            "historical_processing",  # source_connection
            records_processed,
            current_time,  # starttime
            current_time,  # endtime
            extract_datetime,  # lastextractdatetime
            status == "success",  # success
            status,  # status
            error_message  # errormessage
        ))
        
        conn.commit()
        cursor.close()
        conn.close()
        
        logger.info(f"Successfully updated ETL extract log for historical processing: {process_log_id}")
        return True
        
    except Exception as e:
        logger.error(f"Error updating ETL extract log: {e}")
        return False

def get_historical_data_range(
    connection_params: Dict[str, str],
    last_extract_dt: Optional[datetime] = None
) -> tuple:
    """
    Determine the historical data range to process
    
    Args:
        connection_params: Database connection parameters
        last_extract_dt: Last extract datetime (optional)
        
    Returns:
        tuple: (from_datetime, to_datetime) for historical processing
    """
    try:
        if last_extract_dt is None:
            # If no last extract datetime, get it from the log table
            last_extract_dt = get_last_extract_datetime(connection_params)
        
        if last_extract_dt is None:
            # If still no last extract datetime, use a default (e.g., 7 days ago)
            from_datetime = datetime.now() - timedelta(days=7)
            logger.info("No last extract datetime found, using 7 days ago as default")
        else:
            # Use last extract datetime as start point
            from_datetime = last_extract_dt
            logger.info(f"Using last extract datetime as start point: {from_datetime}")
        
        # End point is current time
        to_datetime = datetime.now()
        
        logger.info(f"Historical data range: {from_datetime} to {to_datetime}")
        return (from_datetime, to_datetime)
        
    except Exception as e:
        logger.error(f"Error determining historical data range: {e}")
        # Fallback to default range
        from_datetime = datetime.now() - timedelta(days=1)
        to_datetime = datetime.now()
        return (from_datetime, to_datetime)

def fetch_historical_source_data(from_datetime: datetime, to_datetime: datetime) -> pd.DataFrame:
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
            "ODPD_Actual_Time", "ODPD_PA_Key", "ODPD_Pay_Rate", "ODPD_Piece_Rate", "ODPD_Start_Time",
            "ODPD_CM_Key", "CM_Description", "ODPD_SM_Key", "SM_Description", "ODPD_Normal_Pay_Factor",
            "ODPD_Is_Overtime", "ODPD_Overtime_Factor", "ODPD_Actual_Time_From_Reader", "ODPD_STPO_Key", 
            "source_connection", "created_at"
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

def get_database_connection():
    """
    Get database connection with proper error handling
    """
    try:
        # Get connection parameters from Airflow connection
        try:
            if BaseHook is not None:
                connection = BaseHook.get_connection("pg-ssg")
                host = connection.host
                port = connection.port if connection.port else 5432
                database = connection.schema
                user = connection.login
                password = connection.password
                
                logger.info(f"Using Airflow connection 'pg-ssg'")
            else:
                raise Exception("BaseHook not available")
        except Exception as e:
            logger.warning(f"Could not get Airflow connection 'pg-ssg', using environment variables: {e}")
            # Fallback to environment variables
            host = os.getenv("POSTGRES_HOST", "172.16.7.6")
            port = os.getenv("POSTGRES_PORT", "5432")
            database = os.getenv("POSTGRES_DB", "ssg")
            user = os.getenv("POSTGRES_USER", "postgres")
            password = os.getenv("POSTGRES_PASSWORD", "P@akistan12")
        
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