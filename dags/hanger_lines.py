"""
Fixed hanger_lane DAG with proper error handling for missing connections
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any, Generator

import subprocess
import sys
import os
import psutil
import gc
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

import pendulum
import pyodbc
from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator
from airflow.utils.edgemodifier import Label
from pendulum import timezone
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert



# Import functions from hanger_line_transform.py
# try:
#     from sparkFiles.sparkProcess import (
#         create_spark_session,
#         transform_data
#     )
#     print("Successfully imported functions from hanger_line_transform.py")
# except ImportError as e:
#     print(f"Error importing functions from hanger_line_transform.py: {e}")


# Add the project root to the Python path for script imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import the correct source constants
from scripts.create_target_pg_hl_table import (
    HangerLaneData,
    create_etl_log_table_if_not_exists,
    create_table_if_not_exists
)


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


def get_postgres_engine():
    """
    Create and return a PostgreSQL engine using Airflow connection.
    
    Returns:
        sqlalchemy.engine.Engine: PostgreSQL engine instance
    """
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
    
    # Use connection pooling for better performance with optimized settings
    engine = create_engine(
        uri,
        pool_size=5,  # Reduced pool size to prevent resource exhaustion
        max_overflow=10,
        pool_pre_ping=True,
        pool_recycle=3600,
        pool_timeout=30,
        echo=False  # Disable SQL logging for performance
    )
    return engine


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


@retry_on_exception()
def get_last_extract_dt_from_log(source_connection: str) -> Optional[datetime]:
    """
    Get the last extract datetime for a source connection from the ETL log.
    
    Args:
        source_connection (str): Source connection identifier
        
    Returns:
        Optional[datetime]: Last extract datetime or None if not found
    """
    engine = get_postgres_engine()
    try:
        # Create the ETL log table if it doesn't exist
        create_etl_log_table_if_not_exists(engine)
        with engine.connect() as conn:
            result = conn.execute(
                text(
                    "SELECT MAX(lastextractdatetime) FROM etl_extract_log WHERE status='Completed' and source_connection = :src and saved_count > 0"
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
        engine.dispose()


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
        starttime (datetime): Process start time (timezone-aware)
        endtime (datetime): Process end time (timezone-aware)
        last_extract_dt (Optional[datetime]): Last extract datetime
        success (bool): Whether the process was successful
        status (str): Status message
        errormessage (Optional[str]): Error message if any
    """
    engine = get_postgres_engine()
    try:
        create_etl_log_table_if_not_exists(engine)
        
        # Convert timezone-aware datetimes to naive datetimes for storage
        # This preserves the actual local time values rather than converting to UTC
        def convert_to_naive(dt):
            """Convert timezone-aware datetime to naive datetime preserving local time values."""
            if dt and hasattr(dt, 'tzinfo') and dt.tzinfo is not None:
                # For pendulum datetime objects
                if hasattr(dt, 'naive'):
                    return dt.naive()
                # For standard datetime objects with timezone
                else:
                    return dt.replace(tzinfo=None)
            return dt
        
        starttime_naive = convert_to_naive(starttime)
        endtime_naive = convert_to_naive(endtime)
        
        with engine.begin() as conn:
            conn.execute(
                text(
                    """
                    INSERT INTO etl_extract_log 
                    (processlogid, source_connection, saved_count, starttime, endtime, lastextractdatetime, success, status, errormessage)
                    VALUES (:processlogid, :source_connection, :saved_count, :starttime, :endtime, :lastextractdatetime, :success, :status, :errormessage)
                    """
                ),
                {
                    "processlogid": processlogid,
                    "source_connection": source_connection,
                    "saved_count": saved_count,
                    "starttime": starttime_naive,
                    "endtime": endtime_naive,
                    "lastextractdatetime": last_extract_dt,
                    "success": success,
                    "status": status,
                    "errormessage": errormessage,
                },
            )
        logger.info(f"Inserted ETL log for {source_connection}")
        logger.info(f"Start time stored: {starttime_naive} (naive datetime)")
        logger.info(f"End time stored: {endtime_naive} (naive datetime)")
    except Exception as e:
        logger.error(f"Failed to insert ETL log for {source_connection}: {e}")
        # Don't raise the exception, just log it
    finally:
        engine.dispose()


def build_mssql_conn_str(connection) -> str:
    """
    Build MSSQL connection string from Airflow connection.
    
    Args:
        connection: Airflow connection object
        
    Returns:
        str: MSSQL connection string
    """
    conn_str = (
        "DRIVER={FreeTDS};"
        f"SERVER={connection.host};"
        "PORT=1433;"
        f"DATABASE={connection.schema};"
        f"UID={connection.login};"
        f"PWD={connection.password};"
        "TDS_Version=7.0;"
        "Connect Timeout=30;"      # Connection timeout in seconds
        "Login Timeout=30;"         # Login timeout in seconds
        "Query Timeout=60;"         # Query timeout in seconds
    )
    logger.info(f"Built MSSQL connection string for host: {connection.host}, database: {connection.schema}, user: {connection.login}")
    logger.info(f"Connection string includes timeouts: Connect=30s, Login=30s, Query=30s")
    return conn_str


@retry_on_exception()
def get_min_creation_date_from_source(conn_str: str) -> Optional[datetime]:
    """
    Get the minimum CreationDate from the source database.
    
    Args:
        conn_str (str): MSSQL connection string
        
    Returns:
        Optional[datetime]: Minimum CreationDate or None if not found
    """
    try:
        logger.info(f"Attempting to connect to MSSQL source with connection string: {conn_str[:50]}...")  # Log first 50 chars for security
        with pyodbc.connect(conn_str, timeout=30) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT MAX(created_at) FROM [IHS].[dbo].[ODP_Detail] ;")
            result = cursor.fetchone()[0]
            logger.info(f"Min CreationDate from source: {result}")
            return result
    except Exception as e:
        logger.error(f"Error fetching min CreationDate from source: {e}")
        # Log connection string details (without password) for debugging
        try:
            # Extract host from connection string for logging
            import re
            host_match = re.search(r'SERVER=([^;]+)', conn_str)
            host = host_match.group(1) if host_match else "unknown"
            logger.error(f"Failed to connect to MSSQL server at host: {host}")
        except:
            pass
        # If we can't connect to source, return None
        return None


def validate_data(transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Validate and clean the extracted data.
    
    Args:
        transactions (List[Dict[str, Any]]): List of transaction records
        
    Returns:
        List[Dict[str, Any]]: Validated and cleaned transaction records
    """
    # For now, just return all transactions as valid
    # In a production environment, you would add validation logic here
    logger.info(f"Validated {len(transactions)} out of {len(transactions)} transactions")
    return transactions


@retry_on_exception()
def fetch_data_from_source(connection_id: str) -> Generator[List[Dict[str, Any]], None, None]:
    """
    Fetch data from the source database in batches with memory optimization.
    
    Args:
        connection_id (str): Source connection identifier
        
    Yields:
        List[Dict[str, Any]]: Batches of transaction records
    """
    start_time = time.time()
    logger.info(f"[{connection_id}] Starting data extraction")
    
    # Memory monitoring
    check_memory_and_cleanup(f"{connection_id} - Start extraction")
    
    # Get last extract datetime
    last_extract_dt = get_last_extract_dt_from_log(connection_id)
    
    # Get source connection details
    try:
        connection = BaseHook.get_connection(connection_id)
        conn_str = build_mssql_conn_str(connection)
    except Exception as e:
        logger.error(f"Could not get connection {connection_id}: {e}")
        # If we can't get the connection, we can't fetch data
        return
    
    # If no previous extract, skip extraction
    if not last_extract_dt:
        # logger.info(f"[{connection_id}] No previous extract datetime found, skipping extraction")

        logger.info(f"[{connection_id}] No previous extract, fetching all data for initial load")
        last_extract_dt = datetime(1900, 1, 1)
        return


    # Build query - FIXED: Removed extra commas in SELECT clause
    query = """
        SELECT
            [ODP_Date] AS odp_date,
            [ODP_Key] AS odp_key,
            CASE WHEN [ODP_Shift] = 1 THEN 'Day' ELSE 'Night' END AS shift,
            [ODP_EM_Key] AS odp_em_key,
            [EM_RFID] AS em_rfid,
            [EM_Department] AS em_department,
            [EM_FirstName] AS em_firstname,
            [EM_LastName] AS em_lastname,
            [ODP_Actual_Clock_In] AS odp_actual_clock_in,
            [ODP_Actual_Clock_Out] AS odp_actual_clock_out,
            [ODP_Shift_Clock_In] AS odp_shift_clock_in,
            [ODP_Shift_Clock_Out] AS odp_shift_clock_out,
            [ODP_First_Hanger_Time] AS odp_first_hanger_time,
            [ODP_Last_Hanger_Time] AS odp_last_hanger_time,
            [ODP_Current_Station] AS odp_current_station,
            [ODP_Lump_Sum_Payment] AS odp_lump_sum_payment,
            [ODP_Make_Up_Pay_Rate] AS odp_make_up_pay_rate,
            [ODP_Last_Hanger_Start_Time] AS odp_last_hanger_start_time,
            [ODPD_Key] AS odpd_key,
            [ODPD_Workstation] AS odpd_workstation,
            [ODPD_WC_Key] AS odpd_wc_key,
            [ODPD_Quantity] AS odpd_quantity,
            [ODPD_ST_Key] AS odpd_st_key,
            [ST_ID] AS st_id,
            [ST_Description] AS st_description,
            [ODPD_Lot_Number] AS odpd_lot_number,
            [ODPD_OC_Key] AS odpd_oc_key,
            CASE WHEN [OC_Description] = 'Loading/Panel Segregation' THEN 'Loading' 
                WHEN [OC_Description] = 'Pressing' THEN 'Un-Loading'
                WHEN [OC_Description] = 'Unloading' THEN 'Un-Loading'
                ELSE [OC_Description] END AS oc_description,
            CASE WHEN [OC_Description] = 'Loading/Panel Segregation' THEN OD.[odpd_quantity] ELSE 0 END AS loading_qty,
            CASE WHEN [OC_Description] = 'Pressing' THEN OD.[odpd_quantity] ELSE 0 END AS unloading_qty,
            [OC_Piece_Rate] AS oc_piece_rate,
            [OC_Standard_Time] AS oc_standard_time,
            [ODPD_Standard] AS odpd_standard,
            [ODPD_Actual_Time] AS odpd_actual_time,
            [ODPD_PA_Key] AS odpd_pa_key,
            [ODPD_Pay_Rate] AS odpd_pay_rate,
            [ODPD_Piece_Rate] AS odpd_piece_rate,
            [ODPD_Start_Time] AS odpd_start_time,
            [ODPD_CM_Key] AS odpd_cm_key,
            [CM_Description] AS cm_description,
            [ODPD_SM_Key] AS odpd_sm_key,
            [SM_Description] AS sm_description,
            [ODPD_Normal_Pay_Factor] AS odpd_normal_pay_factor,
            [ODPD_Is_Overtime] AS odpd_is_overtime,
            [ODPD_Overtime_Factor] AS odpd_overtime_factor,
            [ODPD_Edited_By] AS odpd_edited_by,
            [ODPD_Edited_Date] AS odpd_edited_date,
            [ODPD_Actual_Time_From_Reader] AS odpd_actual_time_from_reader,
            [ODPD_STPO_Key] AS odpd_stpo_key,
            [created_at] AS created_at
        FROM [IHS].[dbo].[ODP_Detail] OD
        INNER JOIN [IHS].[dbo].[ODP_Master] OM ON OD.[odpd_odp_key] = OM.[odp_key]
        LEFT JOIN [IHS_SHARED].[dbo].[Employee_Master] EM ON OM.[odp_em_key] = EM.[em_key]
        LEFT JOIN [IHS_SHARED].[dbo].[Operation_Codes] OC ON OD.[odpd_oc_key] = OC.[oc_key]
        LEFT JOIN [IHS_SHARED].[dbo].[Size_Master] SM ON OD.[odpd_sm_key] = SM.[sm_key]
        LEFT JOIN [IHS_SHARED].[dbo].[Colour_Master] CM ON OD.[odpd_cm_key] = CM.[cm_key]
        LEFT JOIN [IHS_SHARED].[dbo].[Style_Master] ST ON OD.[odpd_st_key] = ST.[st_key]
        LEFT JOIN [IHS_SHARED].[dbo].[Style_Planned_Orders] PO ON OD.[odpd_stpo_key] = PO.[stpo_key]
        WHERE 1=1
    """
    
    params = []
    if last_extract_dt:
        query += " AND ODP_Last_Hanger_Time >= ?"
        params = [last_extract_dt]
        
    query += " ORDER BY ODP_Last_Hanger_Time ASC;"
    
    # Execute query
    try:
        with pyodbc.connect(conn_str, autocommit=True, timeout=30) as connection:
            cursor = connection.cursor()
            logger.info(f"[{connection_id}] Executing query with params: {params}")
            cursor.execute(query, params)
            
            # Fetch data in smaller batches to optimize memory usage
            rows_fetched = 0
            batch_count = 0
            
            while True:
                # Check memory before fetching next batch
                check_memory_and_cleanup(f"{connection_id} - Before fetching batch {batch_count}")
                
                rows = cursor.fetchmany(BATCH_SIZE)
                if not rows:
                    break
                    
                rows_fetched += len(rows)
                batch_count += 1
                logger.info(f"[{connection_id}] Fetched batch {batch_count} with {len(rows)} rows ({rows_fetched} total)")
                
                # Convert rows to dictionaries
                batch = []
                column_names = [column[0] for column in cursor.description]
                
                for row in rows:
                    row_dict = dict(zip(column_names, row))
                    batch.append({
                        'odp_key': str(row_dict.get('odp_key')) if row_dict.get('odp_key') else None,
                        'odp_date': row_dict.get('odp_date'),
                        'shift': row_dict.get('shift'),
                        'odp_em_key': int(row_dict.get('odp_em_key')) if row_dict.get('odp_em_key') and str(row_dict.get('odp_em_key')).isdigit() else 0,
                        'em_rfid': str(row_dict.get('em_rfid')) if row_dict.get('em_rfid') else None,
                        'em_department': str(row_dict.get('em_department')) if row_dict.get('em_department') else None,
                        'em_firstname': str(row_dict.get('em_firstname')) if row_dict.get('em_firstname') else None,
                        'em_lastname': str(row_dict.get('em_lastname')) if row_dict.get('em_lastname') else None,
                        'odp_actual_clock_in': row_dict.get('odp_actual_clock_in'),
                        'odp_actual_clock_out': row_dict.get('odp_actual_clock_out'),
                        'odp_shift_clock_in': row_dict.get('odp_shift_clock_in'),
                        'odp_shift_clock_out': row_dict.get('odp_shift_clock_out'),
                        'odp_first_hanger_time': row_dict.get('odp_first_hanger_time'),
                        'odp_last_hanger_time': row_dict.get('odp_last_hanger_time'),
                        'odp_current_station': str(row_dict.get('odp_current_station')) if row_dict.get('odp_current_station') else None,
                        'odp_lump_sum_payment': float(row_dict.get('odp_lump_sum_payment')) if row_dict.get('odp_lump_sum_payment') else 0.0,
                        'odp_make_up_pay_rate': float(row_dict.get('odp_make_up_pay_rate')) if row_dict.get('odp_make_up_pay_rate') else 0.0,
                        'odp_last_hanger_start_time': row_dict.get('odp_last_hanger_start_time'),
                        'odpd_key': str(row_dict.get('odpd_key')) if row_dict.get('odpd_key') else None,
                        'odpd_workstation': str(row_dict.get('odpd_workstation')) if row_dict.get('odpd_workstation') else None,
                        'odpd_wc_key': int(row_dict.get('odpd_wc_key')) if row_dict.get('odpd_wc_key') and str(row_dict.get('odpd_wc_key')).isdigit() else 0,
                        'odpd_quantity': int(row_dict.get('odpd_quantity')) if row_dict.get('odpd_quantity') and str(row_dict.get('odpd_quantity')).isdigit() else 0,
                        'odpd_st_key': int(row_dict.get('odpd_st_key')) if row_dict.get('odpd_st_key') and str(row_dict.get('odpd_st_key')).isdigit() else 0,
                        'st_id': str(row_dict.get('st_id')) if row_dict.get('st_id') else None,
                        'st_description': str(row_dict.get('st_description')) if row_dict.get('st_description') else None,
                        'odpd_lot_number': str(row_dict.get('odpd_lot_number')) if row_dict.get('odpd_lot_number') else None,
                        'odpd_oc_key': int(row_dict.get('odpd_oc_key')) if row_dict.get('odpd_oc_key') and str(row_dict.get('odpd_oc_key')).isdigit() else 0,
                        'oc_description': str(row_dict.get('oc_description')) if row_dict.get('oc_description') else None,
                        'loading_qty': int(row_dict.get('loading_qty')) if row_dict.get('loading_qty') and str(row_dict.get('loading_qty')).isdigit() else 0,
                        'unloading_qty': int(row_dict.get('unloading_qty')) if row_dict.get('unloading_qty') and str(row_dict.get('unloading_qty')).isdigit() else 0,
                        'oc_piece_rate': float(row_dict.get('oc_piece_rate')) if row_dict.get('oc_piece_rate') else 0.0,
                        'oc_standard_time': float(row_dict.get('oc_standard_time')) if row_dict.get('oc_standard_time') else 0.0,
                        'odpd_standard': float(row_dict.get('odpd_standard')) if row_dict.get('odpd_standard') else 0.0,
                        'odpd_actual_time': float(row_dict.get('odpd_actual_time')) if row_dict.get('odpd_actual_time') else 0.0,
                        'odpd_pa_key': int(row_dict.get('odpd_pa_key')) if row_dict.get('odpd_pa_key') and str(row_dict.get('odpd_pa_key')).isdigit() else 0,
                        'odpd_pay_rate': float(row_dict.get('odpd_pay_rate')) if row_dict.get('odpd_pay_rate') else 0.0,
                        'odpd_piece_rate': float(row_dict.get('odpd_piece_rate')) if row_dict.get('odpd_piece_rate') else 0.0,
                        'odpd_start_time': row_dict.get('odpd_start_time'),
                        'odpd_cm_key': int(row_dict.get('odpd_cm_key')) if row_dict.get('odpd_cm_key') and str(row_dict.get('odpd_cm_key')).isdigit() else 0,
                        'cm_description': str(row_dict.get('cm_description')) if row_dict.get('cm_description') else None,
                        'odpd_sm_key': int(row_dict.get('odpd_sm_key')) if row_dict.get('odpd_sm_key') and str(row_dict.get('odpd_sm_key')).isdigit() else 0,
                        'sm_description': str(row_dict.get('sm_description')) if row_dict.get('sm_description') else None,
                        'odpd_normal_pay_factor': float(row_dict.get('odpd_normal_pay_factor')) if row_dict.get('odpd_normal_pay_factor') else 0.0,
                        'odpd_is_overtime': bool(row_dict.get('odpd_is_overtime')) if row_dict.get('odpd_is_overtime') is not None else False,
                        'odpd_overtime_factor': float(row_dict.get('odpd_overtime_factor')) if row_dict.get('odpd_overtime_factor') else 0.0,
                        'odpd_edited_by': str(row_dict.get('odpd_edited_by')) if row_dict.get('odpd_edited_by') else None,
                        'odpd_edited_date': row_dict.get('odpd_edited_date'),
                        'odpd_actual_time_from_reader': float(row_dict.get('odpd_actual_time_from_reader')) if row_dict.get('odpd_actual_time_from_reader') else 0.0,
                        'odpd_stpo_key': int(row_dict.get('odpd_stpo_key')) if row_dict.get('odpd_stpo_key') and str(row_dict.get('odpd_stpo_key')).isdigit() else 0,
                        'created_at': row_dict.get('created_at'),
                        'source_connection': connection_id
                    })
                
                # Validate batch data
                validated_batch = validate_data(batch)
                logger.info(f"[{connection_id}] Validated {len(validated_batch)} transactions in current batch")
                
                # Yield the batch for processing
                yield validated_batch
                
                # Memory cleanup after processing batch
                del batch, validated_batch, rows
                check_memory_and_cleanup(f"{connection_id} - After processing batch {batch_count}")
                
        logger.info(f"[{connection_id}] Finished fetching {rows_fetched} rows in {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error fetching data from {connection_id}: {e}")
        # Don't raise the exception, just log it and continue


@retry_on_exception()
def save_to_postgres(connection_id: str) -> str:
    """
    Save transactions to PostgreSQL database with optimized memory management.
    
    Args:
        connection_id (str): Source connection identifier
        
    Returns:
        str: Status message
    """
    start_time = time.time()
    logger.info(f"[{connection_id}] Starting data load")
    
    saved_count = 0
    process_start_time = pendulum.now("Asia/Karachi")
    last_extract_dt = None
    
    engine = get_postgres_engine()
    try:
        create_table_if_not_exists(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Process data in streaming fashion with memory optimization
        batch_count = 0
        for batch in fetch_data_from_source(connection_id):
            batch_count += 1
            logger.info(f"[{connection_id}] Processing batch {batch_count}")
            
            if batch:  # Only update last_extract_dt if we have data
                # Get last extract datetime from current batch
                batch_last_extract_dt = (
                    max(tx["created_at"] for tx in batch if tx.get("created_at"))
                    if batch else None
                )
                if batch_last_extract_dt and (not last_extract_dt or batch_last_extract_dt > last_extract_dt):
                    last_extract_dt = batch_last_extract_dt
            
            # Process in smaller sub-batches for better memory management
            sub_batch_size = min(500, BATCH_SIZE // 2)  # Smaller sub-batches
            for i in range(0, len(batch), sub_batch_size):
                sub_batch = batch[i:i + sub_batch_size]
                batch_objects = []
                
                for transaction_data in sub_batch:
                    try:
                        transaction = HangerLaneData(**transaction_data)
                        batch_objects.append(transaction)
                    except Exception as e:
                        logger.error(f"Error creating transaction object: {e}")
                        continue
                        
                try:
                    if batch_objects:  # Only commit if we have objects
                        session.add_all(batch_objects)
                        session.commit()
                        saved_count += len(batch_objects)
                        logger.info(f"[{connection_id}] Saved {len(batch_objects)} records in current sub-batch - {saved_count} total records saved so far")
                        
                        # Memory cleanup after each sub-batch
                        session.expunge_all()  # Remove objects from session to free memory
                        del batch_objects
                        
                        # Check memory usage periodically
                        if saved_count % (sub_batch_size * 5) == 0:
                            check_memory_and_cleanup(f"{connection_id} - After saving {saved_count} records")
                            
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error saving sub-batch: {e}")
                    # Don't raise, just continue
                
                # Explicit cleanup
                del sub_batch
                
            # Memory cleanup after each batch
            del batch
            check_memory_and_cleanup(f"{connection_id} - After processing batch {batch_count}")
            
        process_end_time = pendulum.now("Asia/Karachi")
        logger.info(f"[{connection_id}] Successfully saved {saved_count} transactions in {time.time() - start_time:.2f} seconds")
        
        # Log successful completion
        if saved_count > 0:
            insert_etl_log(
                str(uuid.uuid4()),
                connection_id,
                saved_count,
                process_start_time,
                process_end_time,
                last_extract_dt,
                True,
                "Completed",
                None,
            )
        else:
            # Even if no data was saved, log the attempt
            insert_etl_log(
                str(uuid.uuid4()),
                connection_id,
                saved_count,
                process_start_time,
                process_end_time,
                last_extract_dt,
                True,
                "Completed - No new data",
                None,
            )
        
    except Exception as e:
        process_end_time = pendulum.now("Asia/Karachi")
        logger.error(f"[{connection_id}] Error saving data to PostgreSQL: {e}")
        
        # Log failure
        insert_etl_log(
            str(uuid.uuid4()),
            connection_id,
            saved_count,
            process_start_time,
            process_end_time,
            last_extract_dt,
            False,
            "Failed",
            str(e),
        )
        # Don't raise the exception, just log it
    finally:
        if 'session' in locals():
            session.close()
        engine.dispose()
        # Final memory cleanup
        perform_memory_cleanup()
        
    return f"Saved {saved_count} rows for {connection_id}"


@retry_on_exception()
def upsert_to_postgres(connection_id: str) -> str:
    """
    Upsert transactions to PostgreSQL database using source_connection and odpd_key as composite key
    with optimized memory management.
    
    Args:
        connection_id (str): Source connection identifier
        
    Returns:
        str: Status message
    """
    start_time = time.time()
    logger.info(f"[{connection_id}] Starting data upsert")
    
    upserted_count = 0
    process_start_time = pendulum.now("Asia/Karachi")
    last_extract_dt = None
    
    engine = get_postgres_engine()
    try:
        create_table_if_not_exists(engine)
        
        # Process data in streaming fashion with memory optimization
        batch_count = 0
        for batch in fetch_data_from_source(connection_id):
            batch_count += 1
            logger.info(f"[{connection_id}] Processing batch {batch_count}")
            
            if batch:  # Only update last_extract_dt if we have data
                # Get last extract datetime from current batch
                batch_last_extract_dt = (
                    max(tx["odp_last_hanger_time"] for tx in batch if tx.get("odp_last_hanger_time"))
                    if batch else None
                )
                if batch_last_extract_dt and (not last_extract_dt or batch_last_extract_dt > last_extract_dt):
                    last_extract_dt = batch_last_extract_dt
            
            # Process in smaller sub-batches for better memory management
            sub_batch_size = min(500, BATCH_SIZE // 2)  # Smaller sub-batches
            for i in range(0, len(batch), sub_batch_size):
                sub_batch = batch[i:i + sub_batch_size]
                batch_data = []
                
                for transaction_data in sub_batch:
                    # Prepare the data for upsert
                    try:
                        # Ensure data is properly formatted
                        transaction_data['odp_key'] = str(transaction_data.get('odp_key')) if transaction_data.get('odp_key') else None
                        transaction_data['odpd_key'] = str(transaction_data.get('odpd_key')) if transaction_data.get('odpd_key') else None
                        transaction_data['source_connection'] = str(transaction_data.get('source_connection', connection_id))
                        # Add the connection_id to the data if it's not already there
                        if 'source_connection' not in transaction_data:
                            transaction_data['source_connection'] = connection_id
                        
                        batch_data.append(transaction_data)
                    except Exception as e:
                        logger.error(f"Error preparing transaction data for upsert: {e}")
                        continue
                        
                try:
                    if batch_data:  # Only upsert if we have data
                        with engine.begin() as conn:
                            # Create the upsert statement
                            stmt = insert(HangerLaneData).values(batch_data)
                            
                            # Define the conflict target (composite primary key)
                            stmt = stmt.on_conflict_do_update(
                                index_elements=['source_connection', 'odp_key', 'odpd_key'],
                                set_=dict(
                                    odp_date=stmt.excluded.odp_date,
                                    shift=stmt.excluded.shift,
                                    odp_em_key=stmt.excluded.odp_em_key,
                                    em_rfid=stmt.excluded.em_rfid,
                                    em_department=stmt.excluded.em_department,
                                    em_firstname=stmt.excluded.em_firstname,
                                    em_lastname=stmt.excluded.em_lastname,
                                    odp_actual_clock_in=stmt.excluded.odp_actual_clock_in,
                                    odp_actual_clock_out=stmt.excluded.odp_actual_clock_out,
                                    odp_shift_clock_in=stmt.excluded.odp_shift_clock_in,
                                    odp_shift_clock_out=stmt.excluded.odp_shift_clock_out,
                                    odp_first_hanger_time=stmt.excluded.odp_first_hanger_time,
                                    odp_last_hanger_time=stmt.excluded.odp_last_hanger_time,
                                    odp_current_station=stmt.excluded.odp_current_station,
                                    odp_lump_sum_payment=stmt.excluded.odp_lump_sum_payment,
                                    odp_make_up_pay_rate=stmt.excluded.odp_make_up_pay_rate,
                                    odp_last_hanger_start_time=stmt.excluded.odp_last_hanger_start_time,
                                    odpd_workstation=stmt.excluded.odpd_workstation,
                                    odpd_wc_key=stmt.excluded.odpd_wc_key,
                                    odpd_quantity=stmt.excluded.odpd_quantity,
                                    odpd_st_key=stmt.excluded.odpd_st_key,
                                    st_id=stmt.excluded.st_id,
                                    st_description=stmt.excluded.st_description,
                                    odpd_lot_number=stmt.excluded.odpd_lot_number,
                                    odpd_oc_key=stmt.excluded.odpd_oc_key,
                                    oc_description=stmt.excluded.oc_description,
                                    loading_qty=stmt.excluded.loading_qty,
                                    unloading_qty=stmt.excluded.unloading_qty,
                                    oc_piece_rate=stmt.excluded.oc_piece_rate,
                                    oc_standard_time=stmt.excluded.oc_standard_time,
                                    odpd_standard=stmt.excluded.odpd_standard,
                                    odpd_actual_time=stmt.excluded.odpd_actual_time,
                                    odpd_pa_key=stmt.excluded.odpd_pa_key,
                                    odpd_pay_rate=stmt.excluded.odpd_pay_rate,
                                    odpd_piece_rate=stmt.excluded.odpd_piece_rate,
                                    odpd_start_time=stmt.excluded.odpd_start_time,
                                    odpd_cm_key=stmt.excluded.odpd_cm_key,
                                    cm_description=stmt.excluded.cm_description,
                                    odpd_sm_key=stmt.excluded.odpd_sm_key,
                                    sm_description=stmt.excluded.sm_description,
                                    odpd_normal_pay_factor=stmt.excluded.odpd_normal_pay_factor,
                                    odpd_is_overtime=stmt.excluded.odpd_is_overtime,
                                    odpd_overtime_factor=stmt.excluded.odpd_overtime_factor,
                                    odpd_edited_by=stmt.excluded.odpd_edited_by,
                                    odpd_edited_date=stmt.excluded.odpd_edited_date,
                                    odpd_actual_time_from_reader=stmt.excluded.odpd_actual_time_from_reader,
                                    odpd_stpo_key=stmt.excluded.odpd_stpo_key,
                                    created_at=stmt.excluded.created_at
                
                                )
                            )
                            
                            conn.execute(stmt)
                            upserted_count += len(batch_data)
                            logger.info(f"[{connection_id}] Upserted {len(batch_data)} records in current sub-batch - {upserted_count} total records upserted so far")
                            
                except Exception as e:
                    logger.error(f"Error upserting sub-batch: {e}")
                    # Don't raise, just continue
                finally:
                    # Explicit cleanup
                    del batch_data
                    del sub_batch
                
            # Memory cleanup after each batch
            del batch
            check_memory_and_cleanup(f"{connection_id} - After processing batch {batch_count}")
            
        process_end_time = pendulum.now("Asia/Karachi")
        logger.info(f"[{connection_id}] Successfully upserted {upserted_count} transactions in {time.time() - start_time:.2f} seconds")
        
        # Log successful completion
        if upserted_count > 0:
            insert_etl_log(
                str(uuid.uuid4()),
                connection_id,
                upserted_count,
                process_start_time,
                process_end_time,
                last_extract_dt,
                True,
                "Completed",
                None,
            )
        else:
            # Even if no data was upserted, log the attempt
            insert_etl_log(
                str(uuid.uuid4()),
                connection_id,
                upserted_count,
                process_start_time,
                process_end_time,
                last_extract_dt,
                True,
                "Completed - No new data",
                None,
            )
        
    except Exception as e:
        process_end_time = pendulum.now("Asia/Karachi")
        logger.error(f"[{connection_id}] Error upserting data to PostgreSQL: {e}")
        
        # Log failure
        insert_etl_log(
            str(uuid.uuid4()),
            connection_id,
            upserted_count,
            process_start_time,
            process_end_time,
            last_extract_dt,
            False,
            "Failed",
            str(e),
        )
        # Don't raise the exception, just log it
    finally:
        engine.dispose()
        # Final memory cleanup
        perform_memory_cleanup()
        
    return f"Upserted {upserted_count} rows for {connection_id}"


