"""
 hanger_lane QCR DAG with proper error handling for missing connections
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



# Add the project root to the Python path for script imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Import the correct source constants
from scripts.create_target_qcr_table import (
    QualityControlRepair,
    create_etl_log_qcr_table_if_not_exists,
    create_qcr_table_if_not_exists
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
        create_etl_log_qcr_table_if_not_exists(engine)
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
        create_etl_log_qcr_table_if_not_exists(engine)
        
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
                    INSERT INTO etl_qcr_extract_log 
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


def validate_data(transactions: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Validate and clean the extracted data.
    
    Args:
        transactions (List[Dict[str, Any]]): List of transaction records
        
    Returns:
        List[Dict[str, Any]]: Validated and cleaned transaction records
    """
    validated_transactions = []
    
    for transaction in transactions:
        # Clean the data to prevent string length issues
        cleaned_transaction = {}
        
        for key, value in transaction.items():
            if value is None:
                cleaned_transaction[key] = value
            elif isinstance(value, str):
                # Handle empty strings - convert to None for nullable fields
                if value.strip() == '':
                    cleaned_transaction[key] = None
                else:
                    # Truncate strings that might be too long for their respective columns
                    cleaned_transaction[key] = value.strip()
                    
                    # Apply specific length limits based on the QualityControlRepair model
                    if key == 'shift' and len(cleaned_transaction[key]) > 10:
                        cleaned_transaction[key] = cleaned_transaction[key][:10]
                    elif key == 'qcr_key' and len(cleaned_transaction[key]) > 36:
                        cleaned_transaction[key] = cleaned_transaction[key][:36]
                    elif key == 'qcr_hm_id' and len(cleaned_transaction[key]) > 50:
                        cleaned_transaction[key] = cleaned_transaction[key][:50]
                    elif key == 'qcsc_description' and len(cleaned_transaction[key]) > 255:
                        cleaned_transaction[key] = cleaned_transaction[key][:255]
                    elif key == 'em_firstname' and len(cleaned_transaction[key]) > 100:
                        cleaned_transaction[key] = cleaned_transaction[key][:100]
                    elif key == 'em_rfid' and len(cleaned_transaction[key]) > 50:
                        cleaned_transaction[key] = cleaned_transaction[key][:50]
                    elif key == 'st_id' and len(cleaned_transaction[key]) > 50:
                        cleaned_transaction[key] = cleaned_transaction[key][:50]
                    elif key == 'st_description' and len(cleaned_transaction[key]) > 255:
                        cleaned_transaction[key] = cleaned_transaction[key][:255]
                    elif key == 'stpo_id' and len(cleaned_transaction[key]) > 50:
                        cleaned_transaction[key] = cleaned_transaction[key][:50]
                    elif key == 'stpo_ci_name' and len(cleaned_transaction[key]) > 100:
                        cleaned_transaction[key] = cleaned_transaction[key][:100]
                    elif key == 'source_connection' and len(cleaned_transaction[key]) > 50:
                        cleaned_transaction[key] = cleaned_transaction[key][:50]
                    elif key == 'qcr_qcsc_key' and len(cleaned_transaction[key]) > 36:
                        cleaned_transaction[key] = cleaned_transaction[key][:36]
            else:
                cleaned_transaction[key] = value
        
        validated_transactions.append(cleaned_transaction)
    
    logger.info(f"Validated {len(validated_transactions)} out of {len(transactions)} transactions")
    return validated_transactions


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
    

    
    # Get source connection details
    try:
        connection = BaseHook.get_connection(connection_id)
        conn_str = build_mssql_conn_str(connection)
    except Exception as e:
        logger.error(f"Could not get connection {connection_id}: {e}")
        # If we can't get the connection, we can't fetch data
        return

     # Get last extract datetime
    # last_extract_dt = get_last_extract_dt_from_log(connection_id)   
    # If no previous extract, skip extraction
    # if not last_extract_dt:
    #     logger.info(f"[{connection_id}] No previous extract datetime found, skipping extraction")
    #     return

    last_extract_dt = get_last_extract_dt_from_log(connection_id)
    if not last_extract_dt:
        logger.info(f"[{connection_id}] No previous extract, fetching all data for initial load")
        # For initial load, set a very early date to fetch all records
        last_extract_dt = datetime(1900, 1, 1)  # Use a very early date to fetch all data for first run
    else:
        logger.info(f"[{connection_id}] Fetching data after last extract datetime: {last_extract_dt}")


    # Build query - FIXED: Removed extra commas in SELECT clause

    query = """
        SELECT
            QCR_Key as qcr_key,
            QCR.QCR_STPO_Key as qcr_stpo_key,
            QCR.QCR_Defect_DateTime as qcr_defect_datetime,
            CASE 
                WHEN CAST(QCR.QCR_Defect_DateTime AS TIME) BETWEEN '08:00:00' AND '17:00:00' 
                THEN 'Day' 
                ELSE 'Night' 
            END as shift,
            QCR.QCR_Defect_EM_Key as qcr_defect_em_key,
			EM_QCR.EM_FirstName as defect_em_firstname,
			EM_QCR.EM_LastName as defect_em_lastname,
            EM_QCR.EM_RFID as defect_em_rfid,
            QCR.QCR_Defect_ST_Key as qcr_defect_st_key,
            QCR.QCR_Defect_OC_Key as qcr_defect_oc_key,
			[OC_Description] as oc_description,
            QCR.QCR_Sent_To_Rework_By_EM_Key as qcr_sent_to_rework_by_em_key,
            QCR.QCR_Defect_Quantity as qcr_defect_quantity,
            QCR.QCR_From_QC_Station as qcr_from_qc_station,
            QCR.QCR_HM_ID as qcr_hm_id,
            QCR.QCR_QC_DateTime as qcr_qc_datetime,
            QCR.QCR_Repair_EM_Key as qcr_repair_em_key,
			EM_REPAIR.EM_FirstName as em_repair_firstname,
			EM_REPAIR.EM_lastName as em_repair_lastname,
            EM_REPAIR.EM_RFID as em_repair_rfid,
            QCR.QCR_Repair_DateTime as qcr_repair_datetime,
            QCR.QCR_Repair_Quantity as qcr_repair_quantity,
            QCR.QCR_Defect_CM_Key as qcr_defect_cm_key,
    		[CM_Description] as cm_description,
            QCR.QCR_Defect_SM_Key as qcr_defect_sm_key,
			[SM_Description] as sm_description,
            QCR.QCR_QCSC_Key as qcr_qcsc_key,
            QCR.QCR_HM_Key as qcr_hm_key,
            QSC.QCSC_Description as qcsc_description,
            ST.ST_ID as st_id,
            ST.ST_Description as st_description,
            SPO.STPO_ST_Key as stpo_st_key,
            SPO.STPO_ID as stpo_id,
            SPO.STPO_CI_Name as stpo_ci_name
        FROM
            [IHS_SHARED].[dbo].QC_Rework QCR
            INNER JOIN [IHS_SHARED].[dbo].QC_Sub_Codes QSC ON QCR.QCR_QCSC_Key = QSC.QCSC_Key
						INNER JOIN [IHS_SHARED].[dbo].Employee_Master EM_QCR ON QCR.QCR_Defect_EM_Key = EM_QCR.EM_Key
            INNER JOIN [IHS_SHARED].[dbo].Employee_Master EM_REPAIR ON QCR.QCR_Repair_EM_Key = EM_REPAIR.EM_Key
            INNER JOIN [IHS_SHARED].[dbo].Style_Master ST ON QCR.QCR_Defect_ST_Key = ST.ST_Key
            INNER JOIN [IHS_SHARED].[dbo].Style_Planned_Orders SPO ON QCR.QCR_STPO_Key = SPO.STPO_Key
						LEFT JOIN [IHS_SHARED].[dbo].[Operation_Codes] OC ON QCR.[QCR_Defect_OC_Key] = OC.[oc_key]
						LEFT JOIN [IHS_SHARED].[dbo].[Size_Master] SM ON QCR.[QCR_Defect_SM_Key] = SM.[sm_key]
						LEFT JOIN [IHS_SHARED].[dbo].[Colour_Master] CM ON QCR.[QCR_Defect_CM_Key] = CM.[cm_key]
        WHERE 1=1
    """
    
    params = []
    if last_extract_dt:
        query += " AND QCR.QCR_Defect_DateTime > ?"
        params = [last_extract_dt]
        
    query += " ORDER BY QCR.QCR_Defect_DateTime ASC;"
    
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
                        'qcr_key': str(row_dict.get('qcr_key')),
                        'qcr_stpo_key': row_dict.get('qcr_stpo_key'),
                        'qcr_defect_datetime': row_dict.get('qcr_defect_datetime'),
                        'shift': str(row_dict.get('shift')).strip() if row_dict.get('shift') and str(row_dict.get('shift')).strip() else 'Day',
                        'qcr_defect_em_key': row_dict.get('qcr_defect_em_key'),
                        'defect_em_firstname': row_dict.get('defect_em_firstname'),
                        'defect_em_lastname': row_dict.get('defect_em_lastname'),
                        'defect_em_rfid': row_dict.get('defect_em_rfid') if row_dict.get('defect_em_rfid') and str(row_dict.get('defect_em_rfid')).strip() != '' else None,
                        'qcr_defect_st_key': row_dict.get('qcr_defect_st_key'),
                        'qcr_defect_oc_key': row_dict.get('qcr_defect_oc_key'),
                        'oc_description': row_dict.get('oc_description'),
                        'qcr_sent_to_rework_by_em_key': row_dict.get('qcr_sent_to_rework_by_em_key'),
                        'qcr_defect_quantity': int(row_dict.get('qcr_defect_quantity')) if row_dict.get('qcr_defect_quantity') and str(row_dict.get('qcr_defect_quantity')).isdigit() else 0,
                        'qcr_from_qc_station': row_dict.get('qcr_from_qc_station'),
                        'qcr_hm_id': row_dict.get('qcr_hm_id'),
                        'qcr_qc_datetime': row_dict.get('qcr_qc_datetime'),
                        'qcr_repair_em_key': row_dict.get('qcr_repair_em_key'),
                        'em_repair_firstname': row_dict.get('em_repair_firstname'),
                        'em_repair_lastname': row_dict.get('em_repair_lastname'),
                        'em_repair_rfid': row_dict.get('em_repair_rfid') if row_dict.get('em_repair_rfid') and str(row_dict.get('em_repair_rfid')).strip() != '' else None,
                        'qcr_repair_datetime': row_dict.get('qcr_repair_datetime'),
                        'qcr_repair_quantity': int(row_dict.get('qcr_repair_quantity')) if row_dict.get('qcr_repair_quantity') and str(row_dict.get('qcr_repair_quantity')).isdigit() else 0,
                        'qcr_defect_cm_key': row_dict.get('qcr_defect_cm_key'),
                        'cm_description': row_dict.get('cm_description'),
                        'qcr_defect_sm_key': row_dict.get('qcr_defect_sm_key'),
                        'sm_description': row_dict.get('sm_description'),
                        'qcr_qcsc_key': str(row_dict.get('qcr_qcsc_key')) if row_dict.get('qcr_qcsc_key') else None,
                        'qcsc_description': row_dict.get('qcsc_description'),
                        'em_repair_firstname': row_dict.get('em_repair_firstname'),
                        'em_repair_key': row_dict.get('em_repair_key'),
                        'em_repair_rfid': row_dict.get('em_repair_rfid') if row_dict.get('em_repair_rfid') and str(row_dict.get('em_repair_rfid')).strip() != '' else None,
                        'st_id': row_dict.get('st_id'),
                        'st_description': row_dict.get('st_description'),
                        'stpo_st_key': row_dict.get('stpo_st_key'),
                        'stpo_id': row_dict.get('stpo_id'),
                        'stpo_ci_name': row_dict.get('stpo_ci_name'),
                        'created_at': datetime.now(PKT),
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
        create_qcr_table_if_not_exists(engine)
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
                    max(tx["qcr_defect_datetime"] for tx in batch if tx.get("qcr_defect_datetime"))
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
                        # Additional validation for specific fields that might cause issues
                        # Make sure shift is properly handled (should be 'Day' or 'Night')
                        if transaction_data.get('shift') and len(str(transaction_data['shift']).strip()) == 0:
                            transaction_data['shift'] = 'Day'  # Default to 'Day' if empty
                        elif transaction_data.get('shift'):
                            # Ensure shift value is properly formatted
                            shift_val = str(transaction_data['shift']).strip()
                            if shift_val not in ['Day', 'Night']:
                                # Try to normalize common variations
                                shift_val_lower = shift_val.lower()
                                if 'day' in shift_val_lower:
                                    shift_val = 'Day'
                                elif 'night' in shift_val_lower:
                                    shift_val = 'Night'
                                else:
                                    # Default to Day if it's not clearly Day or Night
                                    shift_val = 'Day'
                            transaction_data['shift'] = shift_val
                        
                        transaction = QualityControlRepair(**transaction_data)
                        batch_objects.append(transaction)
                    except Exception as e:
                        logger.error(f"Error creating transaction object: {e}")
                        logger.error(f"Problematic transaction data: {transaction_data}")
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


