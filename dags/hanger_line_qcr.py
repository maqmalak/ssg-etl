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



# # Import functions from hanger_line_transform.py
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
from scripts.constans.db_sources import SOURCE_HANGER_LANE 
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
        create_etl_log_qcr_table_if_not_exists(engine)
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
                    "starttime": starttime,
                    "endtime": endtime,
                    "lastextractdatetime": last_extract_dt,
                    "success": success,
                    "status": status,
                    "errormessage": errormessage,
                },
            )
        logger.info(f"Inserted ETL log for {source_connection}")
    except Exception as e:
        logger.error(f"Failed to insert ETL log for {source_connection}: {e}")
        # Don't raise the exception, just log it
    finally:
        dispose_postgres_engine("pg-ssg")


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
    )
    logger.info(f"Built MSSQL connection string for host: {connection.host}, database: {connection.schema}, user: {connection.login}")
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
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT MIN(QCR_Defect_DateTime) FROM [IHS_SHARED].[dbo].[QC_Rework] WITH (NOLOCK);")
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


def validate_data(transactions: List[Dict[str, Any]], connection_id: str = "unknown") -> List[Dict[str, Any]]:
    """
    Validate and clean the extracted data.
    
    Args:
        transactions (List[Dict[str, Any]]): List of transaction records
        connection_id (str): Source connection identifier for logging
        
    Returns:
        List[Dict[str, Any]]: Validated and cleaned transaction records
    """
    validated_transactions = []
    invalid_count = 0
    
    for transaction in transactions:
        try:
            # Basic validation checks
            if not transaction.get('qcr_key'):
                logger.warning(f"[{connection_id}] Skipping transaction with missing qcr_key: {transaction.get('qcr_key', 'unknown')}")
                invalid_count += 1
                continue
                
            # Check datetime fields
            if not transaction.get('qcr_defect_datetime'):
                logger.warning(f"[{connection_id}] Skipping transaction {transaction.get('qcr_key', 'unknown')} with missing qcr_defect_datetime")
                invalid_count += 1
                continue
                
            # Add source connection if missing
            if 'source_connection' not in transaction:
                transaction['source_connection'] = connection_id
                
            # Add any additional validation logic here as needed
            validated_transactions.append(transaction)
        except Exception as e:
            logger.error(f"[{connection_id}] Error validating transaction {transaction.get('qcr_key', 'unknown')}: {e}")
            invalid_count += 1
            continue
    
    logger.info(f"[{connection_id}] Validated {len(validated_transactions)} out of {len(transactions)} transactions ({invalid_count} invalid)")
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
    
    # Get last extract datetime
    last_extract_dt = get_last_extract_dt_from_log(connection_id)
    
    # Get source connection details
    try:
        connection = BaseHook.get_connection(connection_id)
        conn_str = build_mssql_conn_str(connection)
    except Exception as e:
        logger.error(f"[{connection_id}] Could not get connection: {e}")
        # If we can't get the connection, we can't fetch data
        return
    
    # If no previous extract, get minimum CreationDate from source
    if not last_extract_dt:
        last_extract_dt = get_min_creation_date_from_source(conn_str)
        if last_extract_dt:
            logger.info(f"[{connection_id}] Using min CreationDate from source: {last_extract_dt}")
        else:
            logger.info(f"[{connection_id}] Could not get min CreationDate from source, fetching all data")
    
    # Build optimized query - Removed unnecessary columns and improved query performance
    # Using specific column names instead of * for better performance
    # Added query hints for better optimization
    query = """
        SELECT
            QCR.QCR_Key,
            QCR.QCR_STPO_Key,
            QCR.QCR_Defect_DateTime,
            CASE 
            WHEN CAST(QCR.QCR_Defect_DateTime AS TIME) BETWEEN '08:00:00' AND '17:00:00' 
            THEN 'A' 
            ELSE 'B' 
            END AS SHIFT,
            QCR.QCR_Defect_EM_Key,
            QCR.QCR_Defect_ST_Key,
            QCR.QCR_Defect_OC_Key,
            QCR.QCR_Sent_To_Rework_By_EM_Key,
            QCR.QCR_Defect_Quantity,
            QCR.QCR_From_QC_Station,
            QCR.QCR_HM_ID,
            QCR.QCR_QC_DateTime,
            QCR.QCR_Repair_EM_Key,
            QCR.QCR_Repair_DateTime,
            QCR.QCR_Repair_Quantity,
            QCR.QCR_Defect_CM_Key,
            QCR.QCR_Defect_SM_Key,
            QCR.QCR_QCSC_Key,
            QCR.QCR_HM_Key,
            QSC.QCSC_Description,
            EM.EM_FirstName,
            EM.EM_Key,
            EM.EM_RFID,
            ST.ST_ID,
            ST.ST_Description,
            SPO.STPO_ST_Key,
            SPO.STPO_ID,
            SPO.STPO_CI_Name
        FROM
            [IHS_SHARED].[dbo].QC_Rework QCR WITH (NOLOCK)
            INNER JOIN [IHS_SHARED].[dbo].QC_Sub_Codes QSC WITH (NOLOCK) ON QCR.QCR_QCSC_Key = QSC.QCSC_Key
            INNER JOIN [IHS_SHARED].[dbo].Employee_Master EM WITH (NOLOCK) ON QCR.QCR_Defect_EM_Key = EM.EM_Key
            INNER JOIN [IHS_SHARED].[dbo].Style_Master ST WITH (NOLOCK) ON QCR.QCR_Defect_ST_Key = ST.ST_Key
            INNER JOIN [IHS_SHARED].[dbo].Style_Planned_Orders SPO WITH (NOLOCK) ON QCR.QCR_STPO_Key = SPO.STPO_Key
        WHERE 1=1
    """
    
    params = []
    if last_extract_dt:
        query += " AND QCR.QCR_Defect_DateTime > ?"
        params = [last_extract_dt]
        
    query += " ORDER BY QCR.QCR_Defect_DateTime ASC"
    
    # Execute query with optimized pagination
    try:
        # Reuse connection for better performance
        connection = pyodbc.connect(conn_str, autocommit=True)
        cursor = connection.cursor()
        logger.info(f"[{connection_id}] Executing query with params: {params}")
        
        # Use server-side cursor with optimized fetch size
        cursor.execute(query, params)
        
        # Fetch data in smaller batches to optimize memory usage
        rows_fetched = 0
        batch_count = 0
        
        while True:
            # Check memory before fetching next batch
            check_memory_and_cleanup(f"{connection_id} - Before fetching batch {batch_count}")
            
            # Fetch with optimized batch size
            rows = cursor.fetchmany(BATCH_SIZE)
            if not rows:
                break
                
            rows_fetched += len(rows)
            batch_count += 1
            logger.info(f"[{connection_id}] Fetched batch {batch_count} with {len(rows)} rows ({rows_fetched} total)")
            
            # Convert rows to dictionaries more efficiently
            batch = []
            column_names = [column[0] for column in cursor.description]
            
            for row in rows:
                row_dict = dict(zip(column_names, row))
                batch.append({
                    'qcr_key': str(row_dict['QCR_Key']) if row_dict.get('QCR_Key') is not None else None,
                    'qcr_stpo_key': int(row_dict['QCR_STPO_Key']) if row_dict.get('QCR_STPO_Key') is not None and str(row_dict['QCR_STPO_Key']).isdigit() else None,
                    'qcr_defect_datetime': row_dict.get('QCR_Defect_DateTime'),
                    'shift': str(row_dict.get('SHIFT')) if row_dict.get('SHIFT') is not None else None,
                    'qcr_defect_em_key': int(row_dict['QCR_Defect_EM_Key']) if row_dict.get('QCR_Defect_EM_Key') is not None and str(row_dict['QCR_Defect_EM_Key']).isdigit() else None,
                    'qcr_defect_st_key': int(row_dict['QCR_Defect_ST_Key']) if row_dict.get('QCR_Defect_ST_Key') is not None and str(row_dict['QCR_Defect_ST_Key']).isdigit() else None,
                    'qcr_defect_oc_key': int(row_dict['QCR_Defect_OC_Key']) if row_dict.get('QCR_Defect_OC_Key') is not None and str(row_dict['QCR_Defect_OC_Key']).isdigit() else None,
                    'qcr_sent_to_rework_by_em_key': int(row_dict['QCR_Sent_To_Rework_By_EM_Key']) if row_dict.get('QCR_Sent_To_Rework_By_EM_Key') is not None and str(row_dict['QCR_Sent_To_Rework_By_EM_Key']).isdigit() else None,
                    'qcr_defect_quantity': int(row_dict['QCR_Defect_Quantity']) if row_dict.get('QCR_Defect_Quantity') is not None and str(row_dict['QCR_Defect_Quantity']).isdigit() else None,
                    'qcr_from_qc_station': str(row_dict.get('QCR_From_QC_Station')) if row_dict.get('QCR_From_QC_Station') is not None else None,
                    'qcr_hm_id': str(row_dict.get('QCR_HM_ID')) if row_dict.get('QCR_HM_ID') is not None else None,
                    'qcr_qc_datetime': row_dict.get('QCR_QC_DateTime'),
                    'qcr_repair_em_key': int(row_dict['QCR_Repair_EM_Key']) if row_dict.get('QCR_Repair_EM_Key') is not None and str(row_dict['QCR_Repair_EM_Key']).isdigit() else None,
                    'qcr_repair_datetime': row_dict.get('QCR_Repair_DateTime'),
                    'qcr_repair_quantity': int(row_dict['QCR_Repair_Quantity']) if row_dict.get('QCR_Repair_Quantity') is not None and str(row_dict['QCR_Repair_Quantity']).isdigit() else None,
                    'qcr_defect_cm_key': int(row_dict['QCR_Defect_CM_Key']) if row_dict.get('QCR_Defect_CM_Key') is not None and str(row_dict['QCR_Defect_CM_Key']).isdigit() else None,
                    'qcr_defect_sm_key': int(row_dict['QCR_Defect_SM_Key']) if row_dict.get('QCR_Defect_SM_Key') is not None and str(row_dict['QCR_Defect_SM_Key']).isdigit() else None,
                    'qcr_qcsc_key': int(row_dict['QCR_QCSC_Key']) if row_dict.get('QCR_QCSC_Key') is not None and str(row_dict['QCR_QCSC_Key']).isdigit() else None,
                    'qcr_hm_key': int(row_dict['QCR_HM_Key']) if row_dict.get('QCR_HM_Key') is not None and str(row_dict['QCR_HM_Key']).isdigit() else None,
                    'qcsc_description': str(row_dict.get('QCSC_Description')) if row_dict.get('QCSC_Description') is not None else None,
                    'em_firstname': str(row_dict.get('EM_FirstName')) if row_dict.get('EM_FirstName') is not None else None,
                    'em_key': int(row_dict['EM_Key']) if row_dict.get('EM_Key') is not None and str(row_dict['EM_Key']).isdigit() else None,
                    'em_rfid': str(row_dict.get('EM_RFID')) if row_dict.get('EM_RFID') is not None else None,
                    'st_id': str(row_dict.get('ST_ID')) if row_dict.get('ST_ID') is not None else None,
                    'st_description': str(row_dict.get('ST_Description')) if row_dict.get('ST_Description') is not None else None,
                    'stpo_st_key': int(row_dict['STPO_ST_Key']) if row_dict.get('STPO_ST_Key') is not None and str(row_dict['STPO_ST_Key']).isdigit() else None,
                    'stpo_id': str(row_dict.get('STPO_ID')) if row_dict.get('STPO_ID') is not None else None,
                    'stpo_ci_name': str(row_dict.get('STPO_CI_Name')) if row_dict.get('STPO_CI_Name') is not None else None,
                    'created_at': row_dict.get('QCR_Defect_DateTime'),  # or datetime.utcnow() if you want ingestion time
                    'source_connection': connection_id
                })                
            # Validate batch data
            validated_batch = validate_data(batch, connection_id)
            logger.info(f"[{connection_id}] Validated {len(validated_batch)} transactions in current batch")
            
            # Yield the batch for processing
            yield validated_batch
            
            # Memory cleanup after processing batch
            del batch, validated_batch, rows
            check_memory_and_cleanup(f"{connection_id} - After processing batch {batch_count}")
            
        # Close connection
        cursor.close()
        connection.close()
        
        logger.info(f"[{connection_id}] Finished fetching {rows_fetched} rows in {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"[{connection_id}] Error fetching data: {e}")
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
    
    engine = get_postgres_engine("pg-ssg")
    try:
        # create_table_if_not_exists(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Create table if not exists
        create_qcr_table_if_not_exists(engine)
        create_etl_log_qcr_table_if_not_exists(engine)
        
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
                
                # Use bulk insert instead of creating objects and adding individually
                try:
                    if sub_batch:  # Only insert if we have data
                        # Prepare data for bulk insert
                        insert_data = []
                        for transaction_data in sub_batch:
                            try:
                                # Clean the data before insertion
                                cleaned_data = {k: v for k, v in transaction_data.items() if v is not None}
                                # Remove 'id' field as it's auto-incrementing
                                if 'id' in cleaned_data:
                                    del cleaned_data['id']
                                # Add missing fields with default values if needed
                                if 'source_connection' not in cleaned_data:
                                    cleaned_data['source_connection'] = connection_id
                                insert_data.append(cleaned_data)
                            except Exception as e:
                                logger.error(f"Error preparing transaction data for bulk insert: {e}")
                                continue
                        
                        if insert_data:
                            # Debug: Log the first record to see what fields are being included
                            if insert_data and len(insert_data) > 0:
                                logger.debug(f"[{connection_id}] First record keys: {list(insert_data[0].keys())}")
                                # Check if 'id' field is present in any record
                                records_with_id = [record for record in insert_data if 'id' in record]
                                if records_with_id:
                                    logger.warning(f"[{connection_id}] Found 'id' field in {len(records_with_id)} records, removing them")
                                    # Remove 'id' field from all records
                                    for record in insert_data:
                                        if 'id' in record:
                                            del record['id']
                            
                            # Use bulk insert for better performance with explicit batch size
                            session.bulk_insert_mappings(QualityControlRepair, insert_data, render_nulls=True)
                            session.commit()
                            saved_count += len(insert_data)
                            logger.info(f"[{connection_id}] Saved {len(insert_data)} records in current sub-batch - {saved_count} total records saved so far")
                            
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
        # Dispose of the engine to free resources
        dispose_postgres_engine("pg-ssg")
        # Final memory cleanup
        perform_memory_cleanup()
        
    return f"Saved {saved_count} rows for {connection_id}"


# DAG Definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 8, 17, 8, 20, tzinfo=PKT),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=24),
    "catchup": False,
}

@dag(
    dag_id="etl_hanger_lines_qcr",
    default_args=default_args,
    schedule=timedelta(minutes=30),
    # schedule='9,19,29,39,49,59 * * * *',
    start_date=datetime(2025, 4, 1, 2, 20),  # First run at 2:20 AM
    tags=["ssg", "line", "qcr"],
    catchup=False,
    max_active_runs=1,
    description="ETL pipeline for Hanger lines QCR data from MSSQL to PostgreSQL (Working)",
)

def dynamic_hanger_db_etl_qcr():
    """
    Dynamic ETL DAG for Hanger lines QCR data with proper error handling.
    
    This DAG dynamically creates tasks for each data source defined in SOURCE_HANGER_LANE.
    For each source, it:
    1. Checks if there's new data to extract
    2. Extracts data from the source if needed
    3. Saves the data to the target PostgreSQL database
    """
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    @task
    def check_for_new_data(connection_id: str) -> bool:
        """
        Check if there's new data to extract for a connection.
        
        Args:
            connection_id (str): Source connection identifier
            
        Returns:
            bool: True if there's new data to extract, False otherwise
        """
        try:
            # Get last extract datetime
            last_extract_dt = get_last_extract_dt_from_log(connection_id)
            
            # Get source connection details
            try:
                connection = BaseHook.get_connection(connection_id)
                conn_str = build_mssql_conn_str(connection)
            except Exception as e:
                logger.error(f"[{connection_id}] Could not get connection: {e}")
                # If we can't get the connection, we can't check for new data
                # Let's be conservative and say there's no new data to avoid errors
                return False
            
            if last_extract_dt:
                logger.info(f"[{connection_id}] LAST EXTRACT DATETIME: {last_extract_dt}")
                logger.info(f"[{connection_id}] Found last extract: {last_extract_dt} → Checking for new data")
                # Check if there's new data since last extract
                try:
                    with pyodbc.connect(conn_str) as connection:
                        cursor = connection.cursor()
                        # Query to check if there are records newer than last_extract_dt
                        # Added WITH (NOLOCK) hint for better performance
                        cursor.execute("""
                            SELECT COUNT(*) 
                            FROM [IHS_SHARED].[dbo].[QC_Rework] WITH (NOLOCK)
                            WHERE QCR_Defect_DateTime > ?
                        """, [last_extract_dt])
                        count = cursor.fetchone()[0]
                        has_new_data = count > 0
                        logger.info(f"[{connection_id}] Found {count} new records since last extract → {'Proceeding to extract' if has_new_data else 'Skipping extraction'}")
                        logger.info(f"[{connection_id}] DECISION: {'SAVE PATH' if has_new_data else 'SKIP PATH'} (Last extract: {last_extract_dt})")
                        return has_new_data
                except Exception as e:
                    logger.error(f"[{connection_id}] Error checking for new data in database: {e}")
                    # If we can't connect to the source database, be conservative and skip
                    return False
            else:
                logger.info(f"[{connection_id}] LAST EXTRACT DATETIME: None (First run or no log)")
                logger.info(f"[{connection_id}] No previous extract date found → Proceeding to extract")
                logger.info(f"[{connection_id}] DECISION: SAVE PATH (No previous extract)")
                return True
        except Exception as e:
            logger.error(f"[{connection_id}] Error checking for new data: {e}")
            # Check if it's a connection error
            error_message = str(e).lower()
            connection_error_keywords = [
                "unable to connect", 
                "adaptive server is unavailable", 
                "connection", 
                "timeout", 
                "could not connect",
                "server is unavailable",
                "host not found",
                "name or service not known"
            ]
            
            is_connection_error = any(keyword in error_message for keyword in connection_error_keywords)
            
            if is_connection_error:
                logger.info(f"[{connection_id}] DECISION: SKIP PATH (Server unavailable, skipping extraction)")
                return False
            else:
                # For other errors, it's safer to proceed with extraction
                logger.info(f"[{connection_id}] DECISION: SAVE PATH (Non-connection error occurred, proceeding for safety)")
                return True
    
    @task.branch
    def decide_next_task(connection_id: str, has_new_data: bool) -> str:
        """
        Decide which task to execute next based on whether there's new data.
        
        Args:
            connection_id (str): Source connection identifier
            has_new_data (bool): Whether there's new data to extract
            
        Returns:
            str: Next task to execute
        """
        if has_new_data:
            logger.info(f"[{connection_id}] DECISION: Proceeding to extract data")
            return f"extract_{connection_id}"
        else:
            logger.info(f"[{connection_id}] DECISION: Skipping data extraction")
            return f"skip_{connection_id}"
    
    
    @task
    def extract_from_source(connection_id: str) -> str:
        """
        Extract data from a source.
        
        Args:
            connection_id (str): Source connection identifier
            
        Returns:
            str: Status message
        """
        # This task just triggers the extraction process
        # The actual data fetching happens in save_data_to_postgres
        return f"Started extraction for {connection_id}"
    

    @task
    def save_data_to_postgres(connection_id: str) -> str:
        """
        Save extracted data to PostgreSQL.
        
        Args:
            connection_id (str): Source connection identifier
            
        Returns:
            str: Status message
        """
        return save_to_postgres(connection_id)
    

    @task
    def skip_task(connection_id: str) -> None:
        """
        Skip processing for a connection.
        
        Args:
            connection_id (str): Source connection identifier
        """
        logger.info(f"[{connection_id}] Skipping — no new data since last extract or connection issue.")


    # Create tasks for each data source
    save_tasks = []
    for conn_id in SOURCE_HANGER_LANE:
        # Create task instances with a generic suffix
        # We can't determine the last extract datetime during DAG parsing
        # task_id_suffix = 'dynamic'
            
        # connection_start = EmptyOperator(task_id=f"start_{conn_id}")
        check = check_for_new_data.override(task_id=f"check_{conn_id}")(conn_id)
        decide = decide_next_task.override(task_id=f"decide_{conn_id}")(conn_id, check)
        extract = extract_from_source.override(task_id=f"extract_{conn_id}")(conn_id)
        save = save_data_to_postgres.override(task_id=f"save_{conn_id}")(conn_id)
        skip = skip_task.override(task_id=f"skip_{conn_id}")(conn_id)
        
        # Add save task to list for later use
        save_tasks.append(save)
        
        # Define task dependencies
        start >> check >> decide
        decide >> Label("Has new data") >> extract
        decide >> Label("No new data") >> skip
        extract >> Label("Save results") >> save
        save >> end
        skip >> end
    
    # # Add a single transform task that runs after all saves are complete
    # transform_task = transform.override(task_id="transform")()
    
    # # Connect transform task to run after all save tasks
    # if save_tasks:
    #     for save_task in save_tasks:
    #         save_task >> transform_task
    #     transform_task >> end
    # else:
    #     start >> transform_task >> end
    
    return dynamic_hanger_db_etl_qcr


# Create the DAG instance
dag = dynamic_hanger_db_etl_qcr()

# If running as main module, test the functions
if __name__ == "__main__":
    # This section is for testing purposes only
    print("hanger_line_qcr.py loaded successfully")