"""
ETL Pipeline for Hanger lines Data
This DAG extracts data from MSSQL sources and loads it into a PostgreSQL target.
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import subprocess
import os

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

from dags.create_target_pg_hl_table import (
    HangerLaneData,
    create_etl_log_table_if_not_exists,
    create_table_if_not_exists,
    
)

# Import functions from hanger_line_transform.py
try:
    from sparkFiles.sparkProcess import (
        create_spark_session,
        transform_data
    )
    print("Successfully imported functions from hanger_line_transform.py")
except ImportError as e:
    print(f"Error importing functions from hanger_line_transform.py: {e}")


from scripts.constans.db_sources import SOURCE_HANGER_LANE

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Timezone configuration
PKT = timezone("Asia/Karachi")

# Constants for retry configuration
MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


def get_postgres_engine():
    """
    Create and return a PostgreSQL engine using Airflow connection.
    
    Returns:
        sqlalchemy.engine.Engine: PostgreSQL engine instance
    """
    connection = BaseHook.get_connection("postgres_grafana")
    uri = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
    # Use connection pooling for better performance
    engine = create_engine(
        uri,
        pool_size=10,
        max_overflow=20,
        pool_pre_ping=True,
        pool_recycle=3600,
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
                    "SELECT MAX(lastextractdatetime) FROM etl_extract_log WHERE source_connection = :src"
                ),
                {"src": source_connection},
            ).scalar()
            logger.info(f"Last extract datetime for {source_connection}: {result}")
            return result
    except Exception as e:
        logger.error(f"Error fetching last extract datetime for {source_connection}: {e}")
        raise
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
        starttime (datetime): Process start time
        endtime (datetime): Process end time
        last_extract_dt (Optional[datetime]): Last extract datetime
        success (bool): Whether the process was successful
        status (str): Status message
        errormessage (Optional[str]): Error message if any
    """
    engine = get_postgres_engine()
    try:
        create_etl_log_table_if_not_exists(engine)
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
        raise
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
    return (
        "DRIVER={FreeTDS};"
        f"SERVER={connection.host};"
        "PORT=1433;"
        f"DATABASE={connection.schema};"
        f"UID={connection.login};"
        f"PWD={connection.password};"
        "TDS_Version=7.0;"
    )


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
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            cursor.execute("SELECT MIN(created_at) FROM [IHS].[dbo].[ODP_Detail] ;")
            result = cursor.fetchone()[0]
            logger.info(f"Min CreationDate from source: {result}")
            return result
    except Exception as e:
        logger.error(f"Error fetching min CreationDate from source: {e}")
        raise


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
def fetch_data_from_source(connection_id: str):
    """
    Fetch data from the source database in batches.
    
    Args:
        connection_id (str): Source connection identifier
        
    Yields:
        List[Dict[str, Any]]: Batches of transaction records
    """
    start_time = time.time()
    logger.info(f"[{connection_id}] Starting data extraction")
    
    # Get last extract datetime
    last_extract_dt = get_last_extract_dt_from_log(connection_id)
    
    # Get source connection details
    connection = BaseHook.get_connection(connection_id)
    conn_str = build_mssql_conn_str(connection)
    
    # If no previous extract, get minimum CreationDate from source
    if not last_extract_dt:
        last_extract_dt = get_min_creation_date_from_source(conn_str)
        logger.info(f"[{connection_id}] Using min CreationDate from source: {last_extract_dt}")
    
    # Build query
    query = """
        SELECT
            [ODP_Date]
            ,[ODP_Key]
            ,CASE WHEN [ODP_Shift]=1 THEN 'Day' ELSE 'Night' END AS [Shift]
            ,[ODP_EM_Key]
            ,[EM_RFID]
            ,[EM_Department]
            ,[EM_FirstName]
            ,[EM_LastName]
            ,[ODP_Actual_Clock_In]
            ,[ODP_Actual_Clock_Out]
            ,[ODP_Shift_Clock_In]
            ,[ODP_Shift_Clock_Out]
            ,[ODP_First_Hanger_Time]
            ,[ODP_Last_Hanger_Time]
            ,[ODP_Current_Station]
            ,[ODP_Lump_Sum_Payment]
            ,[ODP_Make_Up_Pay_Rate]
            ,[ODP_Last_Hanger_Start_Time],
            [ODPD_Key],
            [ODPD_Workstation],
            [ODPD_WC_Key],
            [ODPD_Quantity],
            [ODPD_ST_Key],
            [ST_ID],
            [ST_Description],
            [ODPD_Lot_Number],
            [ODPD_OC_Key],
            CASE WHEN [OC_Description]='Loading/Panel Segregation' THEN 'Loading' 
                WHEN [OC_Description]='Pressing' THEN 'Un-Loading'
            else [OC_Description] END AS OC_Description,
            CASE WHEN [OC_Description]='Loading/Panel Segregation' THEN ODPD_Quantity else 0 END AS Loading_Qty,
            CASE WHEN [OC_Description]='Pressing' THEN ODPD_Quantity else 0 END AS UnLoading_Qty,
            [OC_Piece_Rate],
            [OC_Standard_Time],
            [ODPD_Standard],
            ODPD_Actual_Time,
            [ODPD_PA_Key],
            [ODPD_Pay_Rate],
            [ODPD_Piece_Rate],
            [ODPD_Start_Time],
            [ODPD_CM_Key],
            [CM_Description],
            [ODPD_SM_Key],
            [SM_Description],
            [ODPD_Normal_Pay_Factor],
            [ODPD_Is_Overtime],
            [ODPD_Overtime_Factor],
            [ODPD_Edited_By],
            [ODPD_Edited_Date],
            [ODPD_Actual_Time_From_Reader],
            [ODPD_STPO_Key],
            [created_at] as created_at
        FROM [IHS].[dbo].[ODP_Detail] OD
        INNER JOIN [IHS].[dbo].[ODP_Master] OM ON OD.[ODPD_ODP_Key] = OM.[ODP_Key]  
        INNER JOIN [IHS_SHARED].[dbo].[Employee_Master] EM   ON OM.[ODP_EM_Key]=EM.[EM_Key]
        INNER JOIN [IHS_SHARED].[dbo].[Operation_Codes] OC   ON OD.[ODPD_OC_Key]=OC.[OC_Key]
        INNER JOIN [IHS_SHARED].[dbo].[Size_Master] SM ON OD.[ODPD_SM_Key]=SM.[SM_Key]
        INNER JOIN [IHS_SHARED].[dbo].[Colour_Master] CM ON OD.[ODPD_CM_Key]=CM.[CM_Key]
        INNER JOIN [IHS_SHARED].[dbo].[Style_Master] ST ON OD.[ODPD_ST_Key]=ST.[ST_Key]
        INNER JOIN [IHS_SHARED].[dbo].[Style_Planned_Orders] PO ON OD.[ODPD_STPO_Key]=PO.[STPO_Key]
        WHERE 1=1
    """
    
    params = []
    if last_extract_dt:
        query += " AND OD.created_at > ?"
        params = [last_extract_dt]
        
    query += " ORDER BY OD.created_at ASC;"
    
    # Execute query
    try:
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            logger.info(f"[{connection_id}] Executing query with params: {params}")
            cursor.execute(query, params)
            
            # Fetch data in batches to avoid memory issues and provide progress updates
            batch_size = 1000
            rows_fetched = 0
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                    
                rows_fetched += len(rows)
                logger.info(f"[{connection_id}] Fetched {rows_fetched} rows so far...")
                
                # Convert rows to dictionaries
                batch = []
                for row in rows:
                    batch.append({
                    'ODP_Key': str(row.ODP_Key) if row.ODP_Key else None,
                    'ODP_Date': row.ODP_Date,
                    'Shift': row.Shift,
                    'ODP_EM_Key': int(row.ODP_EM_Key) if row.ODP_EM_Key and str(row.ODP_EM_Key).isdigit() else 0,
                    'EM_RFID': str(row.EM_RFID) if row.EM_RFID else None,
                    'EM_Department': str(row.EM_Department) if row.EM_Department else None,
                    'EM_FirstName': str(row.EM_FirstName) if row.EM_FirstName else None,
                    'EM_LastName': str(row.EM_LastName) if row.EM_LastName else None,
                    'ODP_Actual_Clock_In': row.ODP_Actual_Clock_In,
                    'ODP_Actual_Clock_Out': row.ODP_Actual_Clock_Out,
                    'ODP_Shift_Clock_In': row.ODP_Shift_Clock_In,
                    'ODP_Shift_Clock_Out': row.ODP_Shift_Clock_Out,
                    'ODP_First_Hanger_Time': row.ODP_First_Hanger_Time,
                    'ODP_Last_Hanger_Time': row.ODP_Last_Hanger_Time,
                    'ODP_Current_Station': str(row.ODP_Current_Station) if row.ODP_Current_Station else None,
                    'ODP_Lump_Sum_Payment': float(row.ODP_Lump_Sum_Payment) if row.ODP_Lump_Sum_Payment else 0.0,
                    'ODP_Make_Up_Pay_Rate': float(row.ODP_Make_Up_Pay_Rate) if row.ODP_Make_Up_Pay_Rate else 0.0,
                    'ODP_Last_Hanger_Start_Time': row.ODP_Last_Hanger_Start_Time,
                    'ODPD_Key': str(row.ODPD_Key) if row.ODPD_Key else None,
                    'ODPD_Workstation': str(row.ODPD_Workstation) if row.ODPD_Workstation else None,
                    'ODPD_WC_Key': int(row.ODPD_WC_Key) if row.ODPD_WC_Key and str(row.ODPD_WC_Key).isdigit() else 0,
                    'ODPD_Quantity': int(row.ODPD_Quantity) if row.ODPD_Quantity and str(row.ODPD_Quantity).isdigit() else 0,
                    'ODPD_ST_Key': int(row.ODPD_ST_Key) if row.ODPD_ST_Key and str(row.ODPD_ST_Key).isdigit() else 0,
                    'ST_ID': str(row.ST_ID) if row.ST_ID else None,
                    'ST_Description': str(row.ST_Description) if row.ST_Description else None,
                    'ODPD_Lot_Number': str(row.ODPD_Lot_Number) if row.ODPD_Lot_Number else None,
                    'ODPD_OC_Key': int(row.ODPD_OC_Key) if row.ODPD_OC_Key and str(row.ODPD_OC_Key).isdigit() else 0,
                    'OC_Description': str(row.OC_Description) if row.OC_Description else None,
                    'Loading_Qty': int(row.Loading_Qty) if row.Loading_Qty and str(row.Loading_Qty).isdigit() else 0,
                    'UnLoading_Qty': int(row.UnLoading_Qty) if row.UnLoading_Qty and str(row.UnLoading_Qty).isdigit() else 0,
                    'OC_Piece_Rate': float(row.OC_Piece_Rate) if row.OC_Piece_Rate else 0.0,
                    'OC_Standard_Time': float(row.OC_Standard_Time) if row.OC_Standard_Time else 0.0,
                    'ODPD_Standard': float(row.ODPD_Standard) if row.ODPD_Standard else 0.0,
                    'ODPD_Actual_Time': float(row.ODPD_Actual_Time) if row.ODPD_Actual_Time else 0.0,
                    'ODPD_PA_Key': int(row.ODPD_PA_Key) if row.ODPD_PA_Key and str(row.ODPD_PA_Key).isdigit() else 0,
                    'ODPD_Pay_Rate': float(row.ODPD_Pay_Rate) if row.ODPD_Pay_Rate else 0.0,
                    'ODPD_Piece_Rate': float(row.ODPD_Piece_Rate) if row.ODPD_Piece_Rate else 0.0,
                    'ODPD_Start_Time': row.ODPD_Start_Time,
                    'ODPD_CM_Key': int(row.ODPD_CM_Key) if row.ODPD_CM_Key and str(row.ODPD_CM_Key).isdigit() else 0,
                    'CM_Description': str(row.CM_Description) if row.CM_Description else None,
                    'ODPD_SM_Key': int(row.ODPD_SM_Key) if row.ODPD_SM_Key and str(row.ODPD_SM_Key).isdigit() else 0,
                    'SM_Description': str(row.SM_Description) if row.SM_Description else None,
                    'ODPD_Normal_Pay_Factor': float(row.ODPD_Normal_Pay_Factor) if row.ODPD_Normal_Pay_Factor else 0.0,
                    'ODPD_Is_Overtime': bool(row.ODPD_Is_Overtime) if row.ODPD_Is_Overtime is not None else False,
                    'ODPD_Overtime_Factor': float(row.ODPD_Overtime_Factor) if row.ODPD_Overtime_Factor else 0.0,
                    'ODPD_Edited_By': str(row.ODPD_Edited_By) if row.ODPD_Edited_By else None,
                    'ODPD_Edited_Date': row.ODPD_Edited_Date,
                    'ODPD_Actual_Time_From_Reader': float(row.ODPD_Actual_Time_From_Reader) if row.ODPD_Actual_Time_From_Reader else 0.0,
                    'ODPD_STPO_Key': int(row.ODPD_STPO_Key) if row.ODPD_STPO_Key and str(row.ODPD_STPO_Key).isdigit() else 0,
                    'created_at': row.created_at,
                    'source_connection': connection_id
                    })
                
                # Validate batch data
                validated_batch = validate_data(batch)
                logger.info(f"[{connection_id}] Validated {len(validated_batch)} transactions in current batch")
                yield validated_batch
            
        logger.info(f"[{connection_id}] Finished fetching {rows_fetched} rows in {time.time() - start_time:.2f} seconds")
        
    except Exception as e:
        logger.error(f"Error fetching data from {connection_id}: {e}")
        raise


@retry_on_exception()
def save_to_postgres(connection_id: str) -> str:
    """
    Save transactions to PostgreSQL database in a streaming fashion.
    
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
        
        # Process data in streaming fashion
        for batch in fetch_data_from_source(connection_id):
            if batch:  # Only update last_extract_dt if we have data
                # Get last extract datetime from current batch
                batch_last_extract_dt = (
                    max(tx["created_at"] for tx in batch if tx.get("created_at"))
                    if batch else None
                )
                if batch_last_extract_dt and (not last_extract_dt or batch_last_extract_dt > last_extract_dt):
                    last_extract_dt = batch_last_extract_dt
            
            # Batch insert for better performance
            batch_size = 1000
            for i in range(0, len(batch), batch_size):
                sub_batch = batch[i:i + batch_size]
                batch_objects = []
                
                for transaction_data in sub_batch:
                    try:
                        transaction = HangerLaneData(**transaction_data)
                        batch_objects.append(transaction)
                    except Exception as e:
                        logger.error(f"Error creating transaction object: {e}")
                        continue
                        
                try:
                    session.add_all(batch_objects)
                    session.commit()
                    saved_count += len(batch_objects)
                    logger.info(f"[{connection_id}] Saved {len(batch_objects)} records in current sub-batch - {saved_count} total records saved so far")
                except Exception as e:
                    session.rollback()
                    logger.error(f"Error saving sub-batch: {e}")
                    raise
                
        process_end_time = pendulum.now("Asia/Karachi")
        logger.info(f"[{connection_id}] Successfully saved {saved_count} transactions in {time.time() - start_time:.2f} seconds")
        
        # Log successful completion
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
        raise
    finally:
        if 'session' in locals():
            session.close()
        engine.dispose()
        
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
    dag_id="etl_ssg_hanger_lines",
    default_args=default_args,
    schedule="*/30 * * * *",  # Every 30 minutes
    tags=["ssg", "erp", "etl"],
    catchup=False,
    max_active_runs=1,
    description="ETL pipeline for Hanger lines data from MSSQL to PostgreSQL",
)
def dynamic_hanger_db_etl():
    """
    Dynamic ETL DAG for Hanger lines data.
    
    This DAG dynamically creates tasks for each data source defined in DATA_SOURCES_NAMES.
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
            connection = BaseHook.get_connection(connection_id)
            conn_str = build_mssql_conn_str(connection)
            
            if last_extract_dt:
                logger.info(f"[{connection_id}] LAST EXTRACT DATETIME: {last_extract_dt}")
                logger.info(f"[{connection_id}] Found last extract: {last_extract_dt} → Checking for new data")
                # Check if there's new data since last extract
                with pyodbc.connect(conn_str) as connection:
                    cursor = connection.cursor()
                    # Query to check if there are records newer than last_extract_dt
                    cursor.execute("""
                        SELECT COUNT(*) 
                        FROM [IHS].[dbo].[ODP_Detail] 
                        WHERE created_at > ?
                    """, [last_extract_dt])
                    count = cursor.fetchone()[0]
                    has_new_data = count > 0
                    logger.info(f"[{connection_id}] Found {count} new records since last extract → {'Proceeding to extract' if has_new_data else 'Skipping extraction'}")
                    logger.info(f"[{connection_id}] DECISION: {'SAVE PATH' if has_new_data else 'SKIP PATH'} (Last extract: {last_extract_dt})")
                    return has_new_data
            else:
                logger.info(f"[{connection_id}] LAST EXTRACT DATETIME: None (First run)")
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
            return f"extract_{connection_id}"
        else:
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

    def transform(**context):
        """
        Execute the hanger line data transformation using imported functions.
        """
        logger.info("Starting hanger line data transformation")
        
        try:
            # Create Spark session
            logger.info("Creating Spark session...")
            spark = create_spark_session()
            logger.info("Spark session created successfully")
            
            # Fetch data from PostgreSQL that was saved by the ETL process
            logger.info("Fetching data from PostgreSQL...")
            engine = get_postgres_engine()
            try:
                # Query to fetch data from the last day
                query = """
                    SELECT *
                    FROM operator_daily_performance
                    WHERE created_at >= NOW() - INTERVAL '1 day'
                """
                
                with engine.connect() as conn:
                    result = conn.execute(text(query))
                    rows = result.fetchall()
                    
                    # Convert rows to list of dictionaries
                    columns = result.keys()
                    transactions = [dict(zip(columns, row)) for row in rows]
                    
                    logger.info(f"Fetched {len(transactions)} records from PostgreSQL")
                    
                    # If we have data, convert to Spark DataFrame and pass to transform_data
                    if transactions:
                        # Convert to Spark DataFrame
                        df = spark.createDataFrame(transactions)
                        logger.info(f"Created Spark DataFrame with {df.count()} rows")
                        
                        # Execute transformation with the provided DataFrame
                        logger.info("Executing data transformation with provided DataFrame...")
                        success = transform_data(spark, df)
                    else:
                        # No data to process, execute transformation without DataFrame
                        # This will let sparkProcess.py load data as usual
                        logger.info("No recent data found, executing transformation with default data loading...")
                        success = transform_data(spark)
            finally:
                engine.dispose()
            
            if success:
                logger.info("Data transformation completed successfully")
                return "Transformation completed successfully"
            else:
                logger.warning("Data transformation completed with issues")
                return "Transformation completed with issues"
                
        except Exception as e:
            logger.error(f"Error during data transformation: {e}")
            raise



    @task
    def skip_task(connection_id: str) -> None:
        """
        Skip processing for a connection.
        
        Args:
            connection_id (str): Source connection identifier
        """
        logger.info(f"[{connection_id}] Skipping — no new data since last extract.")
    
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
        save >> skip  # Both save and skip go to end
        skip >> end
    
    # Add a single transform task that runs after all saves are complete
    transform_task = transform.override(task_id="transform")()
    
    # Connect transform task to run after all save tasks
    if save_tasks:
        for save_task in save_tasks:
            save_task >> transform_task
        transform_task >> end
    else:
        start >> transform_task >> end
    
    return dynamic_hanger_db_etl


# Create the DAG instance
dag = dynamic_hanger_db_etl()