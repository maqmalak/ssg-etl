"""
ETL Pipeline for General Ledger Data
This DAG extracts data from MSSQL sources and loads it into a PostgreSQL target.
"""

from __future__ import annotations

import logging
import time
import uuid
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

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

from dags.create_target_pg_gl_table import (
    GeneralLedgerTransaction,
    create_etl_log_table_if_not_exists,
    create_table_if_not_exists,
)
from scripts.constans.db_sources import DATA_SOURCES_NAMES

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
    connection = BaseHook.get_connection("pg_grafana")
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
            cursor.execute("SELECT MIN(CreationDate) FROM GeneralLedger;")
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
    validated_transactions = []
    
    for transaction in transactions:
        # Skip records with missing critical fields
        if not transaction.get("CreationDate"):
            logger.warning("Skipping record with missing CreationDate")
            continue
            
        # Ensure numeric fields are properly formatted
        try:
            transaction["debit"] = float(transaction.get("debit", 0.0) or 0.0)
            transaction["credit"] = float(transaction.get("credit", 0.0) or 0.0)
            transaction["net"] = transaction["debit"] - transaction["credit"]
            
            # Validate that net is reasonable (optional business rule)
            if abs(transaction["net"]) > 1e12:  # Arbitrary large value check
                logger.warning(f"Unusually large net value: {transaction['net']}")
                
            validated_transactions.append(transaction)
        except (ValueError, TypeError) as e:
            logger.error(f"Error validating transaction: {e}")
            continue
            
    logger.info(f"Validated {len(validated_transactions)} out of {len(transactions)} transactions")
    return validated_transactions


@retry_on_exception()
def fetch_data_from_source(connection_id: str) -> List[Dict[str, Any]]:
    """
    Fetch data from the source database.
    
    Args:
        connection_id (str): Source connection identifier
        
    Returns:
        List[Dict[str, Any]]: List of transaction records
    """
    start_time = time.time()
    logger.info(f"[{connection_id}] Starting data extraction")
    
    # Get last extract datetime
    last_extract_dt = get_last_extract_dt_from_log(connection_id)
    
    # Get source connection details
    connection = BaseHook.get_connection(connection_id)
    conn_str = build_mssql_conn_str(connection)
    
    # If no previous extract, get minimum CreationDate from source
    last_extract_dt = get_min_creation_date_from_source(conn_str)
    logger.info(f"[{connection_id}] Using min CreationDate from source: {last_extract_dt}")
    
    # Build query
    query = """
            SELECT
                CAST('2008-01-01' AS DATE) as Dated, 
                'OP' as Vtp,
                'OP/01/08/SKT/G/001' AS ScrVoucher_NO, 
                ROW_NUMBER() OVER (ORDER BY (SELECT NULL)) AS EntryNo, 
                CASE WHEN g.opn_amt > 0 THEN g.opn_amt ELSE 0 END AS debit,
                CASE WHEN g.opn_amt < 0 THEN ABS(g.opn_amt) ELSE 0 END AS credit,
                'Opening Balance' AS Narration, 
                '08-09' AS FinancialYear,
                CAST('2008-01-01' AS DATE) AS CreationDate, 
                CAST('2008-01-01' AS DATE) AS VoucherDate, 
                CAST('2008-01-01' AS DATE) AS Prp_Date,
                'root' AS Prp_ID,
                NULL AS ChqDate, 
                NULL AS ChqClearanceDate, 
                NULL AS Chk_Date, 
                NULL AS ChqNo,
                NULL AS ChallanNo, 
                NULL AS CancellDate, 
                NULL AS CancelledBy,
                c.TypeOfID, 
                c.LevelNo, 
                u3.ID AS Level_id1, 
                u3.Title AS Upper_Level_1_Title,
                u4.ID AS Level_id2, 
                u4.Title AS Upper_Level_2_Title,
                coa3.ULID2 AS Sub_id, 
                c.id AS Account_id, 
                c.Title AS Account_Title,
                CASE 
                    WHEN LEFT(coa3.id, 2) IN ('50', '55') THEN 'Asset' 
                    WHEN LEFT(coa3.id, 2) IN ('30', '35') THEN 'Liability'          
                    WHEN LEFT(coa3.id, 2) IN ('10', '15', '20', '25') THEN 'Equity'
                    WHEN LEFT(coa3.id, 2) = '70' THEN 'Income'
                    WHEN LEFT(coa3.id, 2) IN ('75', '80', '85', '90', '95', '96') THEN 'Expense'
                    ELSE 'N/A'
                END AS root_type,
                coa3.atp2_ID,
                CASE 
                    WHEN C.atp1 = 'OE' THEN 'Owner Equity' 
                    WHEN C.atp1 = 'LB' THEN 'Liability'
                    WHEN C.atp1 = 'CL' THEN 'Current Liability' 
                    WHEN C.atp1 = 'SP' THEN 'Supplier'				
                    WHEN C.atp1 = 'FA' THEN 'Fixed Asset'
                    WHEN C.atp1 = 'OA' THEN 'Other Asset'
                    WHEN C.atp1 = 'CT' THEN 'Client'
                    WHEN C.atp1 = 'BA' THEN 'Bank'
                    WHEN C.atp1 = 'CA' THEN 'Cash' 
                    WHEN C.atp1 = 'EM' THEN 'Employee'
                    WHEN C.atp1 = 'IN' THEN 'Income'
                    WHEN C.atp1 = 'EX' THEN 'Expenses' 
                    ELSE 'N/A'
                END AS account_type
            FROM Coa32 AS g
            LEFT JOIN Coa3 AS coa3 ON g.id = coa3.id
            LEFT JOIN UL_COA AS c ON coa3.id = c.id
            LEFT JOIN UL_COA AS ulmid ON coa3.ULID1 = ulmid.ID1 AND coa3.ULID2 = ulmid.ID2
            LEFT JOIN UL_COA AS u4 ON ulmid.ID1 = u4.ID
            LEFT JOIN UL_COA AS u3 ON u4.ID1 = u3.ID
            Order by g.id ;
    """
    
    # Execute query
    try:
        with pyodbc.connect(conn_str) as connection:
            cursor = connection.cursor()
            logger.info(f"[{connection_id}] Executing query ")
            cursor.execute(query)
            
            # Fetch data in batches to avoid memory issues and provide progress updates
            transactions = []
            batch_size = 1000
            rows_fetched = 0
            
            while True:
                rows = cursor.fetchmany(batch_size)
                if not rows:
                    break
                    
                rows_fetched += len(rows)
                logger.info(f"[{connection_id}] Fetched {rows_fetched} rows so far...")
                
                # Convert rows to dictionaries
                for row in rows:
                    transactions.append({
                        'Dated': row.Dated,
                        'Vtp': row.Vtp,
                        'ScrVoucher_NO': row.ScrVoucher_NO,
                        'EntryNo': row.EntryNo,
                        'debit': row.debit,
                        'credit': row.credit,
                        'Narration': row.Narration,
                        'FinancialYear': row.FinancialYear,
                        'CreationDate': row.CreationDate,
                        'account_id': row.Account_id,
                        'sub_id': row.Sub_id,
                        'Account_Title': row.Account_Title,
                        'source_connection': connection_id,
                        'root_type': row.root_type,
                        'account_type': row.account_type,
                        'Level_id1': row.Level_id1,
                        'Upper_Level_1_Title': row.Upper_Level_1_Title,
                        'Level_id2': row.Level_id2,
                        'Upper_Level_2_Title': row.Upper_Level_2_Title,
                        'TypeOfID': row.TypeOfID,
                        'LevelNo': row.LevelNo,
                        'VoucherDate': row.VoucherDate,
                        'Prp_Date': row.Prp_Date,
                        'Prp_ID': row.Prp_ID,
                        'ChqDate': row.ChqDate,
                        'ChqClearanceDate': row.ChqClearanceDate,
                        'Chk_Date': row.Chk_Date,
                        'ChqNo': row.ChqNo,
                        'ChallanNo': row.ChallanNo,
                        'CancellDate': row.CancellDate,
                        'CancelledBy': row.CancelledBy,
                        'atp2_ID': row.atp2_ID
                    })
            
        logger.info(f"[{connection_id}] Fetched {rows_fetched} rows in {time.time() - start_time:.2f} seconds")
        
        # Validate data
        validated_transactions = validate_data(transactions)
        logger.info(f"[{connection_id}] Returning {len(validated_transactions)} validated transactions")
        return validated_transactions
        
    except Exception as e:
        logger.error(f"Error fetching data from {connection_id}: {e}")
        raise


@retry_on_exception()
def save_to_postgres(transactions: List[Dict[str, Any]], connection_id: str) -> str:
    """
    Save transactions to PostgreSQL database.
    
    Args:
        transactions (List[Dict[str, Any]]): List of transaction records
        connection_id (str): Source connection identifier
        
    Returns:
        str: Status message
    """
    start_time = time.time()
    logger.info(f"[{connection_id}] Starting data load of {len(transactions)} records")
    
    # Get last extract datetime from transactions
    last_extract_dt = (
        max(tx["CreationDate"] for tx in transactions if tx.get("CreationDate"))
        if transactions else None
    )
    
    saved_count = 0
    process_start_time = pendulum.now("Asia/Karachi")
    
    engine = get_postgres_engine()
    try:
        create_table_if_not_exists(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        # Batch insert for better performance
        batch_size = 1000
        for i in range(0, len(transactions), batch_size):
            batch = transactions[i:i + batch_size]
            batch_objects = []
            
            for transaction_data in batch:
                try:
                    transaction = GeneralLedgerTransaction(**transaction_data)
                    batch_objects.append(transaction)
                except Exception as e:
                    logger.error(f"Error creating transaction object: {e}")
                    continue
                    
            try:
                session.add_all(batch_objects)
                session.commit()
                saved_count += len(batch_objects)
                logger.info(f"[{connection_id}] Saved batch {i//batch_size + 1}/{(len(transactions)-1)//batch_size + 1} - {saved_count} records saved so far")
            except Exception as e:
                session.rollback()
                logger.error(f"Error saving batch {i//batch_size + 1}: {e}")
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
    "start_date": datetime(2025, 9, 1, 0, 0, tzinfo=PKT),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=24),
    "catchup": False,
}

@dag(
    dag_id="ssg_gl_op",
    default_args=default_args,
    schedule="@monthly",  # Every 10 minutes
    tags=["ssg", "erp", "op"],
    catchup=False,
    max_active_runs=1,
    description="ETL pipeline for General Ledger data from MSSQL to PostgreSQL",
)
def dynamic_gl_db_etl():
    """
    Dynamic ETL DAG for General Ledger data.
    
    This DAG dynamically creates tasks for each data source defined in DATA_SOURCES_NAMES.
    For each source, it:
    1. Checks if there's new data to extract
    2. Extracts data from the source if needed
    3. Saves the data to the target PostgreSQL database
    """
    # start = EmptyOperator(task_id="start")
    # end = EmptyOperator(task_id="end")
    
    dag_start = EmptyOperator(task_id="dag_start")
    # start = EmptyOperator(task_id="start")
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
        # This function will receive context automatically from Airflow
        # We'll get the dag_run_id in the actual implementation
        last_extract = get_last_extract_dt_from_log(connection_id)
        if last_extract:
            logger.info(f"[{connection_id}] Found last extract: {last_extract} → Proceeding to extract")
            return True
        else:
            logger.info(f"[{connection_id}] No previous extract date found → Proceeding to extract anyway")
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
    def extract_from_source(connection_id: str) -> List[Dict[str, Any]]:
        """
        Extract data from a source.
        
        Args:
            connection_id (str): Source connection identifier
            
        Returns:
            List[Dict[str, Any]]: Extracted data
        """
        return fetch_data_from_source(connection_id)
    
    @task
    def save_data_to_postgres(transactions: List[Dict[str, Any]], connection_id: str) -> str:
        """
        Save extracted data to PostgreSQL.
        
        Args:
            transactions (List[Dict[str, Any]]): Data to save
            connection_id (str): Source connection identifier
            
        Returns:
            str: Status message
        """
        return save_to_postgres(transactions, connection_id)
    
    @task
    def skip_task(connection_id: str) -> None:
        """
        Skip processing for a connection.
        
        Args:
            connection_id (str): Source connection identifier
        """
        logger.info(f"[{connection_id}] Skipping — no new data since last extract.")
    
    # Create tasks for each data source
    for conn_id in DATA_SOURCES_NAMES:
        # Create task instances
        # Create task instances with a generic suffix
        # We can't determine the last extract datetime during DAG parsing
        task_id_suffix = 'dynamic'

        connection_start = EmptyOperator(task_id=f"start_{conn_id}_{task_id_suffix}")
        check = check_for_new_data.override(task_id=f"check_{conn_id}")(conn_id)
        decide = decide_next_task.override(task_id=f"decide_{conn_id}")(conn_id, check)

        # check = check_for_new_data.override(task_id=f"check_{conn_id}")(conn_id)
        extract = extract_from_source.override(task_id=f"extract_{conn_id}")(conn_id)
        save = save_data_to_postgres.override(task_id=f"save_{conn_id}")(extract, conn_id)
        skip = skip_task.override(task_id=f"skip_{conn_id}")(conn_id)
        
        # Define task dependencies
        dag_start >> connection_start >> check >> decide
        decide >> Label("Has new data") >> extract
        decide >> Label("No new data") >> skip
        extract >> Label("Save results") >> save
        [save, skip] >> end
        [save, skip] >> end
    
    return dynamic_gl_db_etl


# Create the DAG instance
dag = dynamic_gl_db_etl()