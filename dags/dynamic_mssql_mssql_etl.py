from datetime import datetime, timedelta
from airflow.decorators import dag, task
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Numeric, Text, Date, Float
from sqlalchemy.ext.declarative import declarative_base
from airflow.hooks.base import BaseHook
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import uuid
# from airflow.operators.python import get_current_context
import logging
import pyodbc

from scripts.constans.db_sources import DATA_SOURCES_NAMES

# Define the target table model
Base = declarative_base()

class GeneralLedgerTransaction(Base):
    __tablename__ = 'general_ledger_transactions'
    # MSSQL-specific configurations
    __table_args__ = {'keep_existing': True}
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    Dated = Column(Date)
    Vtp = Column(String(10))
    ScrVoucher_NO = Column(String(100))
    EntryNo = Column(Numeric)
    debit = Column(Float)
    credit = Column(Float)
    net = Column(Float)
    Narration = Column(Text)
    FinancialYear = Column(String(10))
    CreationDate = Column(DateTime)
    account_id = Column(String(20))
    sub_id = Column(String(20))
    Account_Title = Column(String(255))
    source_connection = Column(String(255))
    root_type = Column(String(50))
    account_type = Column(String(50))
    Level_id1 = Column(String(20))
    Upper_Level_1_Title = Column(String(255))
    Level_id2 = Column(String(20))
    Upper_Level_2_Title = Column(String(255))
    TypeOfID = Column(Numeric)
    LevelNo = Column(Numeric)
    VoucherDate = Column(Date)
    Prp_Date = Column(Date)
    Prp_ID = Column(String(50))
    ChqDate = Column(Date)
    ChqClearanceDate = Column(Date)
    Chk_Date = Column(Date)
    ChqNo = Column(String(50))
    ChallanNo = Column(String(50))
    CancellDate = Column(Date)
    CancelledBy = Column(String(50))
    atp2_ID = Column(String(50))

def get_target_mssql_engine():
    """Create SQLAlchemy engine for destination MSSQL database"""
    connection = BaseHook.get_connection('TargetMSSQL')
    conn = (
        "DRIVER={FreeTDS};"
        f"SERVER={connection.host};"
        "PORT=1433;"
        f"DATABASE={connection.schema};"
        f"UID={connection.login};"
        f"PWD={connection.password};"
        "TDS_Version=7.0;"
    )
    # Create SQLAlchemy engine with pyodbc
    uri = f"mssql+pyodbc:///?odbc_connect={conn}"
    return create_engine(uri)

def create_table_if_not_exists(engine):
    """Create the transactions table if it doesn't exist"""
    # For MSSQL, we need to handle table creation differently
    try:
        # Check if table exists
        from sqlalchemy import inspect
        inspector = inspect(engine)
        tables = inspector.get_table_names()
        
        if 'general_ledger_transactions' not in tables:
            Base.metadata.create_all(engine)
            logging.info("Created general_ledger_transactions table")
        else:
            logging.info("general_ledger_transactions table already exists")
    except Exception as e:
        logging.error(f"Error checking/creating table: {str(e)}")
        # Fallback to standard creation
        Base.metadata.create_all(engine)


def fetch_data_from_source(connection_id):
    """Fetch data from a specific source"""
    # Get the source database connection
    connection = BaseHook.get_connection(connection_id)
    
    # Create FreeTDS connection string
    conn = (
        "DRIVER={FreeTDS};"
        f"SERVER={connection.host};"
        "PORT=1433;"
        f"DATABASE={connection.schema};"
        f"UID={connection.login};"
        f"PWD={connection.password};"
        "TDS_Version=7.0;"
    )
    
    # uri = (
    # f"mssql+pyodbc://{connection.login}:{connection.password}"
    # f"@{connection.host}:{connection.port}/{connection.schema}"
    # "?driver=FreeTDS&TDS_Version=7.0"
    # )
    # engine = conn
    print(f"Connecting to source database: {conn}")

    # uri = "mssql+pyodbc://sa:P@kistan12@172.16.7.4:1433/SilverStr?driver=FreeTDS&TDS_Version=7.0"
    # engine = create_engine(uri)



    # uri = f"mssql+pyodbc://{connection.login}:{connection.password}@{connection.host}/{connection.schema}?driver=ODBC+Driver+17+for+SQL+Server"
    # engine = create_engine(uri)
    
    # Manual join query to fetch data with proper joins
    query = """
        SELECT 
            g.Dated,
            g.Vtp,
            g.ScrVoucher_NO,
            g.EntryNo,
            g.debit,
            g.credit,
            g.Narration,
            g.FinancialYear,
            g.CreationDate,
            g.VoucherDate,
            g.Prp_Date,
            g.Prp_ID,
            g.ChqDate,
            g.ChqClearanceDate,
            g.Chk_Date,
            g.ChqNo,
            g.ChallanNo,
            g.CancellDate,
            g.CancelledBy,
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

        FROM GeneralLedger AS g
        LEFT JOIN Coa3 AS coa3 ON g.id = coa3.id
        LEFT JOIN UL_COA AS c ON coa3.id = c.id
        LEFT JOIN UL_COA AS ulmid ON coa3.ULID1 = ulmid.ID1 AND coa3.ULID2 = ulmid.ID2
        LEFT JOIN UL_COA AS u4 ON ulmid.ID1 = u4.ID
        LEFT JOIN UL_COA AS u3 ON u4.ID1 = u3.ID
        where g.id = '1010001'
        ORDER BY g.Dated, g.ScrVoucher_NO;     
    """

    try:
        with pyodbc.connect(conn) as connection:
            result = connection.execute(query)
            print(f"Fetched {result.rowcount} rows from {conn}")
            logging.info(f"Fetched {result.rowcount} rows from {conn}")


            transactions = []
            # Process the result set
            for row in result:
                debit = float(row.debit) if row.debit else 0.0
                credit = float(row.credit) if row.credit else 0.0
                net = debit - credit

                transactions.append({
                    'Dated': row.Dated,
                    'Vtp': row.Vtp,
                    'ScrVoucher_NO': row.ScrVoucher_NO,
                    'EntryNo': row.EntryNo,
                    'debit': debit,
                    'credit': credit,
                    'net': net,
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

            return transactions

    except Exception as e:
        logging.error(f"Error fetching data from {connection_id}: {str(e)}")
    return []

def save_data_to_target(transactions):
    """Save transactions to MSSQL database"""
    try:
        engine = get_target_mssql_engine()
        create_table_if_not_exists(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        for transaction_data in transactions:
            # Handle any data conversion issues
            try:
                transaction = GeneralLedgerTransaction(**transaction_data)
                session.add(transaction)
            except Exception as e:
                logging.error(f"Error adding transaction: {str(e)}")
                # Continue with other transactions
                continue
        
        session.commit()
        session.close()
        logging.info(f"Successfully saved {len(transactions)} transactions to MSSQL database")
        return len(transactions)
    except Exception as e:
        logging.error(f"Error saving data to MSSQL: {str(e)}")
        raise

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=120)
}

@dag(
    dag_id='dynamic_mssql_to_mssql_etl',
    default_args=default_args,
    schedule="*/5 * * * *",  # Run every 5 minutes
    tags=["ssg", "multi_db_etl"],
    catchup=False,
    max_active_runs=1
)
def dynamic_multi_db_etl():
    
    @task
    def get_active_connections():
        """Get list of active database connections"""
        # In a real scenario, you might want to:
        # 1. Query Airflow connections with a specific tag
        # 2. Read from a config file
        # 3. Use the static list defined above
        return DATA_SOURCES_NAMES
    
    @task
    def extract_from_source(connection_id: str):
        """Extract data from a specific source"""
        logging.info(f"Extracting data from source: {connection_id}")
        transactions = fetch_data_from_source(connection_id)
        print(f"Extracted {len(transactions)} transactions from {connection_id}")
        logging.info(f"Extracted {len(transactions)} transactions from {connection_id}")
        return transactions
    
    @task
    def consolidate_and_load(all_transactions: list):
        """Consolidate data from all sources and load to destination"""
        # Flatten the list of lists
        consolidated_transactions = []
        for transactions_list in all_transactions:
            print(transactions_list)
            consolidated_transactions.extend(transactions_list)
        
        logging.info(f"Consolidating {len(consolidated_transactions)} transactions from all sources")
        saved_count = save_data_to_target(consolidated_transactions)
        return f"Successfully loaded {saved_count} transactions"
    
    # Get active connections
    active_connections = get_active_connections()
    
    # Dynamically create tasks for each connection
    extract_tasks = extract_from_source.expand(connection_id=active_connections)
    
    # Consolidate and load all data
    load_task = consolidate_and_load(extract_tasks)
    
    # Set dependencies
    extract_tasks >> load_task

# Instantiate the DAG
dag = dynamic_multi_db_etl()