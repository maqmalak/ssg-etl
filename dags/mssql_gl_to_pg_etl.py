# from datetime import datetime, timedelta
# from airflow.decorators import dag, task
# from sqlalchemy.orm import sessionmaker
# from sqlalchemy import create_engine, text
# from airflow.hooks.base import BaseHook
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.operators.python_operator import BranchPythonOperator
# from airflow.operators.dummy_operator import DummyOperator
# from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
# import uuid
# from airflow.operators.python import get_current_context
# import logging
# import pyodbc

# from scripts.constans.db_sources import DATA_SOURCES_NAMES
# from dags.create_target_pg_gl_table import GeneralLedgerTransaction, create_etl_log_table_if_not_exists, create_table_if_not_exists

# def get_postgres_engine():
#     """Create SQLAlchemy engine for destination PostgreSQL database"""
#     # Get the connection details from Airflow
#     connection = BaseHook.get_connection('postgres_grafana')
#     uri = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
#     return create_engine(uri)


# def get_min_creation_date_from_source(conn):
#     """Get the earliest CreationDate from the source MSSQL table."""
#     try:
#         with pyodbc.connect(conn) as connection:
#             cursor = connection.cursor()
#             cursor.execute("SELECT MIN(CreationDate) FROM GeneralLedger;")
#             result = cursor.fetchone()[0]
#             if result:
#                 logging.info(f"Min CreationDate from source: {result}")
#             else:
#                 logging.warning("No CreationDate found in source table")
#             return result
#     except Exception as e:
#         logging.error(f"Error fetching min CreationDate from source: {e}")
#         return None
    
# def get_last_extract_dt_from_log(source_connection: str):

#     """Return the most recent lastextractdatetime for a given source_connection from etl_extract_log."""
#     try:
#         engine = get_postgres_engine()
#         with engine.connect() as conn:
#             result = conn.execute(
#                 text("""
#                     SELECT MAX(lastextractdatetime) 
#                     FROM etl_extract_log 
#                     WHERE source_connection = :src
#                 """),
#                 {"src": source_connection}
#             ).scalar()
#             if result:
#                 logging.info(f"Last extract datetime for {source_connection}: {result}")
#             else:
#                 logging.info(f"No previous extract log for {source_connection}")
#             return result
#     except Exception as e:
#         logging.error(f"Error fetching last extract datetime for {source_connection}: {e}")
#         return None

# def fetch_data_from_source(connection_id):
#     """Fetch data from a specific source with incremental filter."""
    
#     # STEP 1: Get last extraction datetime from Postgres log
#     last_extract_dt = get_last_extract_dt_from_log(connection_id)
    
#     # STEP 2: Build MSSQL connection
#     connection = BaseHook.get_connection(connection_id)
    
#     # Create FreeTDS connection string
#     conn = (
#         "DRIVER={FreeTDS};"
#         f"SERVER={connection.host};"
#         "PORT=1433;"
#         f"DATABASE={connection.schema};"
#         f"UID={connection.login};"
#         f"PWD={connection.password};"
#         "TDS_Version=7.0;"
#     )
    
#     # uri = (
#     # f"mssql+pyodbc://{connection.login}:{connection.password}"
#     # f"@{connection.host}:{connection.port}/{connection.schema}"
#     # "?driver=FreeTDS&TDS_Version=7.0"
#     # )
#     # engine = conn
#     print(f"Connecting to source database: {conn}")

#     # uri = "mssql+pyodbc://sa:P@kistan12@172.16.7.4:1433/SilverStr?driver=FreeTDS&TDS_Version=7.0"
#     # engine = create_engine(uri)



#     # uri = f"mssql+pyodbc://{connection.login}:{connection.password}@{connection.host}/{connection.schema}?driver=ODBC+Driver+17+for+SQL+Server"
#     # engine = create_engine(uri)

#     # STEP 3: If first run (no log found), get min CreationDate from source
#     if not last_extract_dt:
#         last_extract_dt = get_min_creation_date_from_source(conn)

#     # Manual join query to fetch data with proper joins
#     query = """
#         SELECT 
#             g.Dated,
#             g.Vtp,
#             g.ScrVoucher_NO,
#             g.EntryNo,
#             g.debit,
#             g.credit,
#             g.Narration,
#             g.FinancialYear,
#             g.CreationDate,
#             g.VoucherDate,
#             g.Prp_Date,
#             g.Prp_ID,
#             g.ChqDate,
#             g.ChqClearanceDate,
#             g.Chk_Date,
#             g.ChqNo,
#             g.ChallanNo,
#             g.CancellDate,
#             g.CancelledBy,
#             c.TypeOfID,
#             c.LevelNo,
#             u3.ID AS Level_id1,
#             u3.Title AS Upper_Level_1_Title,
#             u4.ID AS Level_id2,
#             u4.Title AS Upper_Level_2_Title,
#             coa3.ULID2 AS Sub_id,
#             c.id AS Account_id,
#             c.Title AS Account_Title,

#             CASE 
#                 WHEN LEFT(coa3.id, 2) IN ('50', '55') THEN 'Asset' 
#                 WHEN LEFT(coa3.id, 2) IN ('30', '35') THEN 'Liability'          
#                 WHEN LEFT(coa3.id, 2) IN ('10', '15', '20', '25') THEN 'Equity'
#                 WHEN LEFT(coa3.id, 2) = '70' THEN 'Income'
#                 WHEN LEFT(coa3.id, 2) IN ('75', '80', '85', '90', '95', '96') THEN 'Expense'
#                 ELSE 'N/A'
#             END AS root_type,

#             coa3.atp2_ID,

#             CASE 
#                 WHEN C.atp1 = 'OE' THEN 'Owner Equity' 
#                 WHEN C.atp1 = 'LB' THEN 'Liability'
#                 WHEN C.atp1 = 'CL' THEN 'Current Liability' 
#                 WHEN C.atp1 = 'SP' THEN 'Supplier' 			
#                 WHEN C.atp1 = 'FA' THEN 'Fixed Asset'
#                 WHEN C.atp1 = 'OA' THEN 'Other Asset'
#                 WHEN C.atp1 = 'CT' THEN 'Client'
#                 WHEN C.atp1 = 'BA' THEN 'Bank'
#                 WHEN C.atp1 = 'CA' THEN 'Cash' 
#                 WHEN C.atp1 = 'EM' THEN 'Employee'
#                 WHEN C.atp1 = 'IN' THEN 'Income'
#                 WHEN C.atp1 = 'EX' THEN 'Expenses' 
#                 ELSE 'N/A'
#             END AS account_type

#         FROM GeneralLedger AS g
#         LEFT JOIN Coa3 AS coa3 ON g.id = coa3.id
#         LEFT JOIN UL_COA AS c ON coa3.id = c.id
#         LEFT JOIN UL_COA AS ulmid ON coa3.ULID1 = ulmid.ID1 AND coa3.ULID2 = ulmid.ID2
#         LEFT JOIN UL_COA AS u4 ON ulmid.ID1 = u4.ID
#         LEFT JOIN UL_COA AS u3 ON u4.ID1 = u3.ID
#         where 1=1
#     """
#     # STEP 4: Add incremental filter if last_extract_dt exists
#     if last_extract_dt:
#         # Make sure datetime is properly formatted for SQL Server
#         query += f" AND g.CreationDate > '{last_extract_dt.strftime('%Y-%m-%d %H:%M:%S')}'"

#     query += " ORDER BY g.Dated, g.ScrVoucher_NO;"

#     try:
#         with pyodbc.connect(conn) as connection:
#             result = connection.execute(query)
#             print(f"Fetched {result.rowcount} rows from {conn}")
#             logging.info(f"Fetched {result.rowcount} rows from {conn}")


#             transactions = []
#             # Process the result set
#             for row in result:
#                 debit = float(row.debit) if row.debit else 0.0
#                 credit = float(row.credit) if row.credit else 0.0
#                 net = debit - credit

#                 transactions.append({
#                     'Dated': row.Dated,
#                     'Vtp': row.Vtp,
#                     'ScrVoucher_NO': row.ScrVoucher_NO,
#                     'EntryNo': row.EntryNo,
#                     'debit': debit,
#                     'credit': credit,
#                     'net': net,
#                     'Narration': row.Narration,
#                     'FinancialYear': row.FinancialYear,
#                     'CreationDate': row.CreationDate,
#                     'account_id': row.Account_id,
#                     'sub_id': row.Sub_id,
#                     'Account_Title': row.Account_Title,
#                     'source_connection': connection_id,
#                     'root_type': row.root_type,
#                     'account_type': row.account_type,
#                     'Level_id1': row.Level_id1,
#                     'Upper_Level_1_Title': row.Upper_Level_1_Title,
#                     'Level_id2': row.Level_id2,
#                     'Upper_Level_2_Title': row.Upper_Level_2_Title,
#                     'TypeOfID': row.TypeOfID,
#                     'LevelNo': row.LevelNo,
#                     'VoucherDate': row.VoucherDate,
#                     'Prp_Date': row.Prp_Date,
#                     'Prp_ID': row.Prp_ID,
#                     'ChqDate': row.ChqDate,
#                     'ChqClearanceDate': row.ChqClearanceDate,
#                     'Chk_Date': row.Chk_Date,
#                     'ChqNo': row.ChqNo,
#                     'ChallanNo': row.ChallanNo,
#                     'CancellDate': row.CancellDate,
#                     'CancelledBy': row.CancelledBy,
#                     'atp2_ID': row.atp2_ID
#             })

#             return transactions

#     except Exception as e:
#         logging.error(f"Error fetching data from {connection_id}: {str(e)}")
#     return []



# def insert_etl_log(processlogid, source_connection, saved_count, starttime, endtime, last_extract_dt, success, status, errormessage):
#     """Insert a record into etl_extract_log."""
#     try:
#         engine = get_postgres_engine()
#         create_etl_log_table_if_not_exists(engine)
#         with engine.begin() as conn:
#             conn.execute(
#                 """
#                 INSERT INTO etl_extract_log 
#                 (processlogid, source_connection, saved_count, starttime, endtime, lastextractdatetime, success, status, errormessage)
#                 VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
#                 """,
#                 (processlogid, source_connection, saved_count, starttime, endtime, last_extract_dt, success, status, errormessage)
#             )
#         logging.info(f"Inserted ETL log for {source_connection}")
#     except Exception as e:
#         logging.error(f"Failed to insert ETL log for {source_connection}: {e}")




# def save_data_to_postgres(transactions):
#     """Save transactions to PostgreSQL"""
#     try:
#         engine = get_postgres_engine()
#         create_table_if_not_exists(engine)
#         Session = sessionmaker(bind=engine)
#         session = Session()
        
#         for transaction_data in transactions:
#             # Handle any data conversion issues
#             try:
#                 transaction = GeneralLedgerTransaction(**transaction_data)
#                 session.add(transaction)
#             except Exception as e:
#                 logging.error(f"Error adding transaction: {str(e)}")
#                 # Continue with other transactions
#                 continue
        
#         session.commit()
#         session.close()
#         logging.info(f"Successfully saved {len(transactions)} transactions to PostgreSQL")
#         return len(transactions)
#     except Exception as e:
#         logging.error(f"Error saving data to PostgreSQL: {str(e)}")
#         raise

# # Default DAG arguments
# default_args = {
#     "owner": "airflow",
#     "depends_on_past": False,
#     "start_date": datetime(2025, 1, 1),
#     "retries": 1,
#     "retry_delay": timedelta(minutes=5),
#     "execution_timeout": timedelta(minutes=120)
# }

# @dag(
#     dag_id='general_ledger_mssql_to_pg_etl',
#     default_args=default_args,
#     schedule="*/10 * * * *",  # Run every 10 minutes
#     tags=["ssg", "ina_db_to_pg"],
#     catchup=False,
#     max_active_runs=1
# )
# def dynamic_gl_db_etl():
    
#     @task
#     def get_active_connections():
#         """Get list of active database connections"""
#         # In a real scenario, you might want to:
#         # 1. Query Airflow connections with a specific tag
#         # 2. Read from a config file
#         # 3. Use the static list defined above
#         return DATA_SOURCES_NAMES
    
#     @task
#     def extract_from_source(connection_id: str):
#         """Extract data from a specific source"""
#         logging.info(f"Extracting data from source: {connection_id}")
#         transactions = fetch_data_from_source(connection_id)
#         print(f"Extracted {len(transactions)} transactions from {connection_id}")
#         logging.info(f"Extracted {len(transactions)} transactions from {connection_id}")
#         return transactions
    

#     @task
#     def consolidate_and_load(all_transactions: list, source_connections: list):
#         start_time = datetime.now()
#         total_saved = 0

#         for source_conn, transactions_list in zip(source_connections, all_transactions):
#             # Get latest CreationDate for this source
#             last_extract_dt = None
#             if transactions_list and "CreationDate" in transactions_list[0]:
#                 try:
#                     last_extract_dt = max(
#                         tx["CreationDate"] for tx in transactions_list if tx.get("CreationDate") is not None
#                     )
#                 except Exception as e:
#                     logging.warning(f"Unable to get last_extract_dt for {source_conn}: {e}")

#             try:
#                 saved_count = save_data_to_postgres(transactions_list)
#                 print(f"Saved {len(transactions_list)} transactions from {source_conn}")
#                 logging.info(f"Extracted {len(transactions_list)} transactions from {source_conn}")
                
#                 total_saved += saved_count
#                 insert_etl_log(
#                     processlogid=str(uuid.uuid4()),
#                     source_connection=source_conn,
#                     saved_count=saved_count,
#                     starttime=start_time,
#                     endtime=datetime.now(),
#                     last_extract_dt=last_extract_dt,
#                     success=True,
#                     status="Completed",
#                     errormessage=None
#                 )
#             except Exception as e:
#                 insert_etl_log(
#                     processlogid=str(uuid.uuid4()),
#                     source_connection=source_conn,
#                     saved_count=0,
#                     starttime=start_time,
#                     endtime=datetime.now(),
#                     last_extract_dt=last_extract_dt,
#                     success=False,
#                     status="Failed",
#                     errormessage=str(e)
#                 )

#         return f"Processed {len(source_connections)} sources. Total rows saved: {total_saved}"




#     # @task
#     # def consolidate_and_load(all_transactions: list):
#     #     """Consolidate data from all sources and load to destination"""
#     #     # Flatten the list of lists
#     #     consolidated_transactions = []
#     #     for transactions_list in all_transactions:
#     #         print(transactions_list)
#     #         consolidated_transactions.extend(transactions_list)
        
#     #     logging.info(f"Consolidating {len(consolidated_transactions)} transactions from all sources")
#     #     saved_count = save_data_to_postgres(consolidated_transactions)
#     #     return f"Successfully loaded {saved_count} transactions"
    
#     # Get active connections
#     active_connections = get_active_connections()
    
#     # Dynamically create tasks for each connection
#     extract_tasks = extract_from_source.expand(connection_id=active_connections)
    
#     #  Pass both the extracted transactions and the source names
#     load_task = consolidate_and_load(extract_tasks, active_connections)
    
#     # Set dependencies
#     extract_tasks >> load_task

# # Instantiate the DAG
# # dag = dynamic_gl_db_etl()