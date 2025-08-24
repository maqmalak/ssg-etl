from datetime import datetime, timedelta
from airflow.decorators import dag, task
from sqlalchemy.orm import sessionmaker
from sqlalchemy import create_engine, Column, Integer, String, DateTime, Numeric, Text, Date, Float, Boolean
from sqlalchemy.ext.declarative import declarative_base
from airflow.hooks.base import BaseHook
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
import uuid
from airflow.operators.python import get_current_context
import logging
import pyodbc

from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.operators.bash import BashOperator
from airflow.utils.trigger_rule import TriggerRule


from scripts.constans.db_sources import SOURCE_HANGER_LANE

# Define the target table model
Base = declarative_base()


def get_postgres_engine():
    """Create SQLAlchemy engine for destination PostgreSQL database"""
    # Get the connection details from Airflow
    connection = BaseHook.get_connection('postgres_grafana')
    uri = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
    return create_engine(uri)

def create_table_if_not_exists(engine):
    """Create the transactions table if it doesn't exist"""
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
    
    print(f"Connecting to source database: {conn}")

    # Manual join query to fetch data with proper joins
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
        WHERE created_at>=DATEADD(MINUTE, -10, GETDATE());
    """

    try:
        with pyodbc.connect(conn) as connection:
            result = connection.execute(query)
            print(f"Fetched {result.rowcount} rows from {conn}")
            logging.info(f"Fetched {result.rowcount} rows from {conn}")

            hanger_data = []
            # Process the result set
            for row in result:
                hanger_data.append({
                    'ODP_Key': row.ODP_Key,
                    'ODP_Date': row.ODP_Date,
                    'Shift': row.Shift,
                    'ODP_EM_Key': int(row.ODP_EM_Key) if row.ODP_EM_Key and str(row.ODP_EM_Key).isdigit() else 0,
                    'EM_RFID': row.EM_RFID,
                    'EM_Department': row.EM_Department,
                    'EM_FirstName': row.EM_FirstName,
                    'EM_LastName': row.EM_LastName,
                    'ODP_Actual_Clock_In': row.ODP_Actual_Clock_In,
                    'ODP_Actual_Clock_Out': row.ODP_Actual_Clock_Out,
                    'ODP_Shift_Clock_In': row.ODP_Shift_Clock_In,
                    'ODP_Shift_Clock_Out': row.ODP_Shift_Clock_Out,
                    'ODP_First_Hanger_Time': row.ODP_First_Hanger_Time,
                    'ODP_Last_Hanger_Time': row.ODP_Last_Hanger_Time,
                    'ODP_Current_Station': row.ODP_Current_Station,
                    'ODP_Lump_Sum_Payment': float(row.ODP_Lump_Sum_Payment) if row.ODP_Lump_Sum_Payment else 0.0,
                    'ODP_Make_Up_Pay_Rate': float(row.ODP_Make_Up_Pay_Rate) if row.ODP_Make_Up_Pay_Rate else 0.0,
                    'ODP_Last_Hanger_Start_Time': row.ODP_Last_Hanger_Start_Time,
                    'ODPD_Key': row.ODPD_Key,
                    'ODPD_Workstation': row.ODPD_Workstation,
                    'ODPD_WC_Key': int(row.ODPD_WC_Key) if row.ODPD_WC_Key and str(row.ODPD_WC_Key).isdigit() else 0,
                    'ODPD_Quantity': int(row.ODPD_Quantity) if row.ODPD_Quantity and str(row.ODPD_Quantity).isdigit() else 0,
                    'ODPD_ST_Key': int(row.ODPD_ST_Key) if row.ODPD_ST_Key and str(row.ODPD_ST_Key).isdigit() else 0,
                    'ST_ID': int(row.ST_ID) if row.ST_ID and str(row.ST_ID).isdigit() else 0,
                    'ST_Description': row.ST_Description,
                    'ODPD_Lot_Number': row.ODPD_Lot_Number,
                    'ODPD_OC_Key': int(row.ODPD_OC_Key) if row.ODPD_OC_Key and str(row.ODPD_OC_Key).isdigit() else 0,
                    'OC_Description': row.OC_Description,
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
                    'CM_Description': row.CM_Description,
                    'ODPD_SM_Key': int(row.ODPD_SM_Key) if row.ODPD_SM_Key and str(row.ODPD_SM_Key).isdigit() else 0,
                    'SM_Description': row.SM_Description,
                    'ODPD_Normal_Pay_Factor': float(row.ODPD_Normal_Pay_Factor) if row.ODPD_Normal_Pay_Factor else 0.0,
                    'ODPD_Is_Overtime': row.ODPD_Is_Overtime,
                    'ODPD_Overtime_Factor': float(row.ODPD_Overtime_Factor) if row.ODPD_Overtime_Factor else 0.0,
                    'ODPD_Edited_By': row.ODPD_Edited_By,
                    'ODPD_Edited_Date': row.ODPD_Edited_Date,
                    'ODPD_Actual_Time_From_Reader': float(row.ODPD_Actual_Time_From_Reader) if row.ODPD_Actual_Time_From_Reader else 0.0,
                    'ODPD_STPO_Key': int(row.ODPD_STPO_Key) if row.ODPD_STPO_Key and str(row.ODPD_STPO_Key).isdigit() else 0,
                    'created_at': row.created_at,
                    'source_connection': connection_id
                })

            return hanger_data

    except Exception as e:
        logging.error(f"Error fetching data from {connection_id}: {str(e)}")
    return []

def save_data_to_postgres(hanger_data):
    """Save hanger lane data to PostgreSQL"""
    try:
        engine = get_postgres_engine()
        create_table_if_not_exists(engine)
        Session = sessionmaker(bind=engine)
        session = Session()
        
        for data in hanger_data:
            # Handle any data conversion issues
            try:
                hanger_record = HangerLaneData(**data)
                session.add(hanger_record)
            except Exception as e:
                logging.error(f"Error adding hanger record: {str(e)}")
                # Continue with other records
                continue
        
        session.commit()
        session.close()
        logging.info(f"Successfully saved {len(hanger_data)} hanger records to PostgreSQL")
        return len(hanger_data)
    except Exception as e:
        logging.error(f"Error saving data to PostgreSQL: {str(e)}")
        raise

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=30)
}

@dag(
    dag_id='hanger_data_mssql_to_pg_etl',
    default_args=default_args,
    schedule="*/10 * * * *",  # Run every 10 minutes
    tags=["ssg", "hanger_data_to_pg"],
    catchup=False,
    max_active_runs=1
)
def dynamic_hanger_db_etl():
    
    @task
    def get_active_connections():
        """Get list of active database connections"""

        # """Fetch data from a specific source"""
        # # Get the source database connection
        # connection = BaseHook.get_connection(connection_id)
    
        # #    Create FreeTDS connection string
        # conn = (
        #     "DRIVER={FreeTDS};"
        #     f"SERVER={connection.host};"
        #     "PORT=1433;"
        #     f"DATABASE={connection.schema};"
        #     f"UID={connection.login};"
        #     f"PWD={connection.password};"
        #     "TDS_Version=7.0;"
        # )
    
        # print(f"Connecting to source database: {conn}")
        
        return SOURCE_HANGER_LANE
    
    @task
    def extract_from_source(connection_id: str):
        """Extract data from a specific source"""
        logging.info(f"Extracting data from source: {connection_id}")
        hanger_data = fetch_data_from_source(connection_id)
        print(f"Extracted {len(hanger_data)} records from {connection_id}")
        logging.info(f"Extracted {len(hanger_data)} records from {connection_id}")
        return hanger_data
    
    @task
    def consolidate_and_load(all_data: list):
        """Consolidate data from all sources and load to destination"""
        # Flatten the list of lists
        consolidated_data = []
        for data_list in all_data:
            print(data_list)
            consolidated_data.extend(data_list)
        
        logging.info(f"Consolidating {len(consolidated_data)} records from all sources")
        saved_count = save_data_to_postgres(consolidated_data)
        return f"Successfully loaded {saved_count} records"
    
    # Get active connections
    active_connections = get_active_connections()
    
    # Dynamically create tasks for each connection
    extract_tasks = extract_from_source.expand(connection_id=active_connections)
    
    # Consolidate and load all data
    load_task = consolidate_and_load(extract_tasks)
    
    # Set dependencies
    active_connections >> extract_tasks >> load_task

# Instantiate the DAG
dag = dynamic_hanger_db_etl()