"""
Python-based ETL script for transforming hanger lane data.
This script reads data from the postgres_grafana database, performs aggregation, and saves the result.
"""

import sys
import os
import pandas as pd
from sqlalchemy import create_engine, text
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Add the dags directory to the Python path so we can import db_utils
dags_path = os.path.join(os.path.dirname(__file__), '..', 'dags')
sys.path.append(dags_path)

try:
    from db_utils import get_postgres_connection_params, get_postgres_jdbc_properties
    logger.info("Successfully imported db_utils")
except ImportError as e:
    logger.error(f"Error importing db_utils: {e}")
    sys.exit(1)


def get_db_connection():
    """Create and return a database connection"""
    try:
        # Get connection parameters
        connection_params = get_postgres_connection_params("postgres_grafana")
        
        # Create connection string
        uri = f"postgresql://{connection_params['user']}:{connection_params['password']}@{connection_params['host']}:{connection_params['port']}/{connection_params['database']}"
        
        # Create engine and connection
        engine = create_engine(uri)
        connection = engine.connect()
        logger.info("Database connection created successfully")
        return connection, engine
    except Exception as e:
        logger.error(f"Error creating database connection: {e}")
        raise


def transform_data():
    """Transform data to create aggregated tables"""
    connection = None
    engine = None
    try:
        logger.info("Starting data transformation...")
        
        # Get database connection
        connection, engine = get_db_connection()
        
        # Read data from postgres_grafana database
        logger.info("Reading data from PostgreSQL...")
        query = "SELECT * FROM operator_daily_performance"
        df = pd.read_sql(query, connection)
        
        logger.info(f"Data loaded successfully. Row count: {len(df)}")
        
        # Check if table exists and has data
        if len(df) == 0:
            logger.warning("Warning: No data found in operator_daily_performance table")
            return True
            
        # Transform 1: Group by ODP_Date and OC_Description, sum ODPD_Quantity
        logger.info("Performing aggregation 1...")
        aggregated_df1 = df.groupby(['odp_date', 'oc_description'])['odpd_quantity'].sum().reset_index()
        aggregated_df1['source_connection'] = 'Lane-21'  # Default value
        aggregated_df1.rename(columns={'odpd_quantity': 'odpd_quantity'}, inplace=True)
        
        # Transform 2: Group by ODP_Date and Shift, sum ODPD_Quantity
        logger.info("Performing aggregation 2...")
        aggregated_df2 = df.groupby(['odp_date', 'shift'])['odpd_quantity'].sum().reset_index()
        aggregated_df2['source_connection'] = 'Lane-21'  # Default value
        aggregated_df2.rename(columns={'odpd_quantity': 'odpd_quantity'}, inplace=True)
        
        # Transform 3: Group by ODP_Date and Employee, sum ODPD_Quantity
        logger.info("Performing aggregation 3...")
        aggregated_df3 = df.groupby(['odp_date', 'odp_em_key', 'em_rfid', 'em_department', 'em_first_name', 'em_last_name'])['odpd_quantity'].sum().reset_index()
        aggregated_df3['source_connection'] = 'Lane-21'  # Default value
        aggregated_df3.rename(columns={'odpd_quantity': 'odpd_quantity'}, inplace=True)
        
        # Save the results to their respective tables
        logger.info("Saving aggregated data...")
        aggregated_df1.to_sql('opd_date_oc', engine, if_exists='replace', index=False)
        aggregated_df2.to_sql('opd_date_shift', engine, if_exists='replace', index=False)
        aggregated_df3.to_sql('opd_date_employee', engine, if_exists='replace', index=False)
        
        logger.info(f"Successfully transformed and saved {len(aggregated_df1)} records to opd_date_oc table")
        logger.info(f"Successfully transformed and saved {len(aggregated_df2)} records to opd_date_shift table")
        logger.info(f"Successfully transformed and saved {len(aggregated_df3)} records to opd_date_employee table")
        return True
        
    except Exception as e:
        logger.error(f"Error in data transformation: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        # Close connections
        if connection:
            try:
                connection.close()
            except:
                pass
        if engine:
            try:
                engine.dispose()
            except:
                pass


if __name__ == "__main__":
    logger.info("Starting Python ETL process...")
    try:
        success = transform_data()
        logger.info(f"ETL process completed with success: {success}")
        sys.exit(0 if success else 1)
    except Exception as e:
        logger.error(f"Error in main execution: {str(e)}")
        import traceback
        traceback.print_exc()
        sys.exit(1)



    # @task
    # def transform() -> str:
    #     """
    #     Transform the extracted data using Python pandas.
    #     This task executes the python_transform.py script which:
    #     1. Reads data from the operator_daily_performance table
    #     2. Aggregates sum(ODPD_Quantity) grouped by ODP_Date and OC_Description
    #     3. Saves the result to the aggregated tables
    #     """
    #     import subprocess
    #     import os
        
    #     # Path to the Python transform script
    #     transform_script_path = "/opt/airflow/sparkFiles/python_transform.py"
        
    #     # Check if the script exists
    #     if not os.path.exists(transform_script_path):
    #         logger.error(f"Python transform script not found at {transform_script_path}")
    #         raise FileNotFoundError(f"Python transform script not found at {transform_script_path}")
        
    #     # Execute the Python transform script
    #     try:
    #         result = subprocess.run(
    #             ["python", transform_script_path],
    #             capture_output=True,
    #             text=True,
    #             check=True
    #         )
    #         logger.info("Python transformation completed successfully")
    #         logger.info(f"Python script output: {result.stdout}")
    #         if result.stderr:
    #             logger.info(f"Python script stderr: {result.stderr}")
    #         return "Transformation task executed successfully"
    #     except subprocess.CalledProcessError as e:
    #         logger.error(f"Python transformation failed with error: {e}")
    #         logger.error(f"Python script stdout: {e.stdout}")
    #         logger.error(f"Python script stderr: {e.stderr}")
    #         return f"Transformation task failed: {str(e)}"
    #     except Exception as e:
    #         logger.error(f"Unexpected error during Python transformation: {e}")
    #         return f"Transformation task failed: {str(e)}"
