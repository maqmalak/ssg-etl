"""
PySpark ETL script for transforming hanger lane data.
This script reads data from the pg-ssg database, performs aggregation, and saves the result.
"""

import sys
import glob
import psycopg2
import os
import logging
from pendulum import timezone
from datetime import datetime, timedelta

from airflow.hooks.base import BaseHook
# Add the dags directory to the Python path so we can import db_utils
dags_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'dags')
sys.path.append(os.path.abspath(dags_path))


# Timezone configuration
PKT = timezone("Asia/Karachi")

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


print(f"Python path: {sys.path}")
print(f"DAGs path exists: {os.path.exists(dags_path)}")
print(f"db_utils.py exists: {os.path.exists(os.path.join(dags_path, 'db_utils.py'))}")

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import sum as spark_sum, first, lit, current_date, date_sub
    print("Successfully imported PySpark modules")
except ImportError as e:
    print(f"Error importing PySpark modules: {e}")
    sys.exit(1)

try:
    from dags.db_utils import (
        get_postgres_connection_params, 
        get_postgres_jdbc_properties
    )
    print("Successfully imported db_utils")
except ImportError as e:
    print(f"Error importing db_utils: {e}")
    print(f"Files in dags directory: {os.listdir(dags_path) if os.path.exists(dags_path) else 'Directory not found'}")
    sys.exit(1)


def create_spark_session():
    """Create and configure Spark session"""
    print("Creating Spark session...")
    try:
        # Locate the path to the newer PostgreSQL JDBC driver
        # Try multiple possible paths
        possible_driver_paths = [
            "/opt/airflow/sparkFiles/jdbc-drivers/postgresql-42.7.3.jar",
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "jdbc-drivers", "postgresql-42.7.3.jar"),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "postgresql-42.7.3.jar")
        ]
        
        jdbc_driver_path = None
        for path in possible_driver_paths:
            if os.path.exists(path):
                jdbc_driver_path = path
                print(f"Found PostgreSQL JDBC driver at {jdbc_driver_path}")
                break
        
        # If no driver found, try to download it
        if not jdbc_driver_path:
            print("PostgreSQL JDBC driver not found at any expected location")
            
        if jdbc_driver_path and os.path.exists(jdbc_driver_path):
            print(f"Using PostgreSQL JDBC driver: {jdbc_driver_path}")
            # Create Spark session with explicit JDBC driver configuration
            spark = SparkSession.builder \
                .appName("HangerLaneDataTransformation") \
                .config("spark.jars", jdbc_driver_path) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
        else:
            print("Creating session without explicit JDBC driver...")
            spark = SparkSession.builder \
                .appName("HangerLaneDataTransformation") \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
        
        print("Spark session created successfully")
        return spark
    except Exception as e:
        print(f"Error creating Spark session: {e}")
        import traceback
        traceback.print_exc()
        raise




def get_connection_params_fallback():
    # Get connection parameters from Airflow connection
    try:
        connection = BaseHook.get_connection("pg-ssg")
        host = connection.host
        port = connection.port if connection.port else 5432
        database = connection.schema
        user = connection.login
        password = connection.password
        
        logger.info(f"Using Airflow connection 'pg-ssg':")
        logger.info(f"  Host: {host}")
        logger.info(f"  Port: {port}")
        logger.info(f"  Database: {database}")
        logger.info(f"  User: {user}")
        logger.info(f"  Password length: {len(password) if password else 0}")
    except Exception as e:
        logger.warning(f"Could not get Airflow connection 'pg-ssg', using environment variables: {e}")
        # Fallback to environment variables
        host = os.getenv("POSTGRES_HOST", "172.16.7.6")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "ssg")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "P@kistan12")  # Use correct password
        
        logger.info(f"Environment variables check:")
        logger.info(f"  POSTGRES_HOST: {os.getenv('POSTGRES_HOST', 'Not set')}")
        logger.info(f"  POSTGRES_PORT: {os.getenv('POSTGRES_PORT', 'Not set')}")
        logger.info(f"  POSTGRES_DB: {os.getenv('POSTGRES_DB', 'Not set')}")
        logger.info(f"  POSTGRES_USER: {os.getenv('POSTGRES_USER', 'Not set')}")
        logger.info(f"  POSTGRES_PASSWORD: {'*' * len(os.getenv('POSTGRES_PASSWORD', '')) if os.getenv('POSTGRES_PASSWORD') else 'Not set'}")
        
        logger.info(f"Using connection parameters:")
        logger.info(f"  Host: {host}")
        logger.info(f"  Port: {port}")
        logger.info(f"  Database: {database}")
        logger.info(f"  User: {user}")
        logger.info(f"  Password length: {len(password) if password else 0}")
    
    logger.info(f"Connecting to PostgreSQL database: {database} on {host}:{port} as user {user}")
        
        # Connect to PostgreSQL
    conn = psycopg2.connect(
        host=host,
        port=port,
        database=database,
        user=user,
        password=password
    )

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}"
    }






def transform_data(spark):
    """Transform data to create aggregated tables"""
    try:
        print("Starting data transformation...")
        # Get database connection parameters for PostgreSQL (target)
        print("Getting PostgreSQL connection parameters...")
        
        # Use db_utils.py to get connection parameters with fallback
        try:
            postgres_connection_params = get_postgres_connection_params("pg-ssg")
        except Exception as e:
            print(f"Error getting connection params from db_utils: {e}")
            print("Using fallback method with environment variables...")
            postgres_connection_params = get_connection_params_fallback()
            
        postgres_jdbc_properties = get_postgres_jdbc_properties(postgres_connection_params)
        postgres_jdbc_url = postgres_connection_params["jdbc_url"]
        
        # Print connection details (without password for security)
        print(f"PostgreSQL Host: {postgres_connection_params['host']}")
        print(f"PostgreSQL Port: {postgres_connection_params['port']}")
        print(f"PostgreSQL Database: {postgres_connection_params['database']}")
        print(f"PostgreSQL User: {postgres_connection_params['user']}")
        print(f"PostgreSQL JDBC URL: {postgres_jdbc_url}")
        
        # Debug: Print Spark configuration
        print("Spark configuration:")
        for item in spark.sparkContext.getConf().getAll():
            print(f"  {item[0]}: {item[1]}")
        
        # Read data from postgres database
        # This reads the data that was saved by hanger_lane.py
        print("Reading data from PostgreSQL...")
        try:
            df = spark.read \
                .format("jdbc") \
                .option("url", postgres_jdbc_url) \
                .option("dbtable", "operator_daily_performance") \
                .option("user", postgres_jdbc_properties["user"]) \
                .option("password", postgres_jdbc_properties["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .load()
            
            print(f"Data loaded successfully. Row count: {df.count()}")

            # Filter the data to only include records from the last day
            # We'll do this after loading to avoid SQL dialect issues
            from pyspark.sql.functions import current_date, date_sub
            df = df.filter(df["odp_date"] >= date_sub(current_date(), 1))
            
            print(f"Data filtered successfully. Row count: {df.count()}")
        except Exception as e:
            print(f"Error reading data from PostgreSQL with explicit driver: {str(e)}")
            # If we can't read data with explicit driver, try without specifying the driver
            try:
                print("Retrying without specifying driver class...")
                df = spark.read \
                    .format("jdbc") \
                    .option("url", postgres_jdbc_url) \
                    .option("dbtable", "operator_daily_performance") \
                    .option("user", postgres_jdbc_properties["user"]) \
                    .option("password", postgres_jdbc_properties["password"]) \
                    .load()
                
                # Filter the data to only include records from the last day
                from pyspark.sql.functions import current_date, date_sub
                df = df.filter(df["odp_date"] >= date_sub(current_date(), 1))
                
                print(f"Data loaded and filtered successfully without explicit driver. Row count: {df.count()}")
            except Exception as retry_e:
                print(f"Retry also failed: {str(retry_e)}")
                # If we still can't read data, return early
                return False
        
        # Check if table exists and has data
        try:
            row_count = df.count()
        except Exception as e:
            print(f"Error counting rows: {str(e)}")
            row_count = 0
            
        if row_count == 0:
            print("Warning: No data found in operator_daily_performance table for the last day")
            return True
            
        # Transform 1: Group by odp_date and OC_Description, sum ODPD_Quantity
        print("Performing aggregation 1...")
        try:
            aggregated_df1 = df.groupBy("odp_date", "oc_description", "source_connection") \
                .agg(spark_sum("odpd_quantity").alias("odpd_quantity"))
        except Exception as e:
            print(f"Error in aggregation 1: {str(e)}")
            return False
        
        # Transform 2: Group by odp_date and Shift, sum ODPD_Quantity
        print("Performing aggregation 2...")
        try:
            aggregated_df2 = df.groupBy("odp_date", "shift", "source_connection") \
                .agg(spark_sum("odpd_quantity").alias("odpd_quantity"))
        except Exception as e:
            print(f"Error in aggregation 2: {str(e)}")
            return False
  
        # Transform 3: Group by odp_date and Employee, sum ODPD_Quantity
        print("Performing aggregation 3...")
        try:
            aggregated_df3 = df.groupBy("odp_date", "odp_em_key", "em_firstname" "source_connection") \
                .agg(spark_sum("odpd_quantity").alias("odpd_quantity"))
        except Exception as e:
            print(f"Error in aggregation 3: {str(e)}")
            return False

        # Save the results to their respective tables
        print("Saving aggregated data...")
        try:
            save_with_update_insert(aggregated_df1, "odp_date_oc", postgres_jdbc_url, postgres_jdbc_properties)
            save_with_update_insert(aggregated_df2, "odp_date_shift", postgres_jdbc_url, postgres_jdbc_properties)
            save_with_update_insert(aggregated_df3, "odp_date_employee", postgres_jdbc_url, postgres_jdbc_properties)
        except Exception as e:
            print(f"Error saving aggregated data: {str(e)}")
            return False
        
        try:
            record_count1 = aggregated_df1.count()
            record_count2 = aggregated_df2.count()
            record_count3 = aggregated_df3.count()
            print(f"Successfully transformed and saved {record_count1} records to odp_date_oc table")
            print(f"Successfully transformed and saved {record_count2} records to odp_date_shift table")
            print(f"Successfully transformed and saved {record_count3} records to odp_date_employee table")
        except Exception as e:
            print(f"Error counting saved records: {str(e)}")
        
        return True
        
    except Exception as e:
        print(f"Error in data transformation: {str(e)}")
        import traceback
        traceback.print_exc()
        # Instead of failing completely, let's return False to indicate partial success
        return False
    finally:
        try:
            spark.stop()
            print("Spark session stopped")
        except:
            pass


def save_with_update_insert(df, table_name, jdbc_url, jdbc_properties):
    """
    Save DataFrame with update/insert logic.
    This approach uses a staging table and PostgreSQL's ON CONFLICT clause.
    
    Args:
        df: DataFrame to save
        table_name: Target table name
        jdbc_url: JDBC URL for PostgreSQL
        jdbc_properties: JDBC properties dictionary
    """
    try:
        print(f"Saving data to table: {table_name}")
        print(f"DataFrame row count: {df.count()}")
        
        # For simplicity in this implementation, we'll use overwrite mode
        # A full upsert implementation would require:
        # 1. Creating a staging table
        # 2. Writing data to the staging table
        # 3. Using PostgreSQL's ON CONFLICT clause to perform upsert
        # 4. Cleaning up the staging table
        
        # In a production environment, we would implement proper upsert logic
        # For now, we'll use overwrite mode which replaces all data
        try:
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .mode("overwrite") \
                .save()
        except Exception as e:
            print(f"Error saving data with explicit driver: {str(e)}")
            # Retry without specifying the driver
            try:
                print("Retrying without specifying driver class...")
                df.write \
                    .format("jdbc") \
                    .option("url", jdbc_url) \
                    .option("dbtable", table_name) \
                    .option("user", jdbc_properties["user"]) \
                    .option("password", jdbc_properties["password"]) \
                    .mode("overwrite") \
                    .save()
            except Exception as retry_e:
                print(f"Retry also failed: {str(retry_e)}")
                raise
        
        print(f"Data saved to {table_name} (using overwrite mode)")
        
    except Exception as e:
        print(f"Error saving data to {table_name}: {str(e)}")
        import traceback
        traceback.print_exc()
        raise


if __name__ == "__main__":
    print("Starting Spark ETL process...")
    spark = None
    try:
        spark = create_spark_session()
        success = transform_data(spark)
        print(f"ETL process completed with success: {success}")
        sys.exit(0 if success else 1)
    except Exception as e:
        print(f"Error in main execution: {str(e)}")
        import traceback
        traceback.print_exc()
        if spark:
            try:
                spark.stop()
            except:
                pass
        sys.exit(1)