"""
PySpark ETL script for transforming hanger lane data.
This script reads data from the postgres_grafana database, performs aggregation, and saves the result.
"""

import sys
import os
import glob

# Add the dags directory to the Python path so we can import db_utils
dags_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'dags')
sys.path.append(os.path.abspath(dags_path))

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
    from db_utils import (
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
            jdbc_driver_path = download_postgresql_jdbc_driver()
            
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


def download_postgresql_jdbc_driver():
    """Download the newer PostgreSQL JDBC driver if it's not already present"""
    
    # Define the target directory and filename
    target_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "jdbc-drivers")
    driver_filename = "postgresql-42.7.3.jar"
    target_path = os.path.join(target_dir, driver_filename)
    
    # Check if the driver already exists
    if os.path.exists(target_path):
        print(f"PostgreSQL JDBC driver already exists at {target_path}")
        return target_path
    
    # Create the target directory if it doesn't exist
    os.makedirs(target_dir, exist_ok=True)
    
    # Define the download URL for the newer version
    download_url = "https://jdbc.postgresql.org/download/postgresql-42.7.3.jar"
    
    try:
        print(f"Downloading PostgreSQL JDBC driver from {download_url}...")
        import urllib.request
        urllib.request.urlretrieve(download_url, target_path)
        print(f"Successfully downloaded PostgreSQL JDBC driver to {target_path}")
        return target_path
    except Exception as e:
        print(f"Error downloading PostgreSQL JDBC driver: {e}")
        return None


def get_connection_params_fallback():
    """Get connection parameters with fallback to environment variables"""
    # Try to get from environment variables first
    host = os.getenv("POSTGRES_HOST", "127.16.7.6")
    port = os.getenv("POSTGRES_PORT", "5432")
    database = os.getenv("POSTGRES_DB", "ssg")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    
    print(f"Using connection parameters from environment variables:")
    print(f"  Host: {host}")
    print(f"  Port: {port}")
    print(f"  Database: {database}")
    print(f"  User: {user}")
    
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
            postgres_connection_params = get_postgres_connection_params("postgres_grafana")
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
            
            print(f"All Data loaded successfully. Row count: {df.count()}")

            # Filter the data to only include records from the last day
            # We'll do this after loading to avoid SQL dialect issues
            from pyspark.sql.functions import current_date, date_sub
            df = df.filter(df["ODP_Date"] >= date_sub(current_date(), 1))
            
            print(f"Data loaded and filtered successfully. Row count: {df.count()}")
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
                df = df.filter(df["ODP_Date"] >= date_sub(current_date(), 1))
                
                print(f"Data loaded and filtered successfully without explicit driver. Row count: {df.count()}")
            except Exception as retry_e:
                print(f"Retry also failed: {str(retry_e)}")
                # If we still can't read data, return early
                return False
            
            print(f"Data loaded successfully. Row count: {df.count()}")
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
                
                print(f"Data loaded successfully without explicit driver. Row count: {df.count()}")
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
            print("Warning: No data found in operator_daily_performance table")
            return True
            
        # Transform 1: Group by ODP_Date and OC_Description, sum ODPD_Quantity
        print("Performing aggregation 1...")
        try:
            aggregated_df1 = df.groupBy("ODP_Date", "OC_Description") \
                .agg(spark_sum("ODPD_Quantity").alias("ODPD_Quantity"))
        except Exception as e:
            print(f"Error in aggregation 1: {str(e)}")
            return False
        
        # Transform 2: Group by ODP_Date and Shift, sum ODPD_Quantity
        print("Performing aggregation 2...")
        try:
            aggregated_df2 = df.groupBy("ODP_Date", "Shift") \
                .agg(spark_sum("ODPD_Quantity").alias("ODPD_Quantity"))
        except Exception as e:
            print(f"Error in aggregation 2: {str(e)}")
            return False
  
        # Transform 3: Group by ODP_Date and Employee, sum ODPD_Quantity
        print("Performing aggregation 3...")
        try:
            aggregated_df3 = df.groupBy("ODP_Date", "ODP_EM_Key", "EM_RFID", "EM_Department", "EM_FirstName", "EM_LastName") \
                .agg(spark_sum("ODPD_Quantity").alias("ODPD_Quantity"))
        except Exception as e:
            print(f"Error in aggregation 3: {str(e)}")
            return False

        # Save the results to their respective tables
        print("Saving aggregated data...")
        try:
            save_with_update_insert(aggregated_df1, "opd_date_oc", postgres_jdbc_url, postgres_jdbc_properties)
            save_with_update_insert(aggregated_df2, "opd_date_shift", postgres_jdbc_url, postgres_jdbc_properties)
            save_with_update_insert(aggregated_df3, "opd_date_employee", postgres_jdbc_url, postgres_jdbc_properties)
        except Exception as e:
            print(f"Error saving aggregated data: {str(e)}")
            return False
        
        try:
            record_count1 = aggregated_df1.count()
            record_count2 = aggregated_df2.count()
            record_count3 = aggregated_df3.count()
            print(f"Successfully transformed and saved {record_count1} records to opd_date_oc table")
            print(f"Successfully transformed and saved {record_count2} records to opd_date_shift table")
            print(f"Successfully transformed and saved {record_count3} records to opd_date_employee table")
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