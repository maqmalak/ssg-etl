"""
Utility functions for performing upsert operations on PostgreSQL tables from Spark.
"""

import sys
import os
import uuid

# Add the dags directory to the Python path so we can import db_utils
dags_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'dags')
sys.path.append(os.path.abspath(dags_path))

try:
    from pyspark.sql import SparkSession
    from db_utils import (
        get_postgres_connection_params, 
        get_postgres_jdbc_properties
    )
    import psycopg2
    print("Successfully imported required modules")
except ImportError as e:
    print(f"Error importing modules: {e}")
    sys.exit(1)


def upsert_data_via_spark(
    spark: SparkSession,
    data_df,
    table_name: str,
    key_columns: list,
    connection_params: dict = None
) -> bool:
    """
    Perform upsert operation on PostgreSQL table using Spark with staging table approach.
    
    Args:
        spark: SparkSession instance
        data_df: DataFrame containing the data to upsert
        table_name: Name of the target table
        key_columns: List of column names that form the primary key
        connection_params: Database connection parameters (optional)
        
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if data_df is None or data_df.rdd.isEmpty():
            print("No data to upsert")
            return True
            
        # Get connection parameters if not provided
        if not connection_params:
            try:
                connection_params = get_postgres_connection_params("postgres_grafana")
            except Exception as e:
                print(f"Error getting connection params: {e}")
                return False
                
        jdbc_properties = get_postgres_jdbc_properties(connection_params)
        jdbc_url = connection_params["jdbc_url"]
        
        # Generate a unique staging table name
        staging_table = f"{table_name}_staging_{str(uuid.uuid4()).replace('-', '_')}"
        
        try:
            # Create staging table with same structure as target table
            # First, we need to connect directly to PostgreSQL to execute DDL statements
            conn = psycopg2.connect(
                host=connection_params.get("host"),
                port=connection_params.get("port", "5432"),
                database=connection_params.get("database"),
                user=connection_params.get("user"),
                password=connection_params.get("password")
            )
            cursor = conn.cursor()
            
            # Drop staging table if exists
            cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
            
            # Create staging table with same structure as target table
            cursor.execute(f"CREATE TABLE {staging_table} (LIKE {table_name} INCLUDING ALL);")
            conn.commit()
            cursor.close()
            conn.close()
            
            # Write data to staging table using Spark
            data_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", staging_table) \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .mode("append") \
                .save()
            
            # Now perform upsert using ON CONFLICT
            conn = psycopg2.connect(
                host=connection_params.get("host"),
                port=connection_params.get("port", "5432"),
                database=connection_params.get("database"),
                user=connection_params.get("user"),
                password=connection_params.get("password")
            )
            cursor = conn.cursor()
            
            # Get all column names from the DataFrame
            columns = data_df.columns
            all_columns_str = ", ".join(columns)
            
            # Create the SET clause for UPDATE (excluding key columns)
            set_columns = [col for col in columns if col not in key_columns]
            set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in set_columns])
            
            # Create key columns string for ON CONFLICT clause
            key_columns_str = ", ".join(key_columns)
            
            # UPSERT SQL statement
            upsert_sql = f"""
            INSERT INTO {table_name} ({all_columns_str})
            SELECT {all_columns_str} FROM {staging_table}
            ON CONFLICT ({key_columns_str})
            DO UPDATE SET {set_clause};
            """
            
            # Execute upsert
            cursor.execute(upsert_sql)
            conn.commit()
            
            # Clean up staging table
            cursor.execute(f"DROP TABLE {staging_table};")
            conn.commit()
            cursor.close()
            conn.close()
            
            print(f"Data successfully upserted to {table_name}")
            return True
            
        except Exception as e:
            print(f"Error during upsert operation: {str(e)}")
            # Try to clean up staging table if it exists
            try:
                conn = psycopg2.connect(
                    host=connection_params.get("host"),
                    port=connection_params.get("port", "5432"),
                    database=connection_params.get("database"),
                    user=connection_params.get("user"),
                    password=connection_params.get("password")
                )
                cursor = conn.cursor()
                cursor.execute(f"DROP TABLE IF EXISTS {staging_table};")
                conn.commit()
                cursor.close()
                conn.close()
            except:
                pass
            raise
        
    except Exception as e:
        print(f"Error in upsert_data_via_spark: {str(e)}")
        import traceback
        traceback.print_exc()
        return False


def create_spark_session_with_jdbc():
    """Create and configure Spark session with JDBC driver"""
    print("Creating Spark session with JDBC driver...")
    try:
        # Locate the path to the PostgreSQL JDBC driver
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
            # Try to import the download function
            try:
                from sparkProcess import download_postgresql_jdbc_driver
                jdbc_driver_path = download_postgresql_jdbc_driver()
            except ImportError:
                print("Could not import download function")
                jdbc_driver_path = None
                
        if jdbc_driver_path and os.path.exists(jdbc_driver_path):
            print(f"Using PostgreSQL JDBC driver: {jdbc_driver_path}")
            # Create Spark session with explicit JDBC driver configuration
            spark = SparkSession.builder \
                .appName("UpsertToPostgreSQL") \
                .config("spark.jars", jdbc_driver_path) \
                .config("spark.sql.adaptive.enabled", "true") \
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                .getOrCreate()
        else:
            print("Creating session without explicit JDBC driver...")
            spark = SparkSession.builder \
                .appName("UpsertToPostgreSQL") \
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


if __name__ == "__main__":
    print("Testing upsert functionality...")
    # This would be called from other modules, not directly
    print("This module is intended to be imported and used by other scripts.")
