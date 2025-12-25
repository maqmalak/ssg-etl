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
# sys.path.append(os.path.abspath(dags_path))



# sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Timezone configuration
PKT = timezone("Asia/Karachi")

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

TARGETS = [
    {"table": "odp_date_oc",        "group": ["odp_date", "oc_description", "source_connection"], "pk": ["odp_date", "oc_description", "source_connection"]},
    {"table": "odp_date_shift",     "group": ["odp_date", "shift", "source_connection"],         "pk": ["odp_date", "shift", "source_connection"]},
    {"table": "odp_date_employee",  "group": ["odp_date", "odp_em_key", "em_firstname", "source_connection"], "pk": ["odp_date", "odp_em_key","em_firstname", "source_connection"]},
]

print(f"Python path: {sys.path}")
print(f"DAGs path exists: {os.path.exists(dags_path)}")
print(f"db_utils.py exists: {os.path.exists(os.path.join(dags_path, 'db_utils.py'))}")

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import sum as spark_sum, first, lit, current_date, date_sub
    from pyspark import StorageLevel
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
    """Create and configure Spark session with optimized settings"""
    print("Creating Spark session...")
    try:
        # Spark UI / networking (helps with browser access when running in containers)
        # You can override via env vars:
        #   SPARK_UI_PORT=4040
        #   SPARK_UI_BIND=0.0.0.0
        spark_ui_port = os.getenv("SPARK_UI_PORT", "4040")
        spark_ui_bind = os.getenv("SPARK_UI_BIND")
        
        # Use local mode for stability (avoids executor communication issues)
        # For cluster mode, set SPARK_MASTER_URL environment variable

        spark_master = os.getenv("SPARK_MASTER_URL", "local[4]")
        print(f"Spark mode: {spark_master}")


        # Get Spark master URL from environment or use default
        # spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        # print(f"Connecting to Spark master: {spark_master}")

        if spark_master.startswith("local"):
            print("Running in LOCAL mode (4 threads) - no cluster communication")
        else:
            print(f"Connecting to Spark cluster: {spark_master}")

        # Locate the path to the newer PostgreSQL JDBC driver
        # Try multiple possible paths
        possible_driver_paths = [
            "/opt/airflow/sparkFiles/jdbc-drivers/postgresql-42.7.3.jar",
            "/opt/spark/work/jdbc-drivers/postgresql-42.7.3.jar",
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "jdbc-drivers", "postgresql-42.7.3.jar"),
            os.path.join(os.path.dirname(os.path.abspath(__file__)), "postgresql-42.7.3.jar")
        ]

        jdbc_driver_path = None
        for path in possible_driver_paths:
            if os.path.exists(path):
                jdbc_driver_path = path
                print(f"✓ Found PostgreSQL JDBC driver at {jdbc_driver_path}")
                break

        if not jdbc_driver_path:
            print("⚠ PostgreSQL JDBC driver not found at any expected location")

        # Build Spark session with optimized resource allocation
        builder = (
            SparkSession.builder
            .appName("HangerLaneDataTransformation")
            .master(spark_master)  # ← Explicitly connect to Spark cluster
            
            # Adaptive Query Execution for better performance
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            
            # Resource allocation - optimized for local or cluster mode
            .config("spark.driver.memory", "6g")           # Increased for local mode
            .config("spark.executor.memory", "5g")         # For cluster mode
            .config("spark.executor.cores", "3")           # For cluster mode
            .config("spark.driver.cores", "2")             # For local mode
            .config("spark.executor.instances", "2")       # For cluster mode
            
            # Dynamic shuffle partitions (optimized for local mode)
            .config("spark.sql.shuffle.partitions", "8")   # Reduced for local mode (small data)
            
            # Memory management
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.3")
            
            # Serialization optimization
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer.max", "512m")
            
            # Network timeout settings
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "60s")
        )

        # Configure JDBC driver classpath (multiple approaches for reliability)
        if jdbc_driver_path and os.path.exists(jdbc_driver_path):
            print(f"✓ Using PostgreSQL JDBC driver: {jdbc_driver_path}")
            builder = (builder
                .config("spark.jars", jdbc_driver_path)
                .config("spark.driver.extraClassPath", f"{jdbc_driver_path}:/opt/spark/work/jdbc-drivers/*")
                .config("spark.executor.extraClassPath", f"{jdbc_driver_path}:/opt/spark/work/jdbc-drivers/*")
            )
        else:
            # Fallback to wildcard classpath
            print("Using wildcard classpath for JDBC drivers")
            builder = (builder
                .config("spark.driver.extraClassPath", "/opt/spark/work/jdbc-drivers/*:/opt/airflow/sparkFiles/jdbc-drivers/*")
                .config("spark.executor.extraClassPath", "/opt/spark/work/jdbc-drivers/*:/opt/airflow/sparkFiles/jdbc-drivers/*")
            )

        # Optional: bind UI and pick a fixed port (useful for docker port-mapping)
        if spark_ui_port:
            builder = builder.config("spark.ui.port", spark_ui_port)
        if spark_ui_bind:
            # For driver in containers; makes the UI listen on 0.0.0.0 instead of only localhost
            builder = builder.config("spark.driver.bindAddress", spark_ui_bind)
            builder = builder.config("spark.driver.host", spark_ui_bind)

        spark = builder.getOrCreate()
        
        # Print configuration for debugging
        print("=" * 80)
        print("Spark Session Configuration:")
        print(f"  Master: {spark.sparkContext.master}")
        print(f"  App Name: {spark.sparkContext.appName}")
        print(f"  Spark Version: {spark.version}")
        print(f"  Driver Memory: {spark.sparkContext.getConf().get('spark.driver.memory', 'default')}")
        
        if spark.sparkContext.master.startswith("local"):
            print(f"  Mode: LOCAL (single JVM, no executors)")
            print(f"  Threads: {spark.sparkContext.defaultParallelism}")
        else:
            print(f"  Mode: CLUSTER")
            print(f"  Executor Memory: {spark.sparkContext.getConf().get('spark.executor.memory', 'default')}")
            print(f"  Executor Cores: {spark.sparkContext.getConf().get('spark.executor.cores', 'default')}")
            print(f"  Executor Instances: {spark.sparkContext.getConf().get('spark.executor.instances', 'dynamic')}")
        print("=" * 80)

        print("✓ Spark session created successfully")
        return spark
    except Exception as e:
        print(f"✗ Error creating Spark session: {e}")
        import traceback
        traceback.print_exc()
        raise



def get_connection_params_fallback():
    """Get PostgreSQL connection parameters with fallback logic"""
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


def check_for_recent_data(connection_params: dict = None, days: int = 3) -> int:
    """
    Check if there's recent data in operator_daily_performance table to process.
    
    Args:
        connection_params: Database connection parameters (optional, will fetch if not provided)
        days: Number of days to look back for recent data (default: 3)
    
    Returns:
        int: Number of recent records found
    """
    try:
        # Get connection parameters if not provided
        if not connection_params:
            try:
                from dags.db_utils import get_postgres_connection_params
                connection_params = get_postgres_connection_params("pg-ssg")
            except Exception as e:
                print(f"Error getting connection params from db_utils: {e}")
                connection_params = get_connection_params_fallback()
        
        print(f"Checking for data from last {days} days in operator_daily_performance table...")
        
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host=connection_params.get("host"),
            port=connection_params.get("port", "5432"),
            database=connection_params.get("database"),
            user=connection_params.get("user"),
            password=connection_params.get("password")
        )
        
        cursor = conn.cursor()
        
        # Check if there's recent data
        cursor.execute(f"""
            SELECT COUNT(*) FROM operator_daily_performance 
            WHERE created_at >= CURRENT_DATE - INTERVAL '{days} days'
        """)
        count = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        print(f"✓ Found {count} recent records in operator_daily_performance table")
        return count
        
    except Exception as e:
        print(f"✗ Error checking for recent data: {e}")
        import traceback
        traceback.print_exc()
        return 0






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
        
        # Read data from PostgreSQL using single-connection method (most reliable)
        # Parallel partitioning removed due to persistent EOFException errors
        print("Reading data from PostgreSQL with single-connection method...")
        
        df = None
        row_count = 0
        
        # Define the query
        query = """
           (SELECT odp_date, oc_description, shift, odp_em_key, em_firstname,
                odpd_quantity, source_connection
            FROM operator_daily_performance
            WHERE odp_date >= CURRENT_DATE - INTERVAL '15 days') t
        """
        
        # Primary method: Single-connection read with optimized settings
        try:
            print("Using single-connection read with optimized fetch size and timeouts...")
            
            df = spark.read \
                .format("jdbc") \
                .option("url", postgres_jdbc_url) \
                .option("dbtable", query) \
                .option("user", postgres_jdbc_properties["user"]) \
                .option("password", postgres_jdbc_properties["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .option("fetchsize", "5000") \
                .option("queryTimeout", "600") \
                .option("connectTimeout", "60") \
                .load()
            
            # Persist and count
            df = df.persist(StorageLevel.MEMORY_AND_DISK)
            row_count = df.count()
            
            print(f"✓ Data loaded successfully with single connection. Row count: {row_count:,}")
            
        except Exception as primary_error:
            print(f"⚠ Primary read failed: {primary_error}")
            print("Attempting fallback without explicit driver...")
            
            # Fallback: Let Spark auto-detect the driver
            try:
                df = spark.read \
                    .format("jdbc") \
                    .option("url", postgres_jdbc_url) \
                    .option("dbtable", query) \
                    .option("user", postgres_jdbc_properties["user"]) \
                    .option("password", postgres_jdbc_properties["password"]) \
                    .option("fetchsize", "5000") \
                    .option("queryTimeout", "600") \
                    .load()
                
                df = df.persist(StorageLevel.MEMORY_AND_DISK)
                row_count = df.count()
                
                print(f"✓ Data loaded with auto-driver detection. Row count: {row_count:,}")
                
            except Exception as fallback_error:
                print(f"✗ All read attempts failed: {fallback_error}")
                import traceback
                traceback.print_exc()
                return {
                    "success": False,
                    "data_loaded": 0,
                    "data_filtered": 0,
                    "tables_processed": [],
                    "message": f"Failed to read data from PostgreSQL: {str(fallback_error)}"
                }
        
        # Dynamic shuffle partition calculation based on data size
        if row_count > 0:
            optimal_partitions = max(50, min(200, row_count // 10000))
            spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))
            print(f"✓ Adjusted shuffle partitions to {optimal_partitions} based on data size ({row_count:,} rows)")
        
        # Check if table exists and has data (row_count already computed in the primary read path)
        # If we ended up in fallback read path, compute it once here.
        if 'row_count' not in locals():
            try:
                df = df.persist(StorageLevel.MEMORY_AND_DISK)
                row_count = df.count()
            except Exception as e:
                print(f"Error counting rows: {str(e)}")
                row_count = 0
            
        if row_count == 0:
            print("Warning: No data found in operator_daily_performance table for the last day")
            return True
            
        # Process data using TARGETS configuration
        print("Processing data using TARGETS configuration...")
        tables_processed = []
        try:
            for cfg in TARGETS:
                print(f"Processing target table: {cfg['table']}")

                # Filter out records with NULL values in primary key columns
                if cfg["table"] == "odp_date_oc":
                    # oc_description is part of PK, must not be NULL
                    filtered_df = df.filter(df["oc_description"].isNotNull())
                    if os.getenv("DEBUG_ROWCOUNTS", "0") == "1":
                        filtered_df = filtered_df.persist(StorageLevel.MEMORY_AND_DISK)
                        filtered_count = filtered_df.count()
                        print(f"Filtered out {row_count - filtered_count} records with null oc_description")
                    df_to_process = filtered_df
                elif cfg["table"] == "odp_date_employee":
                    # em_firstname is part of PK, must not be NULL
                    filtered_df = df.filter(df["em_firstname"].isNotNull())
                    if os.getenv("DEBUG_ROWCOUNTS", "0") == "1":
                        filtered_df = filtered_df.persist(StorageLevel.MEMORY_AND_DISK)
                        filtered_count = filtered_df.count()
                        print(f"Filtered out {row_count - filtered_count} records with null em_firstname")
                    df_to_process = filtered_df
                else:
                    df_to_process = df

                # Create aggregated dataframe based on group columns
                agg_df = df_to_process.groupBy(*cfg["group"]) \
                          .agg(spark_sum("odpd_quantity").alias("odpd_quantity"))

                # Counting triggers a full Spark job. Make it optional (only for debugging/metrics).
                record_count = None
                if os.getenv("DEBUG_ROWCOUNTS", "0") == "1":
                    try:
                        record_count = agg_df.count()
                    except Exception as e:
                        print(f"Error counting records for {cfg['table']}: {str(e)}")
                        record_count = 0

                # Perform upsert for this target
                success = upsert_data_via_spark(
                    spark=spark,
                    data_df=agg_df,
                    table_name=cfg["table"],
                    key_columns=cfg["pk"],
                    connection_params=postgres_connection_params
                )

                if not success:
                    print(f"Failed to upsert data to {cfg['table']}")
                    return {
                        "success": False,
                        "data_loaded": row_count,
                        "data_filtered": row_count,
                        "tables_processed": tables_processed,
                        "message": f"Failed to upsert data to {cfg['table']}"
                    }

                tables_processed.append({
                    "table": cfg["table"],
                    "records": record_count if record_count is not None else "(count disabled)"
                })

                if record_count is not None:
                    print(f"Successfully upserted {record_count} records to {cfg['table']} table")
                else:
                    print(f"Successfully upserted records to {cfg['table']} table (count disabled)")

        except Exception as e:
            print(f"Error processing data with TARGETS: {str(e)}")
            return {
                "success": False,
                "data_loaded": row_count,
                "data_filtered": row_count,
                "tables_processed": tables_processed,
                "message": f"Error processing data: {str(e)}"
            }

        return {
            "success": True,
            "data_loaded": row_count,
            "data_filtered": row_count,
            "tables_processed": tables_processed,
            "message": "Data transformation completed successfully"
        }
        
    except Exception as e:
        print(f"Error in data transformation: {str(e)}")
        import traceback
        traceback.print_exc()
        # Instead of failing completely, let's return False to indicate partial success
        return False
    finally:
        try:
            # Unpersist if we cached the main df
            if 'df' in locals():
                try:
                    df.unpersist(blocking=False)
                except Exception:
                    pass

            spark.stop()
            print("Spark session stopped")
        except:
            pass


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
                from dags.db_utils import get_postgres_connection_params
                connection_params = get_postgres_connection_params("pg-ssg")
            except Exception as e:
                print(f"Error getting connection params: {e}")
                return False

        # Import psycopg2 here since it's needed for the function
        import psycopg2
        import uuid

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
