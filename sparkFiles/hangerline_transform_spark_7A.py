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
from sqlalchemy import create_engine, text
from sqlalchemy.orm import sessionmaker
from sqlalchemy.dialects.postgresql import insert


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
    {"table": "operator_daily_performance","pk": ["source_connection","odp_key","odpd_key"]},
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


# ---------------- DB INA-7A SOURCE CONNECTIONS ---------------- #
def get_postgres_source_connection():
    """
    Get connection parameters for INA-7A source database.
    Returns a dictionary with connection details for JDBC and direct connections.
    """
    # Get connection parameters from Airflow connection
    try:
        connection = BaseHook.get_connection("INA-7A")
        host = connection.host
        port = connection.port if connection.port else 5433
        database = connection.schema
        user = connection.login
        password = connection.password
        
        logger.info(f"Using Airflow connection 'INA-7A':")
        logger.info(f"  Host: {host}")
        logger.info(f"  Port: {port}")
        logger.info(f"  Database: {database}")
        logger.info(f"  User: {user}")
        logger.info(f"  Password length: {len(password) if password else 0}")
    except Exception as e:
        logger.warning(f"Could not get Airflow connection 'INA-7A', using environment variables: {e}")
        # Fallback to environment variables
        host = os.getenv("INA_7A_HOST", "localhost")
        port = int(os.getenv("INA_7A_PORT", "5433"))
        database = os.getenv("INA_7A_DATABASE", "postgres")
        user = os.getenv("INA_7A_USER", "postgres")
        password = os.getenv("INA_7A_PASSWORD", "")
    
    logger.info(f"Source connection - PostgreSQL database: {database} on {host}:{port} as user {user}")

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}"
    }


# ---------------- DB pg-ssg TARGET CONNECTIONS ---------------- #
def get_target_postgres_connection():
    """
    Get connection parameters for pg-ssg target database.
    Returns a dictionary with connection details for JDBC and direct connections.
    """
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
        host = os.getenv("TARGET_PG_HOST", "172.16.7.6")
        port = int(os.getenv("TARGET_PG_PORT", "5432"))
        database = os.getenv("TARGET_PG_DATABASE", "ssg")
        user = os.getenv("TARGET_PG_USER", "postgres")
        password = os.getenv("TARGET_PG_PASSWORD", "")
    
    logger.info(f"Target connection - PostgreSQL database: {database} on {host}:{port} as user {user}")

    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}"
    }



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
        # local[8] uses 8 threads for better parallelism with JDBC workloads

        spark_master = os.getenv("SPARK_MASTER_URL", "local[8]")
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
            .appName("INA-7A-DataTransformation")
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

def check_for_recent_data(spark: SparkSession = None, last_extract_dt=None, days: int = 1) -> int:
    """
    Check if there's recent data in pmr_production_data table to process using Spark cluster.

    Args:
        spark: SparkSession instance (optional, will create if not provided)
        last_extract_dt: Last extract datetime from ETL log (optional)
                        If provided, checks for data since this datetime
        days: Number of days to look back for recent data (default: 1, used if last_extract_dt is None)
             If days > 5000, counts all records in the table (full table mode)

    Returns:
        int: Number of recent records found
    """
    spark_created = False
    try:
        # Create Spark session if not provided
        if spark is None:
            print("Creating Spark session for data check...")
            spark = create_spark_session()
            spark_created = True

        # Get source connection parameters (INA-7A)
        print("Getting source database connection (INA-7A)...")
        source_conn = get_postgres_source_connection()

        # Build query based on last_extract_dt or days parameter
        if last_extract_dt is not None:
            # Use last extract datetime from ETL log
            # Format datetime for SQL query
            if hasattr(last_extract_dt, 'strftime'):
                last_extract_str = last_extract_dt.strftime('%Y-%m-%d %H:%M:%S')
            else:
                last_extract_str = str(last_extract_dt)

            query = f"""
            (
                SELECT COUNT(*) as record_count
                FROM pmr_production_data
                WHERE ppd_complete_time >= '{last_extract_str}'::timestamp
            ) t
            """
            print(f"📅 Checking for data since last extract ({last_extract_str}) using Spark cluster...")
        elif days > 5000:
            # Full table mode - count all records
            query = """
            (
                SELECT COUNT(*) as record_count
                FROM pmr_production_data
            ) t
            """
            print("🔄 Checking FULL TABLE: Counting all records in pmr_production_data...")
        else:
            # Date-filtered mode (fallback to days-based filtering)
            query = f"""
            (
                SELECT COUNT(*) as record_count
                FROM pmr_production_data
                WHERE ppd_complete_time >= CURRENT_DATE - INTERVAL '{days} days'
            ) t
            """
            print(f"📅 Checking for data in last {days} days using Spark cluster...")

        # Read data using Spark JDBC
        count_df = spark.read \
            .format("jdbc") \
            .option("url", source_conn["jdbc_url"]) \
            .option("dbtable", query) \
            .option("user", source_conn["user"]) \
            .option("password", source_conn["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        # Get the count
        count = count_df.first()["record_count"]

        if last_extract_dt is not None:
            print(f"✓ Found {count:,} records in pmr_production_data since last extract (via Spark)")
        elif days > 360:
            print(f"✓ Found {count:,} total records in pmr_production_data table (full table mode)")
        else:
            print(f"✓ Found {count:,} recent records in pmr_production_data table (via Spark)")
        return count

    except Exception as e:
        print(f"✗ Error checking for data: {e}")
        import traceback
        traceback.print_exc()
        return 0
    finally:
        # Stop Spark session if we created it
        if spark_created and spark is not None:
            try:
                spark.stop()
                print("Spark session stopped after data check")
            except:
                pass


def transform_data(spark: SparkSession = None, last_extract_dt=None, days: int = 1, chunked: bool = True):
    """Transform data to create aggregated tables with chunked processing for large datasets"""
    spark_created = False
    try:
        # Create Spark session if not provided
        if spark is None:
            print("Creating Spark session for data transformation...")
            spark = create_spark_session()
            spark_created = True

        print("Starting data transformation...")

        # Get SOURCE database connection parameters (INA-7A) for reading data
        print("Getting SOURCE database connection parameters (INA-7A)...")
        try:
            source_connection_params = get_postgres_source_connection()
            print(f"✓ Source connection established: {source_connection_params['database']}")
        except Exception as e:
            print(f"Error getting source connection params: {e}")
            raise

        # Get TARGET database connection parameters (pg-ssg) for writing data
        print("Getting TARGET database connection parameters (pg-ssg)...")
        try:
            target_connection_params = get_target_postgres_connection()
            print(f"✓ Target connection established: {target_connection_params['database']}")
        except Exception as e:
            print(f"Error getting target connection params: {e}")
            raise

        # Use SOURCE connection for reading data
        source_jdbc_properties = get_postgres_jdbc_properties(source_connection_params)
        source_jdbc_url = source_connection_params["jdbc_url"]

        # Print SOURCE connection details (without password for security)
        print("=" * 80)
        print("SOURCE Connection (INA-7A) - Reading Data From:")
        print(f"  Host: {source_connection_params['host']}")
        print(f"  Port: {source_connection_params['port']}")
        print(f"  Database: {source_connection_params['database']}")
        print(f"  User: {source_connection_params['user']}")
        print(f"  JDBC URL: {source_jdbc_url}")
        print("=" * 80)
        print("TARGET Connection (pg-ssg) - Writing Data To:")
        print(f"  Host: {target_connection_params['host']}")
        print(f"  Port: {target_connection_params['port']}")
        print(f"  Database: {target_connection_params['database']}")
        print(f"  User: {target_connection_params['user']}")
        print(f"  JDBC URL: {target_connection_params['jdbc_url']}")
        print("=" * 80)

        # Debug: Print Spark configuration
        print("Spark configuration:")
        for item in spark.sparkContext.getConf().getAll():
            print(f"  {item[0]}: {item[1]}")

        if chunked:
            # Use chunked processing for large datasets
            return transform_data_chunked(spark, source_connection_params, target_connection_params, last_extract_dt, days)
        else:
            # Use original single-query approach (for smaller datasets)
            return transform_data_single(spark, source_connection_params, target_connection_params, last_extract_dt, days)

    except Exception as e:
        print(f"Error in data transformation: {str(e)}")
        import traceback
        traceback.print_exc()
        # Instead of failing completely, let's return False to indicate partial success
        return False
    finally:
        try:
            # Stop Spark session if we created it
            if spark_created and spark is not None:
                spark.stop()
                print("Spark session stopped after data transformation")
        except:
            pass


def transform_data_single(spark: SparkSession, source_connection_params: dict, target_connection_params: dict, last_extract_dt=None, days: int = 1):
    """Original single-query data transformation (for smaller datasets)"""
    print("Using single-query processing approach...")

    # Use SOURCE connection for reading data
    source_jdbc_properties = get_postgres_jdbc_properties(source_connection_params)
    source_jdbc_url = source_connection_params["jdbc_url"]

    # Read data from PostgreSQL using single-connection method (most reliable)
    # Parallel partitioning removed due to persistent EOFException errors
    print("Reading data from PostgreSQL with single-connection method...")

    df = None
    row_count = 0

    # Build query based on last_extract_dt or days parameter
    if last_extract_dt is not None:
        # Use last extract datetime from ETL log
        # Format datetime for SQL query
        if hasattr(last_extract_dt, 'strftime'):
            last_extract_str = last_extract_dt.strftime('%Y-%m-%d %H:%M:%S')
        else:
            last_extract_str = str(last_extract_dt)
        date_filter = f"WHERE odp.ppd_complete_time >= '{last_extract_str}'::timestamp"
        print(f"📅 INCREMENTAL MODE: Processing data since last extract ({last_extract_str}) from pmr_production_data")
    elif days > 365:
        # Full table mode - remove date filter
        date_filter = ""
        print("🔄 FULL TABLE MODE: Processing all records from pmr_production_data")
    else:
        # Date-filtered mode (fallback)
        date_filter = f"WHERE odp.ppd_complete_time >= CURRENT_DATE - INTERVAL '{days} days'"
        print(f"📅 DATE FILTERED MODE: Processing last {days} days from pmr_production_data")

    query = f"""
       (
        SELECT
            odp.ppd_key::text AS odpd_key,
            odp.ppd_hei_key::text AS odp_key,
            CASE
                WHEN odp.ppd_hei_code ~ '^[0-9]+$' THEN odp.ppd_hei_code::int
                ELSE NULL
            END AS odp_em_key,
            odp.ppd_hei_name::text AS em_firstname,
            COALESCE(odp.ppd_p_date,odp.ppd_date)::date AS odp_date,
            odp.ppd_p_shift::text AS shift,
            CASE
                WHEN LEFT(odp.ppd_bls_code, 2) = '10' THEN 'line-30'::text
                WHEN LEFT(odp.ppd_bls_code, 2) = '11' THEN 'line-21'::text
                WHEN LEFT(odp.ppd_bls_code, 2) = '12' THEN 'line-32'::text
                WHEN LEFT(odp.ppd_bls_code, 2) = '15' THEN 'line-40'::text
                ELSE odp.ppd_bls_code::text
            END AS source_connection,
            ppd_start_time::timestamp AS odp_first_hanger_time,
            ppd_complete_time::timestamp AS odp_last_hanger_time,
            odp.ppd_poi_code::text AS odpd_oc_key,
            odp.ppd_poi_name::text AS oc_description,
            odp.ppd_psi_key::text AS odpd_st_key,
            odp.ppd_psi_code::text AS st_id,
            odp.ppd_psi_name::text AS st_description,
            pwb.pwb_mixcode::text AS odpd_lot_number,
            odp.ppd_pci_code::text AS odpd_cm_key,
            odp.ppd_pci_name::text AS cm_description,
            odp.ppd_psz_code::text AS odpd_sm_key,
            odp.ppd_psz_name::text AS sm_description,
            odp.ppd_quantity::numeric AS odpd_quantity,
            CASE WHEN odp.ppd_poi_name = 'Loading/Panel Segregation' THEN
                odp.ppd_quantity::numeric ELSE 0
            END AS loading_qty,
            CASE WHEN odp.ppd_poi_name = 'Garment Insert in Poly Bag & Close' THEN
                odp.ppd_quantity::numeric ELSE 0
            END AS unloading_qty,
            odp.ppd_bls_code::text AS odpd_workstation,
            LEFT(odp.ppd_bls_code, 2)::numeric AS odpd_wc_key,
            odp.ppd_bls_name::text AS odp_current_station,
            odp.ppd_total_timeconsuming::float AS odpd_actual_time,
            odp.ppd_standard_time::float AS oc_standard_time,
            odp.ppd_efficiency::float AS efficiency,
            odp.ppd_tvwh::float AS ppd_tvwh
        FROM pmr_production_data AS odp
        LEFT JOIN pm_work_bill pwb on pwb.pwb_key=odp.ppd_pwb_key
        {date_filter}
        ORDER BY
            ppd_complete_time NULLS LAST
        ) t
    """

    # Primary method: Single-connection read with optimized settings from SOURCE database
    try:
        print("Reading from SOURCE database (INA-7A) with optimized fetch size and timeouts...")

        df = spark.read \
            .format("jdbc") \
            .option("url", source_jdbc_url) \
            .option("dbtable", query) \
            .option("user", source_jdbc_properties["user"]) \
            .option("password", source_jdbc_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .option("fetchsize", "5000") \
            .option("queryTimeout", "1200") \
            .option("connectTimeout", "120") \
            .load()

        # Persist and count
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        row_count = df.count()

        print(f"✓ Data loaded successfully with single connection. Row count: {row_count:,}")

    except Exception as primary_error:
        print(f"⚠ Primary read failed: {primary_error}")
        print("Attempting fallback without explicit driver...")

        # Fallback: Let Spark auto-detect the driver from SOURCE database
        try:
            df = spark.read \
                .format("jdbc") \
                .option("url", source_jdbc_url) \
                .option("dbtable", query) \
                .option("user", source_jdbc_properties["user"]) \
                .option("password", source_jdbc_properties["password"]) \
                .option("fetchsize", "5000") \
                .option("queryTimeout", "1200") \
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
        print("Warning: No data found in pmr_production_data table")
        return {
            "success": True,
            "data_loaded": 0,
            "data_filtered": 0,
            "tables_processed": [],
            "message": "No data found to process"
        }

    # Process data using TARGETS configuration
    print("Processing data using TARGETS configuration...")
    tables_processed = []
    try:
        for cfg in TARGETS:
            print(f"Processing target table: {cfg['table']}")


            df_to_process = df



            # Perform upsert for this target (using TARGET connection)
            success = upsert_data_via_spark(
                spark=spark,
                data_df=df_to_process,
                table_name=cfg["table"],
                key_columns=cfg["pk"],
                connection_params=target_connection_params
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
                "records": row_count if row_count is not None else "(count disabled)"
            })

            if row_count is not None:
                print(f"Successfully upserted {row_count} records to {cfg['table']} table")
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

    # Get the maximum ppd_complete_time from processed data for ETL logging
    try:
        max_complete_time = df.selectExpr("max(odp_last_hanger_time) as max_time").first()["max_time"]
        print(f"📅 Maximum odp_last_hanger_time (ppd_complete_time) from processed data: {max_complete_time}")
    except Exception as e:
        print(f"⚠️ Could not get max odp_last_hanger_time: {e}")
        max_complete_time = None

    return {
        "success": True,
        "data_loaded": row_count,
        "data_filtered": row_count,
        "tables_processed": tables_processed,
        "max_ppd_complete_time": max_complete_time,
        "message": "Data transformation completed successfully"
    }


def transform_data_chunked(spark: SparkSession, source_connection_params: dict, target_connection_params: dict, last_extract_dt=None, days: int = 1):
    """Chunked data transformation for large datasets to avoid timeouts"""
    print("🔄 Using CHUNKED processing approach for large datasets...")

    # Use SOURCE connection for reading data
    source_jdbc_properties = get_postgres_jdbc_properties(source_connection_params)
    source_jdbc_url = source_connection_params["jdbc_url"]

    # Determine date range for chunking based on last_extract_dt
    if last_extract_dt is not None:
        # Calculate date difference between CURRENT_DATE and last_extract_dt
        from datetime import datetime, date
        current_date = date.today()
        if hasattr(last_extract_dt, 'date'):
            last_extract_date = last_extract_dt.date()
        else:
            # Assume it's already a date or can be converted
            last_extract_date = last_extract_dt

        # Calculate days difference
        date_diff = (current_date - last_extract_date).days
        print(f"📅 Last extract date: {last_extract_date}, Current date: {current_date}, Difference: {date_diff} days")

        if date_diff > 365:
            # Full table mode - too many days since last extract
            print(f"🔄 FULL TABLE MODE: Date difference ({date_diff} days > 365) - processing all records")
            date_range_query = """
            (
                SELECT
                    MIN(ppd_p_date) as start_date,
                    MAX(ppd_p_date) as end_date,
                    COUNT(*) as total_records
                FROM pmr_production_data
            ) t
            """
            chunk_filter_logic = "full_table"  # Flag for full table processing
        else:
            # Incremental mode - use last_extract_dt for precise filtering
            print(f"📅 INCREMENTAL MODE: Processing data since {last_extract_dt} ({date_diff} days ago)")
            date_range_query = f"""
            (
                SELECT
                    '{last_extract_date}'::date as start_date,
                    CURRENT_DATE as end_date,
                    COUNT(*) as total_records
                FROM pmr_production_data
                WHERE ppd_complete_time >= '{last_extract_dt}'::timestamp
            ) t
            """
            chunk_filter_logic = "incremental"  # Flag for incremental processing
    elif days > 5000:
        print("🔄 FULL TABLE MODE: Processing all records from pmr_production_data using chunks (legacy days > 5000)")
        # Get the full date range from the table
        date_range_query = """
        (
            SELECT
                MIN(ppd_p_date) as start_date,
                MAX(ppd_p_date) as end_date,
                COUNT(*) as total_records
            FROM pmr_production_data
        ) t
        """
        chunk_filter_logic = "full_table"
    else:
        print(f"📅 DATE FILTERED MODE: Processing last {days} days using chunks (legacy fallback)")
        date_range_query = f"""
        (
            SELECT
                (CURRENT_DATE - INTERVAL '{days} days') as start_date,
                CURRENT_DATE as end_date,
                COUNT(*) as total_records
            FROM pmr_production_data
            WHERE ppd_complete_time >= CURRENT_DATE - INTERVAL '{days} days'
        ) t
        """
        chunk_filter_logic = "legacy_days"

    try:
        # Get date range and total count
        range_df = spark.read \
            .format("jdbc") \
            .option("url", source_jdbc_url) \
            .option("dbtable", date_range_query) \
            .option("user", source_jdbc_properties["user"]) \
            .option("password", source_jdbc_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .load()

        range_data = range_df.first()
        if not range_data:
            print("No data found in date range")
            return {
                "success": True,
                "data_loaded": 0,
                "data_filtered": 0,
                "tables_processed": [],
                "message": "No data found to process"
            }

        start_date = range_data["start_date"]
        end_date = range_data["end_date"]
        total_records = range_data["total_records"]

        print(f"📊 Total records to process: {total_records:,}")
        print(f"📅 Date range: {start_date} to {end_date}")
        print(f"📅 Date types: start_date={type(start_date)}, end_date={type(end_date)}")

        # Force conversion to date objects to avoid datetime/date comparison issues
        from datetime import date
        try:
            # Convert to date objects regardless of input type
            if hasattr(start_date, 'date'):
                start_date = start_date.date()
            else:
                start_date = date.fromisoformat(str(start_date).split(' ')[0])

            if hasattr(end_date, 'date'):
                end_date = end_date.date()
            else:
                end_date = date.fromisoformat(str(end_date).split(' ')[0])

            print(f"📅 Converted date types: start_date={type(start_date)}, end_date={type(end_date)}")
        except Exception as date_error:
            print(f"⚠️ Date conversion error: {date_error}")
            # Fallback: try to convert using different methods
            try:
                start_date = date.fromisoformat(str(start_date).split('T')[0].split(' ')[0])
                end_date = date.fromisoformat(str(end_date).split('T')[0].split(' ')[0])
                print(f"📅 Fallback converted date types: start_date={type(start_date)}, end_date={type(end_date)}")
            except Exception as fallback_error:
                print(f"✗ Date conversion failed: {fallback_error}")
                raise

        # Define chunk size (process ~5,000 records per chunk to avoid timeouts)
        chunk_size_days = 1  # Process 1 days at a time
        current_date = start_date
        total_processed = 0
        chunk_number = 1
        max_complete_time_overall = None

        # Process data in chunks
        while current_date <= end_date:
            chunk_end_date = min(current_date + timedelta(days=chunk_size_days), end_date)

            print(f"🔄 Processing chunk {chunk_number}: {current_date} to {chunk_end_date}")

            # Build chunk query based on processing mode
            if chunk_filter_logic == "full_table":
                # Full table mode - no WHERE clause, process all data in date range
                chunk_where_clause = f"WHERE odp.ppd_complete_time >= '{current_date}' AND odp.ppd_complete_time < '{chunk_end_date + timedelta(days=1)}'"
                print(f"  🔄 Full table chunk: Processing all records in date range")
            elif chunk_filter_logic == "incremental":
                # Incremental mode - use precise timestamp filtering
                chunk_where_clause = f"WHERE odp.ppd_complete_time >= '{last_extract_dt}'::timestamp AND odp.ppd_complete_time >= '{current_date}' AND odp.ppd_complete_time < '{chunk_end_date + timedelta(days=1)}'"
                print(f"  📅 Incremental chunk: Processing since {chunk_where_clause}")
            else:
                # Legacy mode fallback
                chunk_where_clause = f"WHERE odp.ppd_complete_time >= '{current_date}' AND odp.ppd_complete_time < '{chunk_end_date + timedelta(days=1)}'"
                print(f"  📅 Legacy chunk: Processing date range")

            chunk_query = f"""
            (
                SELECT
                    odp.ppd_key::text AS odpd_key,
                    odp.ppd_hei_key::text AS odp_key,
                    CASE
                        WHEN odp.ppd_hei_code ~ '^[0-9]+$' THEN odp.ppd_hei_code::int
                        ELSE NULL
                    END AS odp_em_key,
                    odp.ppd_hei_name::text AS em_firstname,
                    COALESCE(odp.ppd_p_date,odp.ppd_date)::date AS odp_date,
                    odp.ppd_p_shift::text AS shift,
                    CASE
                        WHEN LEFT(odp.ppd_bls_code, 2) = '10' THEN 'line-30'::text
                        WHEN LEFT(odp.ppd_bls_code, 2) = '11' THEN 'line-21'::text
                        WHEN LEFT(odp.ppd_bls_code, 2) = '12' THEN 'line-32'::text
                        WHEN LEFT(odp.ppd_bls_code, 2) = '15' THEN 'line-40'::text
                        ELSE odp.ppd_bls_code::text
                    END AS source_connection,
                    ppd_start_time::timestamp AS odp_first_hanger_time,
                    ppd_complete_time::timestamp AS odp_last_hanger_time,
                    odp.ppd_poi_code::text AS odpd_oc_key,
                    odp.ppd_poi_name::text AS oc_description,
                    odp.ppd_psi_key::text AS odpd_st_key,
                    odp.ppd_psi_code::text AS st_id,
                    odp.ppd_psi_name::text AS st_description,
                    pwb.pwb_mixcode::text AS odpd_lot_number,
                    pwb.pwb_code::text AS fg_item_key,
                    odp.ppd_pci_code::text AS odpd_cm_key,
                    odp.ppd_pci_name::text AS cm_description,
                    odp.ppd_psz_code::text AS odpd_sm_key,
                    odp.ppd_psz_name::text AS sm_description,
                    odp.ppd_quantity::numeric AS odpd_quantity,
                    CASE WHEN odp.ppd_poi_name = 'Loading/Panel Segregation' THEN
                        odp.ppd_quantity::numeric ELSE 0
                    END AS loading_qty,
                    CASE WHEN odp.ppd_poi_name = 'Garment Insert in Poly Bag & Close' THEN
                        odp.ppd_quantity::numeric ELSE 0
                    END AS unloading_qty,
                    odp.ppd_bls_code::text AS odpd_workstation,
                    LEFT(odp.ppd_bls_code, 2)::numeric AS odpd_wc_key,
                    odp.ppd_bls_name::text AS odp_current_station,
                    odp.ppd_total_timeconsuming::float AS odpd_actual_time,
                    odp.ppd_standard_time::float AS oc_standard_time,
                    odp.ppd_efficiency::float AS efficiency,
                    odp.ppd_tvwh::float AS ppd_tvwh
                FROM pmr_production_data AS odp
                LEFT JOIN pm_work_bill pwb on pwb.pwb_key=odp.ppd_pwb_key
                {chunk_where_clause}
                ORDER BY ppd_complete_time NULLS LAST
            ) t
            """

            try:
                # Read chunk data
                chunk_df = spark.read \
                    .format("jdbc") \
                    .option("url", source_jdbc_url) \
                    .option("dbtable", chunk_query) \
                    .option("user", source_jdbc_properties["user"]) \
                    .option("password", source_jdbc_properties["password"]) \
                    .option("driver", "org.postgresql.Driver") \
                    .option("fetchsize", "5000") \
                    .option("queryTimeout", "900") \
                    .option("connectTimeout", "60") \
                    .load()

                chunk_df = chunk_df.persist(StorageLevel.MEMORY_AND_DISK)
                chunk_count = chunk_df.count()

                if chunk_count > 0:
                    print(f"✓ Chunk {chunk_number}: Loaded {chunk_count:,} records")

                    # Track max ppd_complete_time from this chunk
                    try:
                        chunk_max_time = chunk_df.selectExpr("max(odp_last_hanger_time) as max_time").first()["max_time"]
                        if chunk_max_time:
                            if max_complete_time_overall is None or chunk_max_time > max_complete_time_overall:
                                max_complete_time_overall = chunk_max_time
                        print(f"📅 Chunk {chunk_number} max odp_last_hanger_time (ppd_complete_time): {chunk_max_time}")
                    except Exception as time_error:
                        print(f"⚠️ Could not get max time for chunk {chunk_number}: {time_error}")

                    # Process chunk data using TARGETS configuration
                    for cfg in TARGETS:
                        print(f"  → Upserting to {cfg['table']}...")

                        success = upsert_data_via_spark(
                            spark=spark,
                            data_df=chunk_df,
                            table_name=cfg["table"],
                            key_columns=cfg["pk"],
                            connection_params=target_connection_params
                        )

                        if not success:
                            print(f"  ✗ Failed to upsert chunk {chunk_number} to {cfg['table']}")
                            return {
                                "success": False,
                                "data_loaded": total_processed,
                                "data_filtered": total_processed,
                                "tables_processed": [],
                                "message": f"Failed to upsert chunk {chunk_number} to {cfg['table']}"
                            }
                        else:
                            print(f"  ✓ Successfully upserted chunk {chunk_number} ({chunk_count:,} records) to {cfg['table']}")

                    total_processed += chunk_count
                    print(f"📊 Progress: {total_processed:,} / {total_records:,} records processed ({total_processed/total_records*100:.1f}%)")
                else:
                    print(f"⚠ Chunk {chunk_number}: No records found")

                # Clean up chunk DataFrame
                chunk_df.unpersist(blocking=False)

            except Exception as chunk_error:
                print(f"✗ Error processing chunk {chunk_number}: {chunk_error}")
                import traceback
                traceback.print_exc()
                return {
                    "success": False,
                    "data_loaded": total_processed,
                    "data_filtered": total_processed,
                    "tables_processed": [],
                    "message": f"Failed to process chunk {chunk_number}: {str(chunk_error)}"
                }

            # Move to next chunk (ensure result is a date object)
            next_date = chunk_end_date + timedelta(days=1)
            current_date = next_date.date() if isinstance(next_date, datetime) else next_date
            chunk_number += 1

        # Return success with final counts
        tables_processed = [{"table": cfg["table"], "records": total_processed} for cfg in TARGETS]

        return {
            "success": True,
            "data_loaded": total_processed,
            "data_filtered": total_processed,
            "tables_processed": tables_processed,
            "max_ppd_complete_time": max_complete_time_overall,
            "message": f"Chunked data transformation completed successfully - processed {total_processed:,} records in {chunk_number-1} chunks"
        }

    except Exception as e:
        print(f"✗ Error in chunked processing: {str(e)}")
        import traceback
        traceback.print_exc()
        return {
            "success": False,
            "data_loaded": 0,
            "data_filtered": 0,
            "tables_processed": [],
            "message": f"Chunked processing failed: {str(e)}"
        }


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
        success = transform_data(spark, days=1)
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
