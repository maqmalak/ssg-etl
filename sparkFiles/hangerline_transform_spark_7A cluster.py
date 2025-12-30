"""
PySpark ETL Script for Hanger Line Data Processing
Cluster Mode: 1 Master + 2 Workers
Phases: source_check, target_check, load_data, transform_data
"""

import sys
import os
import json
import logging
import argparse
import time
from datetime import datetime, timedelta
from typing import Dict, Any, Optional

import psycopg2
from airflow.hooks.base import BaseHook
from pendulum import timezone

# Configure timezone
PKT = timezone("Asia/Karachi")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Target configuration
TARGETS = [
    {
        "table": "operator_daily_performance",
        "pk": ["source_connection", "odp_key", "odpd_key"]
    },
]

# Add dags directory to path
dags_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'dags')
sys.path.append(os.path.abspath(dags_path))

try:
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import sum as spark_sum, first, lit, current_date, date_sub
    from pyspark import StorageLevel
    logger.info("✓ PySpark modules imported successfully")
except ImportError as e:
    logger.error(f"✗ Error importing PySpark modules: {e}")
    sys.exit(1)

# Define JDBC properties function directly (no need to import from dags)
def get_postgres_jdbc_properties(connection_params: Dict[str, Any]) -> Dict[str, str]:
    """Get JDBC properties dictionary from connection parameters"""
    return {
        "user": connection_params.get("user"),
        "password": connection_params.get("password"),
        "driver": "org.postgresql.Driver"
    }

logger.info("✓ Required functions defined successfully")


# ==================== CONNECTION MANAGEMENT ====================

def get_postgres_source_connection() -> Dict[str, Any]:
    """
    Get connection parameters for INA-7A source database.
    Returns dictionary with connection details for JDBC and direct connections.
    """
    try:
        connection = BaseHook.get_connection("INA-7A")
        host = connection.host
        port = connection.port if connection.port else 5433
        database = connection.schema
        user = connection.login
        password = connection.password
        
        logger.info(f"Using Airflow connection 'INA-7A': {database} on {host}:{port}")
    except Exception as e:
        logger.warning(f"Could not get Airflow connection 'INA-7A', using environment variables: {e}")
        host = os.getenv("INA_7A_HOST", "localhost")
        port = int(os.getenv("INA_7A_PORT", "5433"))
        database = os.getenv("INA_7A_DATABASE", "postgres")
        user = os.getenv("INA_7A_USER", "postgres")
        password = os.getenv("INA_7A_PASSWORD", "")
    
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}"
    }


def get_target_postgres_connection() -> Dict[str, Any]:
    """
    Get connection parameters for pg-ssg target database.
    Returns dictionary with connection details for JDBC and direct connections.
    """
    try:
        connection = BaseHook.get_connection("pg-ssg")
        host = connection.host
        port = connection.port if connection.port else 5432
        database = connection.schema
        user = connection.login
        password = connection.password
        
        logger.info(f"Using Airflow connection 'pg-ssg': {database} on {host}:{port}")
    except Exception as e:
        logger.warning(f"Could not get Airflow connection 'pg-ssg', using environment variables: {e}")
        host = os.getenv("TARGET_PG_HOST", "172.16.7.6")
        port = int(os.getenv("TARGET_PG_PORT", "5432"))
        database = os.getenv("TARGET_PG_DATABASE", "ssg")
        user = os.getenv("TARGET_PG_USER", "postgres")
        password = os.getenv("TARGET_PG_PASSWORD", "")
    
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}"
    }


# ==================== SPARK SESSION MANAGEMENT ====================

def create_spark_session(app_name: str = "HangerLine-ETL") -> SparkSession:
    """
    Create and configure Spark session optimized for cluster mode.
    Supports both local and cluster deployment.
    """
    logger.info("Creating Spark session...")
    
    try:
        # Get configuration from environment
        spark_master = os.getenv("SPARK_MASTER_URL", "spark://spark-master:7077")
        spark_ui_port = os.getenv("SPARK_UI_PORT", "4040")
        spark_ui_bind = os.getenv("SPARK_UI_BIND", "0.0.0.0")
        
        logger.info(f"Spark Master URL: {spark_master}")
        
        # Locate JDBC driver
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
                logger.info(f"✓ Found PostgreSQL JDBC driver at {jdbc_driver_path}")
                break
        
        if not jdbc_driver_path:
            logger.warning("⚠ PostgreSQL JDBC driver not found at expected locations")
        
        # Build Spark session with cluster-optimized configuration
        builder = (
            SparkSession.builder
            .appName(app_name)
            .master(spark_master)
            
            # Resource allocation for cluster mode (1 master + 2 workers)
            .config("spark.executor.instances", "2")
            .config("spark.executor.cores", "3")
            .config("spark.executor.memory", "8g")
            .config("spark.driver.memory", "4g")
            .config("spark.driver.cores", "2")
            
            # Memory management
            .config("spark.memory.fraction", "0.8")
            .config("spark.memory.storageFraction", "0.3")
            .config("spark.executor.memoryOverhead", "2g")
            .config("spark.driver.memoryOverhead", "1g")
            
            # Adaptive Query Execution
            .config("spark.sql.adaptive.enabled", "true")
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
            .config("spark.sql.adaptive.skewJoin.enabled", "true")
            .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128MB")
            
            # Shuffle optimization
            .config("spark.sql.shuffle.partitions", "32")
            .config("spark.shuffle.compress", "true")
            .config("spark.shuffle.spill.compress", "true")
            
            # Serialization
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
            .config("spark.kryoserializer.buffer.max", "512m")
            
            # Network and timeout settings (critical for cluster mode)
            .config("spark.network.timeout", "600s")
            .config("spark.executor.heartbeatInterval", "60s")
            .config("spark.rpc.askTimeout", "600s")
            .config("spark.rpc.lookupTimeout", "120s")
            
            # Dynamic allocation
            .config("spark.dynamicAllocation.enabled", "false")
            
            # UI configuration
            .config("spark.ui.port", spark_ui_port)
            .config("spark.ui.enabled", "true")
            .config("spark.driver.bindAddress", spark_ui_bind)
            .config("spark.driver.host", spark_ui_bind)
        )
        
        # Configure JDBC driver classpath
        if jdbc_driver_path and os.path.exists(jdbc_driver_path):
            logger.info(f"✓ Using PostgreSQL JDBC driver: {jdbc_driver_path}")
            builder = (builder
                .config("spark.jars", jdbc_driver_path)
                .config("spark.driver.extraClassPath", f"{jdbc_driver_path}:/opt/spark/work/jdbc-drivers/*")
                .config("spark.executor.extraClassPath", f"{jdbc_driver_path}:/opt/spark/work/jdbc-drivers/*")
            )
        else:
            logger.info("Using wildcard classpath for JDBC drivers")
            builder = (builder
                .config("spark.driver.extraClassPath", "/opt/spark/work/jdbc-drivers/*:/opt/airflow/sparkFiles/jdbc-drivers/*")
                .config("spark.executor.extraClassPath", "/opt/spark/work/jdbc-drivers/*:/opt/airflow/sparkFiles/jdbc-drivers/*")
            )
        
        spark = builder.getOrCreate()
        
        # Print configuration
        logger.info("=" * 80)
        logger.info("Spark Session Configuration:")
        logger.info(f"  Master: {spark.sparkContext.master}")
        logger.info(f"  App Name: {spark.sparkContext.appName}")
        logger.info(f"  Spark Version: {spark.version}")
        logger.info(f"  Driver Memory: {spark.sparkContext.getConf().get('spark.driver.memory', 'default')}")
        
        if spark.sparkContext.master.startswith("local"):
            logger.info(f"  Mode: LOCAL (single JVM)")
            logger.info(f"  Threads: {spark.sparkContext.defaultParallelism}")
        else:
            logger.info(f"  Mode: CLUSTER")
            logger.info(f"  Executor Memory: {spark.sparkContext.getConf().get('spark.executor.memory', 'default')}")
            logger.info(f"  Executor Cores: {spark.sparkContext.getConf().get('spark.executor.cores', 'default')}")
            logger.info(f"  Executor Instances: {spark.sparkContext.getConf().get('spark.executor.instances', 'dynamic')}")
        logger.info("=" * 80)
        
        logger.info("✓ Spark session created successfully")
        return spark
        
    except Exception as e:
        logger.error(f"✗ Error creating Spark session: {e}")
        import traceback
        traceback.print_exc()
        raise


# ==================== PHASE 1: SOURCE CHECK ====================

def phase_source_check(spark: SparkSession, lookback_days: int = 30) -> Dict[str, Any]:
    """
    Phase 1: Check source database for recent data availability.
    
    Args:
        spark: SparkSession instance
        lookback_days: Number of days to look back for data
    
    Returns:
        Dict with metrics about source data availability
    """
    logger.info("=" * 80)
    logger.info("PHASE 1: SOURCE CHECK")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    try:
        # Get source connection
        source_conn = get_postgres_source_connection()
        logger.info(f"Checking source: {source_conn['database']} on {source_conn['host']}:{source_conn['port']}")
        
        # Build query to check for recent data
        query = f"""
        (
            SELECT 
                COUNT(*) as record_count,
                MIN(ppd_date) as earliest_date,
                MAX(ppd_date) as latest_date,
                COUNT(DISTINCT ppd_date) as distinct_dates
            FROM pmr_production_data 
            WHERE ppd_date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
        ) t
        """
        
        logger.info(f"Checking for data in last {lookback_days} days...")
        
        # Read data using Spark JDBC
        count_df = spark.read \
            .format("jdbc") \
            .option("url", source_conn["jdbc_url"]) \
            .option("dbtable", query) \
            .option("user", source_conn["user"]) \
            .option("password", source_conn["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .option("fetchsize", "1000") \
            .load()
        
        # Get results
        result = count_df.first()
        record_count = result["record_count"]
        earliest_date = str(result["earliest_date"]) if result["earliest_date"] else None
        latest_date = str(result["latest_date"]) if result["latest_date"] else None
        distinct_dates = result["distinct_dates"]
        
        duration = time.time() - start_time
        
        logger.info(f"✓ Source check completed:")
        logger.info(f"  Records found: {record_count:,}")
        logger.info(f"  Date range: {earliest_date} to {latest_date}")
        logger.info(f"  Distinct dates: {distinct_dates}")
        logger.info(f"  Duration: {duration:.2f}s")
        
        return {
            "success": True,
            "task": "source_check",
            "duration_sec": duration,
            "record_count": record_count,
            "earliest_date": earliest_date,
            "latest_date": latest_date,
            "distinct_dates": distinct_dates,
            "lookback_days": lookback_days,
            "source_database": source_conn["database"],
            "source_host": source_conn["host"],
        }
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"✗ Source check failed: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "success": False,
            "task": "source_check",
            "duration_sec": duration,
            "error": str(e)
        }


# ==================== PHASE 2: TARGET CHECK ====================

def phase_target_check(spark: SparkSession, lookback_days: int = 30) -> Dict[str, Any]:
    """
    Phase 2: Check target database and table structure.
    
    Args:
        spark: SparkSession instance
        lookback_days: Number of days to look back for data
    
    Returns:
        Dict with metrics about target database status
    """
    logger.info("=" * 80)
    logger.info("PHASE 2: TARGET CHECK")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    try:
        # Get target connection
        target_conn = get_target_postgres_connection()
        logger.info(f"Checking target: {target_conn['database']} on {target_conn['host']}:{target_conn['port']}")
        
        tables_info = []
        
        for cfg in TARGETS:
            table_name = cfg["table"]
            logger.info(f"Checking table: {table_name}")
            
            try:
                # Check if table exists and get count
                query = f"""
                (
                    SELECT 
                        COUNT(*) as record_count,
                        MIN(odp_date) as earliest_date,
                        MAX(odp_date) as latest_date
                    FROM {table_name}
                    WHERE odp_date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
                ) t
                """
                
                count_df = spark.read \
                    .format("jdbc") \
                    .option("url", target_conn["jdbc_url"]) \
                    .option("dbtable", query) \
                    .option("user", target_conn["user"]) \
                    .option("password", target_conn["password"]) \
                    .option("driver", "org.postgresql.Driver") \
                    .load()
                
                result = count_df.first()
                record_count = result["record_count"]
                earliest_date = str(result["earliest_date"]) if result["earliest_date"] else None
                latest_date = str(result["latest_date"]) if result["latest_date"] else None
                
                logger.info(f"✓ Table {table_name}: {record_count:,} records")
                
                tables_info.append({
                    "name": table_name,
                    "exists": True,
                    "count": record_count,
                    "earliest_date": earliest_date,
                    "latest_date": latest_date,
                    "primary_keys": cfg["pk"]
                })
                
            except Exception as table_error:
                logger.warning(f"⚠ Error checking table {table_name}: {table_error}")
                tables_info.append({
                    "name": table_name,
                    "exists": False,
                    "error": str(table_error)
                })
        
        duration = time.time() - start_time
        
        logger.info(f"✓ Target check completed in {duration:.2f}s")
        
        return {
            "success": True,
            "task": "target_check",
            "duration_sec": duration,
            "tables": tables_info,
            "target_database": target_conn["database"],
            "target_host": target_conn["host"],
        }
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"✗ Target check failed: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "success": False,
            "task": "target_check",
            "duration_sec": duration,
            "error": str(e)
        }


# ==================== PHASE 3: LOAD DATA ====================

def phase_load_data(spark: SparkSession, lookback_days: int = 30) -> Dict[str, Any]:
    """
    Phase 3: Load data from source database using Spark.
    
    Args:
        spark: SparkSession instance
        lookback_days: Number of days to look back for data
    
    Returns:
        Dict with metrics about data loading
    """
    logger.info("=" * 80)
    logger.info("PHASE 3: LOAD DATA")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    try:
        # Get source connection
        source_conn = get_postgres_source_connection()
        source_jdbc_properties = get_postgres_jdbc_properties(source_conn)
        source_jdbc_url = source_conn["jdbc_url"]
        
        logger.info(f"Loading data from: {source_conn['database']}")
        
        # SQL query to extract data
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
                COALESCE(odp.ppd_p_date, odp.ppd_date)::date AS odp_date,
                odp.ppd_p_shift::text AS shift,
                CASE
                    WHEN LEFT(odp.ppd_bls_code, 2) = '10' THEN 'line-30'::text
                    WHEN LEFT(odp.ppd_bls_code, 2) = '11' THEN 'line-21'::text
                    WHEN LEFT(odp.ppd_bls_code, 2) = '12' THEN 'line-32'::text
                    ELSE odp.ppd_bls_code::text
                END AS source_connection,
                ppd_start_time::timestamp AS odp_first_hanger_time,
                ppd_complete_time::timestamp AS odp_last_hanger_time,
                odp.ppd_poi_code::text AS odpd_oc_key,
                odp.ppd_poi_name::text AS oc_description,
                odp.ppd_psi_key::text AS odpd_st_key,
                odp.ppd_psi_code::text AS st_id,
                odp.ppd_psi_name::text AS st_description,
                odp.ppd_pci_code::text AS odpd_cm_key,
                odp.ppd_pci_name::text AS cm_description,
                odp.ppd_psz_code::text AS odpd_sm_key,
                odp.ppd_psz_name::text AS sm_description,
                odp.ppd_quantity::numeric AS odpd_quantity,
                CASE WHEN odp.ppd_poi_name = 'Loading/Panel Segregation' THEN 
                    odp.ppd_quantity::numeric ELSE 0 END AS loading_qty,
                CASE WHEN odp.ppd_poi_name = 'Garment Insert in Poly Bag & Close' THEN 
                    odp.ppd_quantity::numeric ELSE 0 END AS unloading_qty,
                odp.ppd_pwb_code::text AS fg_item_key,
                odp.ppd_bls_code::text AS odpd_workstation,
                LEFT(odp.ppd_bls_code, 2)::numeric AS odpd_wc_key,
                odp.ppd_bls_name::text AS odp_current_station,
                odp.ppd_total_timeconsuming::float AS odpd_actual_time,
                odp.ppd_standard_time::float AS oc_standard_time,
                odp.ppd_efficiency::float AS efficiency,
                odp.ppd_tvwh::float AS ppd_tvwh
            FROM pmr_production_data AS odp
            WHERE odp.ppd_date >= CURRENT_DATE - INTERVAL '{lookback_days} days'
            ORDER BY ppd_complete_time NULLS LAST
        ) t
        """
        
        logger.info(f"Reading data from source (last {lookback_days} days)...")
        
        # Load data with optimized settings for cluster mode
        df = spark.read \
            .format("jdbc") \
            .option("url", source_jdbc_url) \
            .option("dbtable", query) \
            .option("user", source_jdbc_properties["user"]) \
            .option("password", source_jdbc_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .option("fetchsize", "10000") \
            .option("queryTimeout", "600") \
            .load()
        
        # Persist for reuse
        df = df.persist(StorageLevel.MEMORY_AND_DISK)
        row_count = df.count()
        
        # Adjust shuffle partitions based on data size
        if row_count > 0:
            optimal_partitions = max(32, min(200, row_count // 10000))
            spark.conf.set("spark.sql.shuffle.partitions", str(optimal_partitions))
            logger.info(f"✓ Adjusted shuffle partitions to {optimal_partitions}")
        
        duration = time.time() - start_time
        
        logger.info(f"✓ Data loaded successfully:")
        logger.info(f"  Records: {row_count:,}")
        logger.info(f"  Columns: {len(df.columns)}")
        logger.info(f"  Duration: {duration:.2f}s")
        logger.info(f"  Throughput: {row_count/duration if duration > 0 else 0:.0f} records/sec")
        
        # Store DataFrame in globals for next phase
        globals()['loaded_df'] = df
        
        return {
            "success": True,
            "task": "load_data",
            "duration_sec": duration,
            "record_count": row_count,
            "column_count": len(df.columns),
            "throughput_rps": row_count / duration if duration > 0 else 0,
            "lookback_days": lookback_days,
        }
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"✗ Data loading failed: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "success": False,
            "task": "load_data",
            "duration_sec": duration,
            "error": str(e)
        }


# ==================== PHASE 4: TRANSFORM AND UPSERT DATA ====================

def phase_transform_data(spark: SparkSession, lookback_days: int = 30) -> Dict[str, Any]:
    """
    Phase 4: Transform and upsert data to target database.
    
    Args:
        spark: SparkSession instance
        lookback_days: Number of days (for metrics only)
    
    Returns:
        Dict with metrics about transformation and upsert
    """
    logger.info("=" * 80)
    logger.info("PHASE 4: TRANSFORM DATA")
    logger.info("=" * 80)
    
    start_time = time.time()
    
    try:
        # Get loaded DataFrame from previous phase
        if 'loaded_df' not in globals():
            raise ValueError("No data loaded. Run load_data phase first.")
        
        df = globals()['loaded_df']
        
        # Get target connection
        target_conn = get_target_postgres_connection()
        logger.info(f"Upserting to target: {target_conn['database']}")
        
        tables_processed = []
        
        for cfg in TARGETS:
            table_name = cfg["table"]
            pk_columns = cfg["pk"]
            
            logger.info(f"Processing table: {table_name}")
            table_start = time.time()
            
            # Perform upsert
            success = upsert_data_via_spark(
                spark=spark,
                data_df=df,
                table_name=table_name,
                key_columns=pk_columns,
                connection_params=target_conn
            )
            
            table_duration = time.time() - table_start
            
            if not success:
                logger.error(f"✗ Failed to upsert data to {table_name}")
                return {
                    "success": False,
                    "task": "transform_data",
                    "duration_sec": time.time() - start_time,
                    "error": f"Failed to upsert to {table_name}"
                }
            
            record_count = df.count()
            
            logger.info(f"✓ Table {table_name} processed:")
            logger.info(f"  Records: {record_count:,}")
            logger.info(f"  Duration: {table_duration:.2f}s")
            
            tables_processed.append({
                "name": table_name,
                "count": record_count,
                "duration_sec": table_duration,
                "throughput_rps": record_count / table_duration if table_duration > 0 else 0
            })
        
        # Unpersist DataFrame
        df.unpersist()
        if 'loaded_df' in globals():
            del globals()['loaded_df']
        
        duration = time.time() - start_time
        
        logger.info(f"✓ Transform and upsert completed in {duration:.2f}s")
        
        return {
            "success": True,
            "task": "transform_data",
            "duration_sec": duration,
            "tables": tables_processed,
            "target_database": target_conn["database"],
        }
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error(f"✗ Transform failed: {e}")
        import traceback
        traceback.print_exc()
        
        return {
            "success": False,
            "task": "transform_data",
            "duration_sec": duration,
            "error": str(e)
        }


# ==================== UPSERT UTILITY ====================

def upsert_data_via_spark(
    spark: SparkSession,
    data_df,
    table_name: str,
    key_columns: list,
    connection_params: dict
) -> bool:
    """
    Perform upsert operation using staging table approach.
    Optimized for cluster mode with batching.
    
    Args:
        spark: SparkSession instance
        data_df: DataFrame with data to upsert
        table_name: Target table name
        key_columns: List of primary key columns
        connection_params: Database connection parameters
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        if data_df is None or data_df.rdd.isEmpty():
            logger.warning("No data to upsert")
            return True
        
        import uuid
        
        jdbc_properties = get_postgres_jdbc_properties(connection_params)
        jdbc_url = connection_params["jdbc_url"]
        
        # Generate unique staging table name
        staging_table = f"{table_name}_staging_{str(uuid.uuid4()).replace('-', '_')}"
        
        logger.info(f"Creating staging table: {staging_table}")
        
        try:
            # Connect to PostgreSQL
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
            
            # Create staging table
            cursor.execute(f"CREATE TABLE {staging_table} (LIKE {table_name} INCLUDING ALL);")
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"Writing data to staging table...")
            
            # Write data to staging table with optimized settings for cluster
            data_df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", staging_table) \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .option("driver", "org.postgresql.Driver") \
                .option("batchsize", "5000") \
                .option("numPartitions", "8") \
                .mode("append") \
                .save()
            
            logger.info(f"Performing upsert from staging to {table_name}...")
            
            # Perform upsert
            conn = psycopg2.connect(
                host=connection_params.get("host"),
                port=connection_params.get("port", "5432"),
                database=connection_params.get("database"),
                user=connection_params.get("user"),
                password=connection_params.get("password")
            )
            cursor = conn.cursor()
            
            # Get column names
            columns = data_df.columns
            all_columns_str = ", ".join(columns)
            
            # Create SET clause (excluding key columns)
            set_columns = [col for col in columns if col not in key_columns]
            set_clause = ", ".join([f"{col} = EXCLUDED.{col}" for col in set_columns])
            
            # Create key columns string
            key_columns_str = ", ".join(key_columns)
            
            # UPSERT SQL
            upsert_sql = f"""
            INSERT INTO {table_name} ({all_columns_str})
            SELECT {all_columns_str} FROM {staging_table}
            ON CONFLICT ({key_columns_str})
            DO UPDATE SET {set_clause};
            """
            
            cursor.execute(upsert_sql)
            upserted_count = cursor.rowcount
            conn.commit()
            
            logger.info(f"✓ Upserted {upserted_count:,} records")
            
            # Clean up staging table
            cursor.execute(f"DROP TABLE {staging_table};")
            conn.commit()
            cursor.close()
            conn.close()
            
            logger.info(f"✓ Staging table cleaned up")
            
            return True
            
        except Exception as e:
            logger.error(f"✗ Error during upsert: {e}")
            # Try to clean up staging table
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
        logger.error(f"✗ Upsert failed: {e}")
        import traceback
        traceback.print_exc()
        return False


# ==================== METRICS WRITER ====================

def write_metrics(metrics: Dict[str, Any], metrics_path: str):
    """Write metrics to JSON file"""
    try:
        # Ensure directory exists
        os.makedirs(os.path.dirname(metrics_path), exist_ok=True)
        
        with open(metrics_path, 'w', encoding='utf-8') as f:
            json.dump(metrics, f, indent=2)
        
        logger.info(f"✓ Metrics written to: {metrics_path}")
        
    except Exception as e:
        logger.error(f"✗ Error writing metrics: {e}")


# ==================== MAIN ====================

def main():
    """Main execution function"""
    parser = argparse.ArgumentParser(description='Hanger Line ETL Process')
    parser.add_argument('--phase', required=True, 
                       choices=['source_check', 'target_check', 'load_data', 'transform_data'],
                       help='ETL phase to execute')
    parser.add_argument('--lookback-days', type=int, default=30,
                       help='Number of days to look back for data')
    parser.add_argument('--metrics-path', required=True,
                       help='Path to write metrics JSON file')
    
    args = parser.parse_args()
    
    logger.info("=" * 80)
    logger.info(f"Starting ETL Phase: {args.phase}")
    logger.info(f"Lookback Days: {args.lookback_days}")
    logger.info(f"Metrics Path: {args.metrics_path}")
    logger.info("=" * 80)
    
    spark = None
    metrics = {}
    
    try:
        # Create Spark session
        spark = create_spark_session(app_name=f"HangerLine-ETL-{args.phase}")
        
        # Execute the requested phase
        if args.phase == 'source_check':
            metrics = phase_source_check(spark, args.lookback_days)
            
        elif args.phase == 'target_check':
            metrics = phase_target_check(spark, args.lookback_days)
            
        elif args.phase == 'load_data':
            metrics = phase_load_data(spark, args.lookback_days)
            
        elif args.phase == 'transform_data':
            metrics = phase_transform_data(spark, args.lookback_days)
        
        # Write metrics to file
        write_metrics(metrics, args.metrics_path)
        
        # Exit with appropriate code
        if metrics.get('success', False):
            logger.info(f"✓ Phase {args.phase} completed successfully")
            sys.exit(0)
        else:
            logger.error(f"✗ Phase {args.phase} failed")
            sys.exit(1)
            
    except Exception as e:
        logger.error(f"✗ Fatal error in phase {args.phase}: {e}")
        import traceback
        traceback.print_exc()
        
        # Write error metrics
        metrics = {
            "success": False,
            "task": args.phase,
            "error": str(e),
            "traceback": traceback.format_exc()
        }
        write_metrics(metrics, args.metrics_path)
        
        sys.exit(1)
        
    finally:
        # Clean up Spark session
        if spark is not None:
            try:
                spark.stop()
                logger.info("Spark session stopped")
            except Exception as e:
                logger.warning(f"Error stopping Spark session: {e}")


if __name__ == "__main__":
    main()
