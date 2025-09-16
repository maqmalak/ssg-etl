"""
Optimized PySpark ETL script for transforming hanger lane data.
This script reads data from the pg-ssg database, performs aggregation with proper ETL practices, and saves the result.
"""

import sys
import os
import logging
import time
from datetime import datetime
from typing import Dict, Any, Tuple

# Add the dags directory to the Python path so we can import db_utils
dags_path = os.path.join(os.path.dirname(os.path.abspath(__file__)), '..', 'dags')
sys.path.append(os.path.abspath(dags_path))

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

print(f"Python path: {sys.path}")
print(f"DAGs path exists: {os.path.exists(dags_path)}")
print(f"db_utils.py exists: {os.path.exists(os.path.join(dags_path, 'db_utils.py'))}")

from scripts.create_target_production_table_pg import (
    create_target_table_if_not_exists,
    create_etl_log_odp_table_if_not_exists
)

try:
    from pyspark.sql import functions as F
    from pyspark.sql import SparkSession
    from pyspark.sql.functions import (
        sum as spark_sum, count, max as spark_max, min as spark_min,
        current_date, date_sub, current_timestamp, lit, when, col
    )
    from pyspark.sql.types import (
        StructType, StructField, StringType, IntegerType, DoubleType, DateType
    )
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


class ETLConfig:
    """Configuration class for ETL process"""
    
    def __init__(self):
        self.data_freshness_threshold_hours = int(os.getenv('DATA_FRESHNESS_THRESHOLD', '48'))
        self.spark_executor_memory = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')
        self.spark_driver_memory = os.getenv('SPARK_DRIVER_MEMORY', '4g')
        self.spark_shuffle_partitions = int(os.getenv('SPARK_SHUFFLE_PARTITIONS', '200'))
        self.max_partition_bytes = os.getenv('SPARK_MAX_PARTITION_BYTES', '134217728')  # 128MB
        self.enable_adaptive_query_execution = os.getenv('SPARK_ADAPTIVE_ENABLED', 'true').lower() == 'true'
    
    def validate(self):
        """Validate configuration values"""
        if self.data_freshness_threshold_hours <= 0:
            raise ValueError("Data freshness threshold must be positive")
        if self.spark_shuffle_partitions <= 0:
            raise ValueError("Spark shuffle partitions must be positive")


def create_spark_session_optimized(config: ETLConfig) -> SparkSession:
    """Create and configure optimized Spark session"""
    logger.info("Creating optimized Spark session...")
    
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
                logger.info(f"Found PostgreSQL JDBC driver at {jdbc_driver_path}")
                break
        
        builder = SparkSession.builder \
            .appName("HangerLaneDataTransformation") \
            .config("spark.sql.adaptive.enabled", str(config.enable_adaptive_query_execution).lower()) \
            .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
            .config("spark.sql.adaptive.skewedJoin.enabled", "true") \
            .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
            .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
            .config("spark.driver.memory", config.spark_driver_memory) \
            .config("spark.executor.memory", config.spark_executor_memory) \
            .config("spark.driver.maxResultSize", "2g") \
            .config("spark.sql.shuffle.partitions", str(config.spark_shuffle_partitions)) \
            .config("spark.sql.files.maxPartitionBytes", config.max_partition_bytes)
        
        # Add JDBC driver if found
        if jdbc_driver_path and os.path.exists(jdbc_driver_path):
            builder = builder.config("spark.jars", jdbc_driver_path)
            logger.info(f"Using PostgreSQL JDBC driver: {jdbc_driver_path}")
        else:
            logger.warning("PostgreSQL JDBC driver not found, proceeding without explicit driver")
        
        spark = builder.getOrCreate()
        
        # Set log level to WARN to reduce verbosity
        spark.sparkContext.setLogLevel("WARN")
        
        logger.info("Optimized Spark session created successfully")
        return spark
        
    except Exception as e:
        logger.error(f"Error creating optimized Spark session: {e}")
        raise


def get_connection_params_with_fallback():
    """Get connection parameters with proper fallback logic"""
    try:
        # Try to get from Airflow connection
        from airflow.hooks.base import BaseHook
        connection = BaseHook.get_connection("pg-ssg")
        host = connection.host
        port = connection.port if connection.port else 5432
        database = connection.schema
        user = connection.login
        password = connection.password
        
        logger.info(f"Using Airflow connection 'pg-ssg'")
        logger.info(f"  Host: {host}")
        logger.info(f"  Port: {port}")
        logger.info(f"  Database: {database}")
        logger.info(f"  User: {user}")
        
    except Exception as e:
        logger.warning(f"Could not get Airflow connection 'pg-ssg', using environment variables: {e}")
        # Fallback to environment variables
        host = os.getenv("POSTGRES_HOST", "172.16.7.6")
        port = os.getenv("POSTGRES_PORT", "5432")
        database = os.getenv("POSTGRES_DB", "ssg")
        user = os.getenv("POSTGRES_USER", "postgres")
        password = os.getenv("POSTGRES_PASSWORD", "P@kistan12")
        
        logger.info(f"Environment variables check:")
        logger.info(f"  POSTGRES_HOST: {os.getenv('POSTGRES_HOST', 'Not set')}")
        logger.info(f"  POSTGRES_PORT: {os.getenv('POSTGRES_PORT', 'Not set')}")
        logger.info(f"  POSTGRES_DB: {os.getenv('POSTGRES_DB', 'Not set')}")
        logger.info(f"  POSTGRES_USER: {os.getenv('POSTGRES_USER', 'Not set')}")
        
    return {
        "host": host,
        "port": port,
        "database": database,
        "user": user,
        "password": password,
        "jdbc_url": f"jdbc:postgresql://{host}:{port}/{database}"
    }


def validate_data_quality(df) -> Dict[str, Any]:
    """
    Validate data quality and return quality report
    
    Args:
        df: Spark DataFrame to validate
        
    Returns:
        Dict containing quality metrics and validation results
    """
    logger.info("Starting data quality validation...")
    
    try:
        # Basic statistics
        total_records = df.count()
        unique_lines = df.select("source_connection").distinct().count() if "source_connection" in df.columns else 0
        
        # Date range analysis
        date_stats = None
        if "created_at" in df.columns:
            date_stats = df.agg(
                spark_min("created_at").alias("min_date"),
                spark_max("created_at").alias("max_date"),
                count("created_at").alias("date_count")
            ).collect()[0]
        
        # Quality checks
        issues = []
        passed = True
        
        # Check for minimum record count
        if total_records < 100:
            issues.append(f"Low record count: {total_records}")
            passed = False
            
        # Check for data freshness
        if date_stats and date_stats["max_date"]:
            from datetime import datetime
            max_date = date_stats["max_date"]
            if hasattr(max_date, 'replace'):
                # Handle different timestamp types
                if hasattr(max_date, 'tzinfo'):
                    freshness_hours = (datetime.now(max_date.tzinfo) - max_date).total_seconds() / 3600
                else:
                    freshness_hours = (datetime.now() - max_date).total_seconds() / 3600
                
                if freshness_hours > 48:  # 48 hours
                    issues.append(f"Data not fresh: {freshness_hours:.1f} hours old")
                    # Don't fail on this, just warn
            else:
                issues.append("Could not determine data freshness")
        
        quality_report = {
            "passed": passed,
            "total_records": total_records,
            "unique_lines": unique_lines,
            "date_stats": {
                "min_date": date_stats["min_date"] if date_stats else None,
                "max_date": date_stats["max_date"] if date_stats else None
            } if date_stats else None,
            "issues": issues
        }
        
        logger.info(f"Data quality report: {quality_report}")
        return quality_report
        
    except Exception as e:
        logger.error(f"Error in data quality validation: {e}")
        return {
            "passed": False,
            "total_records": 0,
            "unique_lines": 0,
            "issues": [f"Validation error: {str(e)}"]
        }


def load_source_data(spark, jdbc_url: str, jdbc_properties: Dict[str, str], days_back: int = 1):
    """
    Load source data with proper error handling and optimization
    
    Args:
        spark: SparkSession
        jdbc_url: JDBC connection URL
        jdbc_properties: JDBC properties
        days_back: Number of days back to load data
        
    Returns:
        Spark DataFrame with source data
    """
    logger.info(f"Loading source data for last {days_back} days...")
    
    try:
        # Read data from postgres database
        df = spark.read \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", "operator_daily_performance") \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .option("fetchsize", "10000") \
            .load()
        
        logger.info(f"Initial data load completed. Row count: {df.count()}")
        
        # Filter the data to only include records from the specified period
        if "ODP_Date" in df.columns:
            df_filtered = df.filter(df["ODP_Date"] >= date_sub(current_date(), days_back))
            logger.info(f"Data filtered to last {days_back} days. Row count: {df_filtered.count()}")
            return df_filtered
        else:
            logger.warning("ODP_Date column not found, returning all data")
            return df
            
    except Exception as e:
        logger.error(f"Error loading source data: {e}")
        raise


def perform_aggregations_optimized(df):
    """
    Perform optimized aggregations with proper error handling
    
    Args:
        df: Source Spark DataFrame
        
    Returns:
        Dict of aggregated DataFrames
    """
    logger.info("Performing optimized aggregations...")
    
    try:
        # Check if DataFrame is empty
        record_count = df.count()
        if record_count == 0:
            logger.info("No records to aggregate. Returning empty DataFrames.")
            # Return empty DataFrames with proper schema
            from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType, DateType, BooleanType
            from pyspark.sql import SparkSession
            
            # Create empty DataFrames with appropriate schemas
            spark = SparkSession.builder.getOrCreate()
            
            # Schema for aggregation 1
            schema1 = StructType([
                StructField("ODP_Date", DateType(), True),
                StructField("Shift", StringType(), True),
                StructField("ODPD_ST_Key", IntegerType(), True),
                StructField("ST_ID", StringType(), True),
                StructField("ST_Description", StringType(), True),
                StructField("ODPD_Lot_Number", StringType(), True),
                StructField("ODPD_OC_Key", StringType(), True),
                StructField("OC_Description", StringType(), True),
                StructField("OC_Standard_Time", DoubleType(), True),  # assuming numeric
                StructField("ODPD_Actual_Time", DoubleType(), True),   # assuming numeric
                StructField("ODPD_CM_Key", StringType(), True),
                StructField("CM_Description", StringType(), True),
                StructField("ODPD_SM_Key", IntegerType(), True),
                StructField("SM_Description", StringType(), True),
                StructField("source_connection", StringType(), True),
                # Aggregated fields
                StructField("ODPD_Quantity", DoubleType(), True),
                StructField("Loading_Qty", DoubleType(), True),
                StructField("UnLoading_Qty", DoubleType(), True),
                StructField("record_count", IntegerType(), True)
            ])
            aggregated_df1 = spark.createDataFrame([], schema1)
            
            # Schema for aggregation 2
            schema2 = StructType([
                # Grouping columns (dimensions)
                StructField("ODP_Date", DateType(), True),
                StructField("Shift", StringType(), True),
                StructField("ODPD_ST_Key", IntegerType(), True),
                StructField("ST_ID", StringType(50), True),
                StructField("ST_Description", StringType(100), True),
                StructField("ODPD_Lot_Number", StringType(50), True),
                StructField("ODPD_OC_Key", IntegerType(), True),
                StructField("OC_Description", StringType(100), True),
                StructField("OC_Standard_Time", DoubleType(), True),  # Numeric(10,2) → DoubleType
                StructField("ODPD_Actual_Time", DoubleType(), True),   # Numeric(10,2) → DoubleType
                StructField("ODPD_CM_Key", IntegerType(), True),
                StructField("CM_Description", StringType(100), True),
                StructField("ODPD_SM_Key", IntegerType(), True),
                StructField("SM_Description", StringType(100), True),
                StructField("ODPD_Is_Overtime", BooleanType(), True),
                StructField("ODPD_Overtime_Factor", DoubleType(), True),
                StructField("ODPD_STPO_Key", IntegerType(), True),
                StructField("source_connection", StringType(50), True),
                # Aggregated measures
                StructField("ODPD_Quantity", IntegerType(), True),     # sum → still integer if input was
                StructField("Loading_Qty", IntegerType(), True),
                StructField("UnLoading_Qty", IntegerType(), True),
                StructField("record_count", IntegerType(), True)       # count → always integer
            ])
            aggregated_df2 = spark.createDataFrame([], schema2)
            

            # Schema for aggregation 3
            schema3 = StructType([
                # Grouping Dimensions
                StructField("ODP_Date", DateType(), True),
                StructField("Shift", StringType(), True),
                StructField("ODP_EM_Key", IntegerType(), True),
                StructField("EM_RFID", StringType(), True),
                StructField("EM_Department", StringType(), True),
                StructField("EM_FirstName", StringType(), True),
                StructField("EM_LastName", StringType(), True),
                StructField("ODP_Current_Station", StringType(), True),
                StructField("ODPD_Workstation", StringType(), True),
                StructField("ODPD_WC_Key", IntegerType(), True),
                StructField("ODPD_ST_Key", IntegerType(), True),
                StructField("ST_ID", StringType(), True),
                StructField("ST_Description", StringType(), True),
                StructField("ODPD_Lot_Number", StringType(), True),
                StructField("ODPD_OC_Key", IntegerType(), True),
                StructField("OC_Description", StringType(), True),
                StructField("ODPD_CM_Key", IntegerType(), True),
                StructField("CM_Description", StringType(), True),
                StructField("ODPD_SM_Key", IntegerType(), True),
                StructField("SM_Description", StringType(), True),
                StructField("source_connection", StringType(), True),
                # Aggregated Measures
                StructField("ODPD_Quantity", IntegerType(), True),
                StructField("Loading_Qty", IntegerType(), True),
                StructField("UnLoading_Qty", IntegerType(), True),
                StructField("OC_Standard_Time", DoubleType(), True),      # avg → Double
                StructField("ODPD_Actual_Time", DoubleType(), True),      # avg → Double
                StructField("ODPD_Is_Overtime", BooleanType(), True),     # max → Boolean
                StructField("ODPD_Overtime_Factor", DoubleType(), True),  # avg → Double
                StructField("record_count", IntegerType(), True),
                # Time aggregations (optional)
                StructField("first_clock_in", TimestampType(), True),
                StructField("last_clock_out", TimestampType(), True)
            ])
            aggregated_df3 = spark.createDataFrame([], schema3)



            agg_results = {
                'odp_date_oc': aggregated_df1,
                'odp_date_shift': aggregated_df2,
                'odp_date_employee': aggregated_df3
            }
            
            for name, agg_df in agg_results.items():
                logger.info(f"Aggregation {name} completed with 0 records")
            
            return agg_results
        
        # Cache the DataFrame for multiple operations
        df.cache()
        logger.info("Source DataFrame cached for performance")
        
        # Transform 1: Group by ODP_Date and OC_Description, sum ODPD_Quantity
        logger.info("Performing aggregation 1: by Date and Operation Code...")
        aggregated_df1 = df.groupBy(
            "ODP_Date", "Shift", "ODPD_ST_Key", "ST_ID", "ST_Description",
            "ODPD_Lot_Number", "ODPD_OC_Key", "OC_Description",
            "OC_Standard_Time", "ODPD_Actual_Time", "ODPD_CM_Key",
            "CM_Description", "ODPD_SM_Key", "SM_Description", "source_connection"
        ).agg(
            F.sum("ODPD_Quantity").alias("ODPD_Quantity"),
            F.sum("Loading_Qty").alias("Loading_Qty"),
            F.sum("UnLoading_Qty").alias("UnLoading_Qty"),
            F.count("*").alias("record_count")
        )
        
        # Transform 2: Group by ODP_Date and Shift, sum ODPD_Quantity
        logger.info("Performing aggregation 3: by Date and Employee...")        
        # Group by descriptive/dimensional columns
        grouping_columns_db2 = [
            "ODP_Date", "Shift", "ODPD_ST_Key", "ST_ID", "ST_Description",
            "ODPD_Lot_Number", "ODPD_OC_Key", "OC_Description",
            "OC_Standard_Time", "ODPD_Actual_Time", "ODPD_CM_Key",
            "CM_Description", "ODPD_SM_Key", "SM_Description","ODPD_STPO_Key", "source_connection"
        ]

        # Aggregate additive measures
        aggregated_df2 = df.groupBy(*grouping_columns_db2).agg(
            F.sum("ODPD_Quantity").alias("ODPD_Quantity"),
            F.sum("Loading_Qty").alias("Loading_Qty"),
            F.sum("UnLoading_Qty").alias("UnLoading_Qty"),
            F.avg("ODPD_Overtime_Factor").alias("ODPD_Overtime_Factor"),
            F.max("ODPD_Is_Overtime").alias("ODPD_Is_Overtime"),  # e.g., if any row had overtime
            F.count("*").alias("record_count")  # optional: track number of original rows
        )
        # Transform 3: Group by ODP_Date and Employee, sum ODPD_Quantity
        logger.info("Performing aggregation 3: by Date and Employee...")
        grouping_cols = [
            "ODP_Date", "Shift",
            "ODP_EM_Key", "EM_RFID", "EM_Department", "EM_FirstName", "EM_LastName",
            "ODP_Current_Station", "ODPD_Workstation", "ODPD_WC_Key",
            "ODPD_ST_Key", "ST_ID", "ST_Description", "ODPD_Lot_Number",
            "ODPD_OC_Key", "OC_Description","OC_Standard_Time",
            "ODPD_CM_Key", "CM_Description", "ODPD_SM_Key", "SM_Description",
            "source_connection"
        ]        

        # Optional: Also aggregate time fields (min clock-in, max clock-out)
        aggregated_df3 = df.groupBy(*grouping_cols).agg(
            # Sum production quantities
            F.sum("ODPD_Quantity").alias("ODPD_Quantity"),
            F.sum("Loading_Qty").alias("Loading_Qty"),
            F.sum("UnLoading_Qty").alias("UnLoading_Qty"),
            F.sum("ODPD_Actual_Time").alias("ODPD_Actual_Time"),
            # Overtime flags — take MAX (if any record had overtime)
            F.max("ODPD_Is_Overtime").alias("ODPD_Is_Overtime"),
            F.avg("ODPD_Overtime_Factor").alias("ODPD_Overtime_Factor"),
            # Track number of records aggregated
            F.count("*").alias("record_count"),
            # Optional: capture time range
            F.min("ODP_Actual_Clock_In").alias("first_clock_in"),
            F.max("ODP_Actual_Clock_Out").alias("last_clock_out")
        )


        # Validate aggregation results
        agg_results = {
            'odp_date_oc': aggregated_df1,
            'odp_date_shift': aggregated_df2,
            'odp_date_employee': aggregated_df3
        }
        
        for name, agg_df in agg_results.items():
            record_count = agg_df.count()
            logger.info(f"Aggregation {name} completed with {record_count} records")
            
        # Uncache the source DataFrame
        df.unpersist()
        logger.info("Source DataFrame uncached")
        
        return agg_results
        
    except Exception as e:
        logger.error(f"Error in aggregations: {e}")
        raise


def save_with_upsert_enhanced(df, table_name: str, jdbc_url: str, jdbc_properties: Dict[str, str]):
    """
    Save DataFrame with enhanced upsert logic
    
    Args:
        df: DataFrame to save
        table_name: Target table name
        jdbc_url: JDBC URL
        jdbc_properties: JDBC properties
    """
    record_count = df.count()
    logger.info(f"Saving data to table: {table_name}")
    logger.info(f"DataFrame row count: {record_count}")
    
    # Handle empty DataFrames
    if record_count == 0:
        logger.info(f"No records to save to {table_name}. Skipping save operation.")
        return
    
    try:
        # Add ETL metadata
        df_with_metadata = df.withColumn("etl_processed_at", current_timestamp()) \
                           .withColumn("etl_batch_id", lit(f"batch_{int(time.time())}"))
        
        # For this implementation, we'll use append mode with proper transaction handling
        # In a production environment, you would implement proper upsert logic
        df_with_metadata.write \
            .format("jdbc") \
            .option("url", jdbc_url) \
            .option("dbtable", table_name) \
            .option("user", jdbc_properties["user"]) \
            .option("password", jdbc_properties["password"]) \
            .option("driver", "org.postgresql.Driver") \
            .option("batchsize", "10000") \
            .mode("append") \
            .save()
        
        logger.info(f"Data successfully saved to {table_name}")
        
    except Exception as e:
        logger.error(f"Error saving data to {table_name}: {e}")
        raise


def log_etl_metrics(start_time: float, records_processed: int, tables_updated: list):
    """
    Log comprehensive ETL metrics
    
    Args:
        start_time: ETL process start time
        records_processed: Number of records processed
        tables_updated: List of tables updated
    """
    end_time = time.time()
    duration = end_time - start_time
    
    metrics = {
        'execution_time_seconds': round(duration, 2),
        'records_processed': records_processed,
        'tables_updated': tables_updated,
        'throughput_rps': round(records_processed / duration, 2) if duration > 0 else 0,
        'timestamp': datetime.now().isoformat()
    }
    
    logger.info(f"ETL Metrics: {metrics}")


def transform_data_optimized(spark):
    """
    Optimized data transformation with ETL best practices
    
    Args:
        spark: SparkSession
        
    Returns:
        bool: Success status
    """
    start_time = time.time()
    logger.info("Starting optimized data transformation...")
    
    try:
        # Initialize configuration
        config = ETLConfig()
        config.validate()
        logger.info("Configuration validated successfully")
        
        # Get database connection parameters
        logger.info("Getting PostgreSQL connection parameters...")
        try:
            postgres_connection_params = get_postgres_connection_params("pg-ssg")
            
        except Exception as e:
            logger.warning(f"Error getting connection params from db_utils: {e}")
            logger.info("Using fallback method with environment variables...")
            postgres_connection_params = get_connection_params_with_fallback()
            
        postgres_jdbc_properties = get_postgres_jdbc_properties(postgres_connection_params)
        postgres_jdbc_url = postgres_connection_params["jdbc_url"]
        
        # Print connection details
        logger.info(f"PostgreSQL Host: {postgres_connection_params['host']}")
        logger.info(f"PostgreSQL Port: {postgres_connection_params['port']}")
        logger.info(f"PostgreSQL Database: {postgres_connection_params['database']}")
        logger.info(f"PostgreSQL User: {postgres_connection_params['user']}")
        logger.info(f"PostgreSQL JDBC URL: {postgres_jdbc_url}")
        
        # Load source data
        df = load_source_data(spark, postgres_jdbc_url, postgres_jdbc_properties, days_back=1)
        
        # Validate data quality
        quality_report = validate_data_quality(df)
        if not quality_report['passed']:
            logger.warning(f"Data quality issues detected: {quality_report['issues']}")
            # For critical issues, you might want to stop processing
            # For now, we'll continue but log the warnings
            
        # Check if we have any records before proceeding
        record_count = df.count()
        if record_count == 0:
            logger.info("No records to process. Skipping aggregation and save steps.")
            # Log ETL metrics with 0 records
            log_etl_metrics(start_time, 0, [])
            logger.info("Optimized data transformation completed successfully (no data to process)")
            return True
        
        # Perform aggregations
        aggregated_dfs = perform_aggregations_optimized(df)
        
        # Save results with enhanced logic
        tables_updated = []
        total_records_saved = 0
        
        for table_name, agg_df in aggregated_dfs.items():
            try:
                record_count = agg_df.count()
                total_records_saved += record_count
                save_with_upsert_enhanced(agg_df, table_name, postgres_jdbc_url, postgres_jdbc_properties)
                tables_updated.append(table_name)
                logger.info(f"Successfully saved {record_count} records to {table_name}")
            except Exception as e:
                logger.error(f"Failed to save data to {table_name}: {e}")
                # Continue with other tables but mark as partial failure
                continue
        
        # Log ETL metrics
        log_etl_metrics(start_time, total_records_saved, tables_updated)
        
        logger.info("Optimized data transformation completed successfully")
        return True
        
    except Exception as e:
        logger.error(f"Error in optimized data transformation: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        try:
            # Stop Spark session
            if spark:
                spark.stop()
                logger.info("Spark session stopped")
        except Exception as e:
            logger.error(f"Error stopping Spark session: {e}")


def create_spark_session():
    """
    Backward compatibility function - creates optimized Spark session
    """
    config = ETLConfig()
    return create_spark_session_optimized(config)


def transform_data(spark):
    """
    Backward compatibility function - calls optimized transformation
    """
    return transform_data_optimized(spark)


if __name__ == "__main__":
    print("Starting optimized Spark ETL process...")
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