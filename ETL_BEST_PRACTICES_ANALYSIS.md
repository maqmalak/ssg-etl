# ETL Process Analysis: hanger_line_daily_transform.py and hangerline_transform.py

## Current Implementation Overview

### 1. DAG Structure (hanger_line_daily_transform.py)
```
start_task → check_data_task → [has_data_label → transform_task → save_task → end_task]
                           → [no_data_label → skip_task → end_task]
```

### 2. Data Flow
1. **Extract Check**: Check if recent data exists in `operator_daily_performance` table
2. **Transform**: Use Spark to read data, perform aggregations, and save results
3. **Load**: Save aggregated data to target tables

## ETL Best Practices Analysis

### Current Strengths ✅
1. **Modular Design**: Separate DAG logic from transformation logic
2. **Error Handling**: Basic try-catch blocks with logging
3. **Connection Management**: Uses Airflow Connections with fallback
4. **Branching Logic**: Conditional execution based on data availability
5. **Logging**: Detailed logging throughout the process

### Areas for Improvement ⚠️

## 1. Extraction Improvements

### Current Issues:
- Simple row count check, not checking data quality or freshness
- No data profiling or validation
- Single point of failure in database connection

### Recommended Improvements:
```python
def check_for_data_enhanced(**context):
    """
    Enhanced data check with quality metrics
    """
    logger.info("Starting enhanced data check")
    
    try:
        # Get connection (existing logic)
        connection = get_database_connection()
        
        # Enhanced data quality checks
        cursor = connection.cursor()
        
        # 1. Check recent data count
        cursor.execute("""
            SELECT 
                COUNT(*) as total_records,
                COUNT(DISTINCT source_connection) as line_count,
                MAX(created_at) as latest_record,
                MIN(created_at) as oldest_record
            FROM operator_daily_performance 
            WHERE created_at >= CURRENT_DATE - INTERVAL '2 days'
        """)
        result = cursor.fetchone()
        
        metrics = {
            'total_records': result[0],
            'line_count': result[1],
            'latest_record': result[2],
            'oldest_record': result[3],
            'data_freshness_hours': (datetime.now() - result[2]).total_seconds() / 3600 if result[2] else None
        }
        
        logger.info(f"Data quality metrics: {metrics}")
        
        # Quality thresholds
        if metrics['total_records'] > 0 and metrics['data_freshness_hours'] < 48:
            logger.info("Data quality acceptable, proceeding with transformation")
            return 'has_data'
        else:
            logger.warning("Data quality issues detected")
            return 'no_data'
            
    except Exception as e:
        logger.error(f"Error in enhanced data check: {e}")
        return 'no_data'
```

## 2. Transformation Improvements

### Current Issues:
- Uses overwrite mode instead of proper upsert
- No data validation or cleansing
- Limited error handling in Spark transformations
- No performance optimization for large datasets

### Recommended Improvements:
```python
def transform_data_enhanced(spark):
    """
    Enhanced transformation with ETL best practices
    """
    try:
        # 1. Data Quality Validation
        logger.info("Starting data quality validation")
        df = load_source_data(spark)
        
        # Validate data quality
        quality_report = validate_data_quality(df)
        if not quality_report['passed']:
            logger.warning(f"Data quality issues: {quality_report['issues']}")
            # Handle based on severity
        
        # 2. Incremental Processing
        logger.info("Performing incremental processing")
        # Filter for new/updated records only
        
        # 3. Optimized Aggregations with Caching
        logger.info("Performing optimized aggregations")
        df.cache()  # Cache for multiple transformations
        
        # Transform with proper error handling
        aggregated_dfs = perform_aggregations(df)
        
        # 4. Data Validation
        logger.info("Validating transformation results")
        for name, agg_df in aggregated_dfs.items():
            validate_aggregation_results(agg_df, name)
        
        # 5. Upsert instead of Overwrite
        logger.info("Saving with upsert logic")
        save_with_upsert(aggregated_dfs)
        
        return True
        
    except Exception as e:
        logger.error(f"Transformation error: {e}")
        # Rollback mechanism
        return False

def save_with_upsert(aggregated_dfs):
    """
    Proper upsert implementation instead of overwrite
    """
    for table_name, df in aggregated_dfs.items():
        try:
            # Create staging table
            staging_table = f"{table_name}_staging"
            
            # Write to staging table
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", staging_table) \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .mode("overwrite") \
                .save()
            
            # Perform upsert using PostgreSQL MERGE or ON CONFLICT
            # This is a simplified example
            upsert_query = f"""
                INSERT INTO {table_name} 
                SELECT * FROM {staging_table}
                ON CONFLICT (primary_key_columns) 
                DO UPDATE SET 
                    column1 = EXCLUDED.column1,
                    column2 = EXCLUDED.column2
            """
            
            # Execute upsert
            execute_query(upsert_query)
            
            # Clean up staging table
            cleanup_query = f"DROP TABLE {staging_table}"
            execute_query(cleanup_query)
            
        except Exception as e:
            logger.error(f"Upsert failed for {table_name}: {e}")
            raise
```

## 3. Loading Improvements

### Current Issues:
- Uses overwrite mode which can cause data loss
- No transaction management
- No data lineage tracking
- No performance monitoring

### Recommended Improvements:
```python
def save_with_data_lineage(df, table_name, source_info):
    """
    Save with data lineage tracking
    """
    try:
        # Add lineage columns
        from pyspark.sql.functions import current_timestamp, lit
        df_with_lineage = df.withColumn("etl_processed_at", current_timestamp()) \
                          .withColumn("etl_source", lit(source_info['source'])) \
                          .withColumn("etl_batch_id", lit(source_info['batch_id']))
        
        # Save with proper transaction management
        save_with_transaction_control(df_with_lineage, table_name)
        
        # Log lineage information
        log_data_lineage(source_info, table_name)
        
    except Exception as e:
        logger.error(f"Error in data lineage tracking: {e}")
        raise

def save_with_transaction_control(df, table_name):
    """
    Save with transaction control and rollback capability
    """
    try:
        # Start transaction
        connection = get_database_connection()
        connection.autocommit = False
        
        try:
            # Save data
            df.write \
                .format("jdbc") \
                .option("url", jdbc_url) \
                .option("dbtable", table_name) \
                .option("user", jdbc_properties["user"]) \
                .option("password", jdbc_properties["password"]) \
                .mode("append") \
                .save()
            
            # Commit transaction
            connection.commit()
            logger.info(f"Successfully saved data to {table_name}")
            
        except Exception as e:
            # Rollback on error
            connection.rollback()
            logger.error(f"Transaction failed, rolled back: {e}")
            raise
            
    except Exception as e:
        logger.error(f"Error in transaction control: {e}")
        raise
    finally:
        if connection:
            connection.close()
```

## 4. Performance Optimization

### Current Issues:
- No partitioning strategy
- No memory management
- No parallel processing optimization

### Recommended Improvements:
```python
def create_spark_session_optimized():
    """
    Create optimized Spark session
    """
    spark = SparkSession.builder \
        .appName("HangerLaneDataTransformation") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.sql.adaptive.skewedJoin.enabled", "true") \
        .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "true") \
        .config("spark.driver.memory", "4g") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.maxResultSize", "2g") \
        .config("spark.sql.shuffle.partitions", "200") \
        .config("spark.sql.files.maxPartitionBytes", "134217728") \  # 128MB
        .getOrCreate()
    
    return spark

def optimize_dataframe_processing(df):
    """
    Optimize DataFrame processing
    """
    # Repartition for optimal processing
    optimal_partitions = max(200, df.rdd.getNumPartitions() // 2)
    df = df.repartition(optimal_partitions)
    
    # Cache frequently used DataFrames
    df.cache()
    
    # Use broadcast joins for small tables
    # df.join(broadcast(small_df), join_conditions)
    
    return df
```

## 5. Monitoring and Observability

### Current Issues:
- Basic logging only
- No performance metrics
- No alerting mechanism

### Recommended Improvements:
```python
def log_etl_metrics(start_time, records_processed, tables_updated):
    """
    Log comprehensive ETL metrics
    """
    end_time = time.time()
    duration = end_time - start_time
    
    metrics = {
        'execution_time_seconds': duration,
        'records_processed': records_processed,
        'tables_updated': tables_updated,
        'throughput_rps': records_processed / duration if duration > 0 else 0,
        'timestamp': datetime.now().isoformat()
    }
    
    logger.info(f"ETL Metrics: {metrics}")
    
    # Send to monitoring system
    # send_metrics_to_monitoring(metrics)

def setup_error_alerting():
    """
    Setup error alerting mechanism
    """
    try:
        # Configure alerting for critical errors
        # send_alert_to_slack("ETL Process Error", error_message)
        pass
    except Exception as e:
        logger.error(f"Alerting setup failed: {e}")
```

## 6. Configuration Management

### Current Issues:
- Hardcoded values and magic numbers
- No configuration validation

### Recommended Improvements:
```python
# config.py
class ETLConfig:
    def __init__(self):
        self.data_freshness_threshold_hours = int(os.getenv('DATA_FRESHNESS_THRESHOLD', '48'))
        self.min_records_threshold = int(os.getenv('MIN_RECORDS_THRESHOLD', '1000'))
        self.spark_executor_memory = os.getenv('SPARK_EXECUTOR_MEMORY', '4g')
        self.spark_driver_memory = os.getenv('SPARK_DRIVER_MEMORY', '4g')
        self.max_retry_attempts = int(os.getenv('MAX_RETRY_ATTEMPTS', '3'))
    
    def validate(self):
        """Validate configuration values"""
        if self.data_freshness_threshold_hours <= 0:
            raise ValueError("Data freshness threshold must be positive")
        if self.min_records_threshold < 0:
            raise ValueError("Minimum records threshold cannot be negative")
```

## Summary of Recommendations

### Immediate Improvements:
1. **Enhanced Data Quality Checks**: Add comprehensive data validation
2. **Proper Upsert Logic**: Replace overwrite with upsert operations
3. **Better Error Handling**: Add rollback mechanisms and detailed error reporting
4. **Performance Optimization**: Configure Spark for better resource utilization

### Medium-term Improvements:
1. **Data Lineage Tracking**: Add ETL process metadata
2. **Monitoring and Alerting**: Implement comprehensive metrics and alerting
3. **Configuration Management**: Externalize configuration with validation

### Long-term Improvements:
1. **Incremental Processing**: Implement change data capture
2. **Schema Evolution**: Handle schema changes gracefully
3. **Data Quality Framework**: Implement comprehensive data quality checks

These improvements will make the ETL process more robust, maintainable, and production-ready while following industry best practices.