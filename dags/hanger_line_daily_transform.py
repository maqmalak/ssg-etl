"""
DAG for running hanger line data transformation daily using SparkSubmitOperator.
This DAG submits Spark jobs to the cluster for distributed processing.

Best Practices:
- Uses SparkSubmitOperator for proper Spark job submission
- JSON metrics for monitoring and XCom communication
- Configurable via Airflow Variables
- Clean resource management (Spark handles its own session lifecycle)
"""

from datetime import datetime, timedelta
import json
import logging
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from pendulum import timezone

# Timezone configuration
PKT = timezone("Asia/Karachi")

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Paths
SPARK_APP = "/opt/airflow/sparkFiles/hangerline_transform_spark.py"
METRICS_DIR = "/opt/airflow/logs/etl_metrics"

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2025, 11, 20, tzinfo=PKT),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'execution_timeout': timedelta(hours=2),  # Increased timeout for large data processing
}


def read_metrics_and_push_xcom(metrics_path: str, **context):
    """
    Reads JSON metrics written by Spark, logs a friendly summary,
    and returns dict -> stored as XCom automatically.
    """
    try:
        with open(metrics_path, "r", encoding="utf-8") as f:
            metrics = json.load(f)
    except FileNotFoundError:
        logger.error("Metrics file not found: %s", metrics_path)
        return {
            "success": False,
            "message": "Metrics file not found",
            "tables_processed": []
        }
    except json.JSONDecodeError as e:
        logger.error("Invalid JSON in metrics file: %s", e)
        return {
            "success": False,
            "message": f"Invalid metrics JSON: {e}",
            "tables_processed": []
        }

    # Extract key information
    success = metrics.get("success", False)
    lookback_days = metrics.get("lookback_days", 7)
    data_loaded = metrics.get("source", {}).get("rows", "(count disabled)")
    tables_processed = []
    
    # Process target tables information
    for target in metrics.get("targets", []):
        tables_processed.append({
            "table": target.get("table"),
            "records": target.get("staging_rows", "(count disabled)"),
            "aggregate_sec": target.get("durations", {}).get("aggregate_sec"),
            "write_sec": target.get("durations", {}).get("write_staging_sec"),
            "upsert_sec": target.get("durations", {}).get("upsert_sec")
        })

    # Friendly summary in Airflow logs
    logger.info("=" * 80)
    logger.info("=== TRANSFORMATION SUMMARY ===")
    logger.info(f"✅ Success: {success}")
    logger.info(f"📅 Lookback Days: {lookback_days}")
    logger.info(f"📊 Data Loaded: {data_loaded}")
    logger.info(f"🎯 Spark Master: {metrics.get('spark', {}).get('master', 'N/A')}")
    logger.info(f"📋 Tables Processed: {len(tables_processed)}")
    
    for table_info in tables_processed:
        table_name = table_info.get("table", "Unknown")
        records = table_info.get("records", 0)
        agg_sec = table_info.get("aggregate_sec", 0)
        write_sec = table_info.get("write_sec", 0)
        upsert_sec = table_info.get("upsert_sec", 0)
        
        # Format count with thousand separator if numeric
        if isinstance(records, (int, float)):
            count_str = f"{records:,}"
        else:
            count_str = str(records)
        
        logger.info(f"   • {table_name}: {count_str} rows")
        logger.info(f"      ⏱️  Aggregate: {agg_sec}s, Write: {write_sec}s, Upsert: {upsert_sec}s")
    
    # Log warnings if any
    warnings = metrics.get("warnings", [])
    if warnings:
        logger.warning("⚠️  Warnings:")
        for w in warnings:
            logger.warning(f"   - {w}")
    
    logger.info(f"💬 Message: {metrics.get('message', 'No message')}")
    logger.info("=" * 80)

    # Return structured results for XCom
    return {
        "success": success,
        "data_loaded": data_loaded,
        "tables_processed": tables_processed,
        "message": metrics.get("message", ""),
        "lookback_days": lookback_days,
        "warnings": warnings
    }


def log_start(**context):
    """Log the start of the DAG execution."""
    logger.info("=" * 80)
    logger.info("Starting hanger_line_daily_transform DAG execution")
    logger.info(f"Execution date: {context['execution_date']}")
    logger.info(f"Run ID: {context['run_id']}")
    logger.info("=" * 80)
    return "DAG execution started"


def log_end(**context):
    """Log the end of the DAG execution."""
    ti = context['task_instance']
    results = ti.xcom_pull(task_ids='read_metrics')
    
    logger.info("=" * 80)
    logger.info("Completed hanger_line_daily_transform DAG execution")
    logger.info(f"Execution date: {context['execution_date']}")
    logger.info(f"Run ID: {context['run_id']}")
    
    if results:
        logger.info(f"Final Status: {'✅ SUCCESS' if results.get('success') else '❌ FAILED'}")
    
    logger.info("=" * 80)
    return "DAG execution completed"


# Define the DAG
with DAG(
    dag_id='hanger_line_daily_transform',
    default_args=default_args,
    description='Daily transformation of hanger line data using Spark cluster',
    schedule="*/7 8-23,0-1 * * 1-6",  # Every 7 min, 8AM–2AM, Mon–Sat
    catchup=False,
    tags=['ssg', 'hanger_line', 'transformation', 'spark'],
    max_active_runs=1
) as dag:

    # Unique metrics path per run (templated)
    metrics_path = f"{METRICS_DIR}/metrics__{{{{ dag.dag_id }}}}__{{{{ ts_nodash }}}}.json"

    # Start task
    start_task = PythonOperator(
        task_id='start',
        python_callable=log_start,
    )

    # Run Spark ETL job using SparkSubmitOperator
    run_spark_etl = SparkSubmitOperator(
        task_id="run_spark_transformation",
        application=SPARK_APP,
        conn_id="spark_default",  # Uses existing Spark cluster connection
        verbose=True,
        application_args=[
            "--conn-id", "pg-ssg",
            "--lookback-days", "{{ var.value.get('odp_lookback_days', 7) }}",
            "--metrics-path", metrics_path,
            "--app-name", "HangerLaneDataTransformation_{{ ts_nodash }}",
            # Uncomment if you want source Spark count in addition:
            # "--debug-rowcounts",
        ],
        env_vars={
            # Tuning knobs (use Airflow Variables for easy configuration)
            "JDBC_SINK_PARTITIONS": "{{ var.value.get('odp_jdbc_sink_partitions', 16) }}",
            "JDBC_BATCHSIZE": "{{ var.value.get('odp_jdbc_batchsize', 5000) }}",
            "SPARK_SHUFFLE_PARTITIONS": "{{ var.value.get('odp_spark_shuffle_partitions', 48) }}",
            "JDBC_FETCHSIZE": "{{ var.value.get('odp_jdbc_fetchsize', 10000) }}",
            "JDBC_QUERY_TIMEOUT": "{{ var.value.get('odp_jdbc_query_timeout', 600) }}",
            "LOG_LEVEL": "INFO",
        },
    )

    # Read metrics and push to XCom
    read_metrics = PythonOperator(
        task_id="read_metrics",
        python_callable=read_metrics_and_push_xcom,
        op_kwargs={"metrics_path": metrics_path},
    )

    # End task
    end_task = PythonOperator(
        task_id='end',
        python_callable=log_end,
    )

    # Set task dependencies
    start_task >> run_spark_etl >> read_metrics >> end_task
