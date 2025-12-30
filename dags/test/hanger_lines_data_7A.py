"""
Optimized ETL DAG for Hanger Line Data Processing
Using Spark Cluster Mode (1 Master + 2 Workers)
Task Flow: source_check >> target_check >> load_data >> transform_data >> summary
"""

from __future__ import annotations

import json
import os
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.utils.task_group import TaskGroup


# Configuration
DAG_ID = "hanger_lines_data_7A_cluster"
SPARK_APP_BASE = "/opt/airflow/sparkFiles"
METRICS_DIR = "/opt/airflow/logs/etl_metrics"
LOOKBACK_DAYS = 30

# Spark cluster configuration (temporarily using local mode for testing)
SPARK_MASTER = os.getenv("SPARK_MASTER_URL", "local[*]")
SPARK_CONN_ID = "spark_default"

# JDBC driver path (must be accessible from all Spark nodes)
POSTGRES_JDBC_JAR = "/opt/airflow/sparkFiles/jdbc-drivers/postgresql-42.7.3.jar"


def create_metrics_path(task_name: str, **context) -> str:
    """Generate unique metrics path for each task"""
    dag_id = context['dag'].dag_id
    ts = context['ts_nodash']
    path = f"{METRICS_DIR}/metrics__{dag_id}__{task_name}__{ts}.json"
    return path


def read_and_log_metrics(metrics_path: str, task_name: str, **context):
    """
    Read JSON metrics written by Spark and log summary.
    Returns metrics dict for XCom.
    """
    if not os.path.exists(metrics_path):
        print(f"⚠ Metrics file not found: {metrics_path}")
        return {"success": False, "task": task_name, "error": "Metrics file not found"}
    
    try:
        with open(metrics_path, "r", encoding="utf-8") as f:
            metrics = json.load(f)
        
        print(f"{'='*80}")
        print(f"ETL METRICS - {task_name.upper()}")
        print(f"{'='*80}")
        print(f"Success: {metrics.get('success', False)}")
        print(f"Task: {metrics.get('task', task_name)}")
        print(f"Duration: {metrics.get('duration_sec', 0):.2f} seconds")
        
        if 'record_count' in metrics:
            print(f"Records: {metrics.get('record_count', 0):,}")
        
        if 'tables' in metrics:
            for table in metrics['tables']:
                print(f"  Table: {table.get('name', 'unknown')}")
                print(f"    Records: {table.get('count', 0):,}")
                print(f"    Duration: {table.get('duration_sec', 0):.2f}s")
        
        if metrics.get('warnings'):
            print("Warnings:")
            for w in metrics['warnings']:
                print(f"  - {w}")
        
        print(f"{'='*80}")
        
        return metrics
        
    except Exception as e:
        print(f"✗ Error reading metrics: {e}")
        return {"success": False, "task": task_name, "error": str(e)}


def generate_summary(**context):
    """
    Generate comprehensive ETL summary from all task metrics.
    Pulls XCom data from previous tasks.
    """
    ti = context['ti']
    
    # Pull metrics from all tasks
    source_check = ti.xcom_pull(task_ids='source_check_task.read_metrics')
    target_check = ti.xcom_pull(task_ids='target_check_task.read_metrics')
    load_data = ti.xcom_pull(task_ids='load_data_task.read_metrics')
    transform_data = ti.xcom_pull(task_ids='transform_data_task.read_metrics')
    
    print(f"{'='*80}")
    print("ETL PIPELINE SUMMARY")
    print(f"{'='*80}")
    print(f"DAG: {context['dag'].dag_id}")
    print(f"Run: {context['execution_date']}")
    print(f"{'='*80}")
    
    total_duration = 0
    total_records = 0
    
    for task_name, metrics in [
        ('Source Check', source_check),
        ('Target Check', target_check),
        ('Load Data', load_data),
        ('Transform Data', transform_data)
    ]:
        if metrics:
            success = "✓" if metrics.get('success') else "✗"
            duration = metrics.get('duration_sec', 0)
            records = metrics.get('record_count', 0)
            
            print(f"{success} {task_name}:")
            print(f"    Duration: {duration:.2f}s")
            if records > 0:
                print(f"    Records: {records:,}")
            
            total_duration += duration
            total_records += records
    
    print(f"{'='*80}")
    print(f"Total Duration: {total_duration:.2f} seconds ({total_duration/60:.2f} minutes)")
    print(f"Total Records Processed: {total_records:,}")
    print(f"{'='*80}")
    
    summary = {
        "dag_id": context['dag'].dag_id,
        "execution_date": str(context['execution_date']),
        "total_duration_sec": total_duration,
        "total_records": total_records,
        "tasks": {
            "source_check": source_check,
            "target_check": target_check,
            "load_data": load_data,
            "transform_data": transform_data
        }
    }
    
    # Save summary to file
    summary_path = f"{METRICS_DIR}/summary__{context['dag'].dag_id}__{context['ts_nodash']}.json"
    os.makedirs(METRICS_DIR, exist_ok=True)
    with open(summary_path, 'w', encoding='utf-8') as f:
        json.dump(summary, f, indent=2)
    
    print(f"✓ Summary saved to: {summary_path}")
    
    return summary


# Common Spark configuration (local mode for testing)
def get_spark_conf():
    """Get optimized Spark configuration for local mode testing"""
    return {
        # Local mode configuration
        "spark.master": SPARK_MASTER,

        # Resource allocation for local mode (use available cores)
        "spark.driver.memory": "4g",
        "spark.driver.cores": "4",

        # Memory management for local mode
        "spark.memory.fraction": "0.8",
        "spark.memory.storageFraction": "0.3",
        "spark.driver.memoryOverhead": "1g",

        # Adaptive Query Execution
        "spark.sql.adaptive.enabled": "true",
        "spark.sql.adaptive.coalescePartitions.enabled": "true",
        "spark.sql.adaptive.skewJoin.enabled": "true",
        "spark.sql.adaptive.advisoryPartitionSizeInBytes": "128MB",

        # Shuffle optimization
        "spark.sql.shuffle.partitions": "8",  # Reduced for local mode
        "spark.shuffle.compress": "true",
        "spark.shuffle.spill.compress": "true",

        # Serialization
        "spark.serializer": "org.apache.spark.serializer.KryoSerializer",
        "spark.kryoserializer.buffer.max": "512m",

        # UI configuration
        "spark.ui.port": "4040",
        "spark.ui.enabled": "true",

        # JDBC driver configuration for local mode
        "spark.jars": POSTGRES_JDBC_JAR,
        "spark.driver.extraClassPath": f"{POSTGRES_JDBC_JAR}",
    }


# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    dag_id=DAG_ID,
    default_args=default_args,
    description='ETL Pipeline for Hanger Line Data using Spark Cluster Mode',
    schedule_interval=None,  # Set to your preferred schedule (e.g., '@daily')
    start_date=datetime(2025, 1, 1),
    catchup=False,
    tags=['spark-cluster', 'etl', 'hanger-line', 'production'],
    max_active_runs=1,
) as dag:

    # Task 1: Source Check
    with TaskGroup(group_id='source_check_task') as source_check_task:
        
        run_source_check = SparkSubmitOperator(
            task_id='run_source_check',
            application=f"{SPARK_APP_BASE}/hangerline_transform_spark_7A.py",
            conn_id=SPARK_CONN_ID,
            conf=get_spark_conf(),
            jars=POSTGRES_JDBC_JAR,
            application_args=[
                '--phase', 'source_check',
                '--lookback-days', str(LOOKBACK_DAYS),
                '--metrics-path', "{{ ti.xcom_pull(task_ids='source_check_task.generate_path') }}",
            ],
            verbose=True,
            executor_cores=2,
            executor_memory='8g',
            driver_memory='4g',
            num_executors=2,
        )
        
        generate_path = PythonOperator(
            task_id='generate_path',
            python_callable=create_metrics_path,
            op_kwargs={'task_name': 'source_check'},
        )
        
        read_metrics = PythonOperator(
            task_id='read_metrics',
            python_callable=read_and_log_metrics,
            op_kwargs={
                'metrics_path': "{{ ti.xcom_pull(task_ids='source_check_task.generate_path') }}",
                'task_name': 'source_check',
            },
        )
        
        generate_path >> run_source_check >> read_metrics

    # Task 2: Target Check
    with TaskGroup(group_id='target_check_task') as target_check_task:
        
        run_target_check = SparkSubmitOperator(
            task_id='run_target_check',
            application=f"{SPARK_APP_BASE}/hangerline_transform_spark_7A.py",
            conn_id=SPARK_CONN_ID,
            conf=get_spark_conf(),
            jars=POSTGRES_JDBC_JAR,
            application_args=[
                '--phase', 'target_check',
                '--lookback-days', str(LOOKBACK_DAYS),
                '--metrics-path', "{{ ti.xcom_pull(task_ids='target_check_task.generate_path') }}",
            ],
            verbose=True,
            executor_cores=2,
            executor_memory='8g',
            driver_memory='4g',
            num_executors=2,
        )
        
        generate_path = PythonOperator(
            task_id='generate_path',
            python_callable=create_metrics_path,
            op_kwargs={'task_name': 'target_check'},
        )
        
        read_metrics = PythonOperator(
            task_id='read_metrics',
            python_callable=read_and_log_metrics,
            op_kwargs={
                'metrics_path': "{{ ti.xcom_pull(task_ids='target_check_task.generate_path') }}",
                'task_name': 'target_check',
            },
        )
        
        generate_path >> run_target_check >> read_metrics

    # Task 3: Load Data
    with TaskGroup(group_id='load_data_task') as load_data_task:
        
        run_load_data = SparkSubmitOperator(
            task_id='run_load_data',
            application=f"{SPARK_APP_BASE}/hangerline_transform_spark_7A.py",
            conn_id=SPARK_CONN_ID,
            conf=get_spark_conf(),
            jars=POSTGRES_JDBC_JAR,
            application_args=[
                '--phase', 'load_data',
                '--lookback-days', str(LOOKBACK_DAYS),
                '--metrics-path', "{{ ti.xcom_pull(task_ids='load_data_task.generate_path') }}",
            ],
            verbose=True,
            executor_cores=3,
            executor_memory='8g',
            driver_memory='4g',
            num_executors=2,
        )
        
        generate_path = PythonOperator(
            task_id='generate_path',
            python_callable=create_metrics_path,
            op_kwargs={'task_name': 'load_data'},
        )
        
        read_metrics = PythonOperator(
            task_id='read_metrics',
            python_callable=read_and_log_metrics,
            op_kwargs={
                'metrics_path': "{{ ti.xcom_pull(task_ids='load_data_task.generate_path') }}",
                'task_name': 'load_data',
            },
        )
        
        generate_path >> run_load_data >> read_metrics

    # Task 4: Transform Data
    with TaskGroup(group_id='transform_data_task') as transform_data_task:
        
        run_transform_data = SparkSubmitOperator(
            task_id='run_transform_data',
            application=f"{SPARK_APP_BASE}/hangerline_transform_spark_7A.py",
            conn_id=SPARK_CONN_ID,
            conf=get_spark_conf(),
            application_args=[
                '--phase', 'transform_data',
                '--lookback-days', str(LOOKBACK_DAYS),
                '--metrics-path', "{{ ti.xcom_pull(task_ids='transform_data_task.generate_path') }}",
            ],
            verbose=True,
        )
        
        generate_path = PythonOperator(
            task_id='generate_path',
            python_callable=create_metrics_path,
            op_kwargs={'task_name': 'transform_data'},
        )
        
        read_metrics = PythonOperator(
            task_id='read_metrics',
            python_callable=read_and_log_metrics,
            op_kwargs={
                'metrics_path': "{{ ti.xcom_pull(task_ids='transform_data_task.generate_path') }}",
                'task_name': 'transform_data',
            },
        )
        
        generate_path >> run_transform_data >> read_metrics

    # Task 5: Summary
    summary_task = PythonOperator(
        task_id='summary',
        python_callable=generate_summary,
    )

    # Define task dependencies
    source_check_task >> target_check_task >> load_data_task >> transform_data_task >> summary_task
