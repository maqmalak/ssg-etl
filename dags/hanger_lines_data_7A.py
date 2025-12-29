from __future__ import annotations

import json
import os
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator


# DAG_ID = "hanger_lines_data_7A"
SPARK_APP = "/opt/airflow/sparkFiles/hangerline_transform_spark_7A.py"  # <-- adjust path
METRICS_DIR = "/opt/airflow/logs/etl_metrics"  # shared path accessible by Airflow + Spark container


def read_metrics_and_push_xcom(metrics_path: str, **context):
    """
    Reads JSON metrics written by Spark, logs a friendly summary,
    and returns dict -> stored as XCom automatically.
    """
    with open(metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    # Friendly summary in Airflow logs
    targets = metrics.get("targets", [])
    total_rows = sum((t.get("staging_rows") or 0) for t in targets)

    print("==== ETL SUMMARY ====")
    print(f"Success: {metrics.get('success')}")
    print(f"Lookback days: {metrics.get('lookback_days')}")
    print(f"Spark master: {metrics.get('spark', {}).get('master')}")
    print(f"Targets: {len(targets)}")
    print(f"Total staging rows (sum): {total_rows}")
    for t in targets:
        print(
            f"- {t.get('table')}: rows={t.get('staging_rows')}, "
            f"agg_sec={t.get('durations', {}).get('aggregate_sec')}, "
            f"write_sec={t.get('durations', {}).get('write_staging_sec')}, "
            f"upsert_sec={t.get('durations', {}).get('upsert_sec')}"
        )
    if metrics.get("warnings"):
        print("Warnings:")
        for w in metrics["warnings"]:
            print(f"  - {w}")

    print("=====================")

    # Return dict -> XCom
    return metrics


with DAG(
    dag_id='hanger_lines_data_7A',
    start_date=datetime(2025, 1, 1),
    schedule=None,  # set your cron if needed
    catchup=False,
    tags=["spark", "etl", "postgres"],
    default_args={"owner": "airflow"},
) as dag:

    # Unique metrics path per run (templated)
    metrics_path = f"{METRICS_DIR}/metrics__{{{{ dag.dag_id }}}}__{{{{ ts_nodash }}}}.json"

    run_spark_etl = SparkSubmitOperator(
        task_id="run_spark_etl",
        application=SPARK_APP,
        conn_id="spark_default",  # configure in Airflow Connections
        verbose=True,
        application_args=[
            "--conn-id", "pg-ssg",
            "--lookback-days", "{{ var.value.get('odp_lookback_days', 7) }}",
            "--metrics-path", metrics_path,
            # Uncomment if you want source Spark count in addition:
            # "--debug-rowcounts",
        ],
        env_vars={
            # Optional tuning knobs (safe defaults)
            "JDBC_SINK_PARTITIONS": "{{ var.value.get('odp_jdbc_sink_partitions', 16) }}",
            "JDBC_BATCHSIZE": "{{ var.value.get('odp_jdbc_batchsize', 5000) }}",
            "SPARK_SHUFFLE_PARTITIONS": "{{ var.value.get('odp_spark_shuffle_partitions', 48) }}",
            # If needed:
            # "POSTGRES_JDBC_JAR": "/opt/airflow/sparkFiles/jdbc-drivers/postgresql-42.7.3.jar",
            "LOG_LEVEL": "INFO",
        },
    )

    push_metrics_to_xcom = PythonOperator(
        task_id="push_metrics_to_xcom",
        python_callable=read_metrics_and_push_xcom,
        op_kwargs={"metrics_path": metrics_path},
    )

    run_spark_etl >> push_metrics_to_xcom
