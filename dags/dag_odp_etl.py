from __future__ import annotations

import json
from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

DAG_ID = "odp_operator_daily_performance_etl"

# Visible in Airflow container due to your mount: ./sparkFiles -> /opt/airflow/sparkFiles
SPARK_APP = "/opt/airflow/sparkFiles/apps/etl_operator_daily_performance.py"
METRICS_DIR = "/opt/airflow/logs/etl_metrics"


def read_metrics_and_xcom(metrics_path: str, **_):
    with open(metrics_path, "r", encoding="utf-8") as f:
        metrics = json.load(f)

    targets = metrics.get("targets", [])
    total_rows = sum((t.get("staging_rows") or 0) for t in targets)

    print("==== ETL SUMMARY ====")
    print(f"success={metrics.get('success')}")
    print(f"lookback_days={metrics.get('lookback_days')}")
    print(f"spark_master={metrics.get('spark', {}).get('master')}")
    print(f"targets={len(targets)} total_staging_rows={total_rows}")
    for t in targets:
        d = t.get("durations", {})
        print(
            f"- {t.get('table')}: rows={t.get('staging_rows')} "
            f"agg={d.get('aggregate_sec')}s write={d.get('write_staging_sec')}s upsert={d.get('upsert_sec')}s"
        )
    if metrics.get("warnings"):
        print("Warnings:")
        for w in metrics["warnings"]:
            print(f"  - {w}")
    print("=====================")

    return metrics  # XCom dict


with DAG(
    dag_id=DAG_ID,
    start_date=datetime(2025, 1, 1),
    schedule=None,
    catchup=False,
    tags=["spark", "etl", "postgres"],
) as dag:

    metrics_path = f"{METRICS_DIR}/metrics__{{{{ dag.dag_id }}}}__{{{{ ts_nodash }}}}.json"

    run_spark = SparkSubmitOperator(
        task_id="run_spark_etl",
        conn_id="spark_default",   # create in Airflow Connections
        application=SPARK_APP,
        name="odp_etl",
        verbose=True,
        deploy_mode="client",
        application_args=[
            "--conn-id", "pg-ssg",
            "--lookback-days", "{{ var.value.get('odp_lookback_days', 7) }}",
            "--metrics-path", metrics_path,
        ],
        env_vars={
            "LOG_LEVEL": "INFO",
            "JDBC_SINK_PARTITIONS": "{{ var.value.get('odp_jdbc_sink_partitions', 16) }}",
            "JDBC_BATCHSIZE": "{{ var.value.get('odp_jdbc_batchsize', 5000) }}",
            "SPARK_SHUFFLE_PARTITIONS": "{{ var.value.get('odp_spark_shuffle_partitions', 48) }}",
            # If you prefer explicit jar (optional because SPARK_CLASSPATH is set in Dockerfile):
            "POSTGRES_JDBC_JAR": "/opt/airflow/sparkFiles/jdbc-drivers/postgresql-42.7.3.jar",
        },
    )

    push_metrics_to_xcom = PythonOperator(
        task_id="push_metrics_to_xcom",
        python_callable=read_metrics_and_xcom,
        op_kwargs={"metrics_path": metrics_path},
    )

    run_spark >> push_metrics_to_xcom
