"""
Hourly DAG for aggregating hanger line data and upserting to odp_hourly_summary
"""

import logging, json
from datetime import datetime, timedelta
import pandas as pd
from pendulum import timezone
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.base import BaseHook
from urllib.parse import quote_plus
from sqlalchemy import create_engine

# --- Imports from helpers ---
from scripts.create_table_hourly import (
    create_hourly_table_if_not_exists,
    create_etl_hourly_log_odp_table_if_not_exists,
)
from upsert_utils import (
    upsert_data_via_postgres,
    create_connection_params_from_airflow,
)

# ---------------- CONFIG ---------------- #
PKT = timezone("Asia/Karachi")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ---------------- DB UTILS ---------------- #
def get_database_connection_uri():
    """Return SQLAlchemy DB URI from Airflow connection"""
    conn = BaseHook.get_connection("pg-ssg")
    password = quote_plus(conn.password or "")
    return f"postgresql://{conn.login}:{password}@{conn.host}:{conn.port}/{conn.schema}"


def log_etl(process_log_id, step, status, record_count=0, error_message=None):
    """Write step logs into etl_extract_hourly_log"""
    try:
        uri = get_database_connection_uri()
        engine = create_engine(uri)
        now = datetime.now(PKT)
        with engine.begin() as conn:
            conn.execute(
                """
                INSERT INTO etl_extract_hourly_log
                (processlogid, source_connection, saved_count,
                 starttime, endtime, opd_date,
                 lastextractdatetime, success, status, errormessage)
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    process_log_id,
                    "pg-ssg",
                    record_count,
                    now, now,
                    now.date(),
                    now,
                    status == "success",
                    f"{step}_{status}",
                    error_message,
                ),
            )
        engine.dispose()
        logger.info(f"✅ Logged {step} - {status} - {record_count}")
    except Exception as e:
        logger.error(f"❌ Failed to log ETL step {step}: {e}")


# ---------------- ETL TASKS ---------------- #
def create_tables(**context):
    process_id = f"hourly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    uri = get_database_connection_uri()
    engine = create_engine(uri)

    create_hourly_table_if_not_exists(engine)
    create_etl_hourly_log_odp_table_if_not_exists(engine)

    engine.dispose()
    log_etl(process_id, "create_tables", "success")
    return "Tables created"


def extract(**context):
    process_id = f"hourly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    uri = get_database_connection_uri()
    engine = create_engine(uri)

    needed_columns = ["odpd_key",
        "odp_date", "shift", "odp_em_key", "em_firstname", "em_lastname",
        "odpd_workstation", "odpd_wc_key", "odpd_st_key", "st_id", "st_description",
        "odpd_lot_number", "odpd_oc_key", "oc_description","odpd_quantity",
        "loading_qty", "unloading_qty", "oc_standard_time", "odpd_actual_time",
        "odpd_cm_key", "cm_description", "odpd_sm_key", "sm_description",
        "odpd_is_overtime", "odpd_stpo_key", "source_connection", "created_at"
    ]

    columns_str = ", ".join([f'"{col}"' for col in needed_columns])
    query = f"""
        SELECT {columns_str}
        FROM operator_daily_performance
        WHERE "created_at" >= NOW() - INTERVAL '1 hours'
    """

    df = pd.read_sql(query, engine)
    engine.dispose()

    count = len(df)
    context["ti"].xcom_push(key="source_df", value=df.to_json(orient="records", date_format="iso"))
    log_etl(process_id, "extract", "success" if count > 0 else "no_data", count)
    return f"Extracted {count} records"


def aggregate(**context):
    process_id = f"hourly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    df_json = context["ti"].xcom_pull(task_ids="extract", key="source_df")
    if not df_json:
        log_etl(process_id, "aggregate", "no_data")
        return "No data to aggregate"

    df = pd.read_json(df_json, orient="records")

    if df.empty:
        log_etl(process_id, "aggregate", "no_data")
        return "No records to aggregate"

    # Build em_description
    df["em_description"] = df["odp_em_key"].astype(str) + "-" + df["em_firstname"].fillna("") + "-" + df["em_lastname"].fillna("")

    # Perform aggregation
    agg_df = df.groupby(
        ["odp_date", "shift", "odp_em_key", "em_description",
         "odpd_workstation", "odpd_wc_key", "odpd_st_key", "st_id", "st_description",
         "odpd_lot_number", "odpd_oc_key", "oc_description",
         "odpd_cm_key", "cm_description", "odpd_sm_key", "sm_description",
         "odpd_is_overtime", "odpd_stpo_key", "source_connection"]
    ).agg(
        total_quantity=("odpd_quantity", "sum"),
        total_loading_qty=("loading_qty", "sum"),
        total_unloading_qty=("unloading_qty", "sum"),
        total_actual_time=("odpd_actual_time", "sum"),
        total_standard_time=("oc_standard_time", "sum"),
        record_count=("odpd_key", "count")
    ).reset_index()

    # Add system columns
    agg_df["hour_timestamp"] = datetime.now(PKT).replace(minute=0, second=0, microsecond=0)
    agg_df["created_at"] = datetime.now(PKT)

    context["ti"].xcom_push(key="agg_df", value=agg_df.to_json(orient="records", date_format="iso"))
    log_etl(process_id, "aggregate", "success", len(agg_df))
    return f"Aggregated {len(agg_df)} rows"


def upsert(**context):
    process_id = f"hourly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    df_json = context["ti"].xcom_pull(task_ids="aggregate", key="agg_df")
    if not df_json:
        log_etl(process_id, "upsert", "no_data")
        return "No aggregated data to upsert"

    agg_records = json.loads(df_json)
    if not agg_records:
        log_etl(process_id, "upsert", "no_data")
        return "No records"

    key_columns = [
        "hour_timestamp", "odp_date", "shift", "odp_em_key", "em_description",
        "odpd_workstation", "odpd_wc_key", "odpd_st_key", "st_id", "st_description",
        "odpd_lot_number", "odpd_oc_key", "oc_description",
        "odpd_cm_key", "cm_description", "odpd_sm_key", "sm_description",
        "odpd_is_overtime", "odpd_stpo_key", "source_connection"
    ]
    conn_params = create_connection_params_from_airflow("pg-ssg")

    success = upsert_data_via_postgres(
        agg_records, "odp_hourly_summary", key_columns, conn_params
    )

    status = "success" if success else "failed"
    log_etl(process_id, "upsert", status, len(agg_records))
    return f"Upsert {status} with {len(agg_records)} records"

def vacuum(**context):
    """
    Run VACUUM ANALYZE outside of transaction safely.
    """
    import psycopg2
    from urllib.parse import quote_plus
    from airflow.hooks.base import BaseHook

    process_id = f"hourly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"

    try:
        # Use direct psycopg2 (since SQLAlchemy auto-wraps transactions)
        conn_info = BaseHook.get_connection("pg-ssg")
        password = quote_plus(conn_info.password or "")
        conn = psycopg2.connect(
            host=conn_info.host,
            port=conn_info.port,
            database=conn_info.schema,
            user=conn_info.login,
            password=password,
        )

        # VACUUM must be autocommit
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("VACUUM ANALYZE odp_hourly_summary;")
        cursor.close()
        conn.close()

        log_etl(process_id, "vacuum", "success")
        return "✅ Vacuum completed successfully"

    except Exception as e:
        log_etl(process_id, "vacuum", "failed", error_message=str(e))
        print(f"❌ Vacuum failed: {e}")
        return f"❌ Vacuum failed: {e}"





def summary(**context):
    process_id = f"hourly_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
    report = f"""
    ================================
    📌 HOURLY ETL SUMMARY
    -------------------------------
    ✅ Extract
    ✅ Aggregate
    ✅ Upsert
    ✅ Vacuum
    Run: {datetime.now(PKT)}
    ================================
    """
    log_etl(process_id, "summary", "success")
    logger.info(report)
    return report


# ---------------- DAG ---------------- #
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 3, tzinfo=PKT),
    # "start_date":datetime.now(PKT) - timedelta(minutes=10),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "hourly_hanger_line_production_upsert",
    default_args=default_args,
    description="Hourly hanger line ETL pipeline with logging",
    # schedule="10 * * * *",
    schedule="10 8-23,0-1 * * 1-6",  # ✅ Every 10 min, 8AM–2AM, Mon–Sat
    catchup=False,
    tags=["ssg", "hangerline", "data", "hourly"],
    max_active_runs=1,
) as dag:

    create_tables_task = PythonOperator(task_id="create_tables", python_callable=create_tables)
    extract_task = PythonOperator(task_id="extract", python_callable=extract)
    aggregate_task = PythonOperator(task_id="aggregate", python_callable=aggregate)
    upsert_task = PythonOperator(task_id="upsert", python_callable=upsert)
    vacuum_task = PythonOperator(task_id="vacuum", python_callable=vacuum)
    summary_task = PythonOperator(task_id="summary", python_callable=summary)

    create_tables_task >> extract_task >> aggregate_task >> upsert_task >> vacuum_task >> summary_task
