"""
Optimized Hourly DAG for aggregating hanger line data and upserting to production tables
"""

import time, logging, os, sys
from datetime import datetime, timedelta
from collections import defaultdict
from urllib.parse import quote_plus

import pandas as pd
import psycopg2
from sqlalchemy import create_engine
from pendulum import timezone

from airflow import DAG
from airflow.decorators import task
from airflow.hooks.base import BaseHook

import sys
import os
# sys.path.append(os.path.dirname(os.path.abspath(__file__)))
# from dags.source_target_conn import SOURCE_HANGER_LANE
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Add scripts to path
# scripts_path = os.path.join(os.path.dirname(__file__), '..', 'scripts')
# sys.path.append(os.path.abspath(scripts_path))

# Import helpers
from upsert_utils import upsert_data_via_postgres, create_connection_params_from_airflow
from scripts.create_table_hourly import create_hourly_table_if_not_exists

# Timezone
PKT = timezone("Asia/Karachi")

# Logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
if not logger.handlers:
    handler = logging.StreamHandler()
    handler.setFormatter(logging.Formatter('%(asctime)s - %(levelname)s - %(message)s'))
    logger.addHandler(handler)


# ---------------- DATABASE CONNECTION ---------------- #
def get_database_connection():
    """Get psycopg2 connection"""
    try:
        try:
            conn = BaseHook.get_connection("pg-ssg")
            return psycopg2.connect(
                host=conn.host,
                port=conn.port or 5432,
                database=conn.schema,
                user=conn.login,
                password=conn.password,  # no encoding for psycopg2
                connect_timeout=30
            )
        except Exception as e:
            logger.warning(f"Airflow connection not found, using env vars: {e}")
            return psycopg2.connect(
                host=os.getenv("POSTGRES_HOST", "172.16.7.6"),
                port=os.getenv("POSTGRES_PORT", "5432"),
                database=os.getenv("POSTGRES_DB", "ssg"),
                user=os.getenv("POSTGRES_USER", "postgres"),
                password=os.getenv("POSTGRES_PASSWORD", "P@kistan12"),
                connect_timeout=30
            )
    except Exception as e:
        logger.error(f"DB connection failed: {e}")
        raise


# ---------------- LOGGING HELPERS ---------------- #
def log_etl_extraction(process_log_id, source_connection, saved_count, start_time, status="success", error=None):
    """Insert into etl_extract_hourly_log"""
    try:
        conn = get_database_connection()
        cur = conn.cursor()
        # Use PKT timezone for consistent timestamp handling
        now = datetime.now(PKT)
        cur.execute("""
            INSERT INTO etl_extract_hourly_log
            (processlogid, source_connection, saved_count, starttime, endtime,
             lastextractdatetime, success, status, errormessage)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, (
            process_log_id,
            source_connection,
            saved_count,
            datetime.fromtimestamp(start_time),
            now,
            now,
            status == "success",
            status,
            error
        ))
        conn.commit()
        cur.close()
        conn.close()
        logger.info(f"ETL log recorded: {process_log_id} ({status})")
    except Exception as e:
        logger.error(f"Log insert failed: {e}")


# ---------------- CORE LOGIC ---------------- #
def fetch_recent_source_data(hours_back=1):
    """Fetch recent rows from operator_daily_performance"""
    start = time.time()
    query = f"""
        SELECT * FROM operator_daily_performance
        WHERE created_at >= NOW() - INTERVAL '{hours_back} hours'
    """
    try:
        conn = get_database_connection()
        df = pd.read_sql_query(query, conn)
        conn.close()
        logger.info(f"Fetched {len(df)} records in {time.time()-start:.2f}s")
        return df
    except Exception as e:
        logger.error(f"Fetch failed: {e}")
        return pd.DataFrame()


def perform_aggregations(df: pd.DataFrame):
    """Return aggregated DataFrames"""
    if df.empty:
        return {"odp_hourly_employee": pd.DataFrame(), "odp_hourly_summary": pd.DataFrame()}

    df.columns = df.columns.str.lower()

    # --- Employee-level aggregation ---
    emp_group = [
        "odp_date","shift","odp_em_key","em_firstname","em_lastname",
        "odpd_workstation","odpd_wc_key","odpd_st_key","st_id","st_description",
        "odpd_lot_number","odpd_oc_key","oc_description","odpd_cm_key",
        "cm_description","odpd_sm_key","sm_description","odpd_is_overtime",
        "odpd_stpo_key","source_connection"
    ]
    agg_emp = df.groupby([c for c in emp_group if c in df.columns]).agg(
        loading_qty=("loading_qty","sum"),
        unloading_qty=("unloading_qty","sum"),
        record_count=("odpd_key","count")
    ).reset_index()
    # Use PKT timezone for consistent timestamp handling
    agg_emp["hour_timestamp"] = pd.to_datetime(df["created_at"]).dt.tz_localize(None).dt.floor("H").iloc[0]
    agg_emp["created_at"] = datetime.now(PKT)

    # --- Summary aggregation ---
    agg_sum = df.groupby([
        pd.to_datetime(df["created_at"]).dt.floor("H"),
        "odp_date","shift","st_id","st_description","oc_description","source_connection"
    ]).agg(
        total_quantity=("odpd_quantity","sum"),
        total_loading_qty=("loading_qty","sum"),
        total_unloading_qty=("unloading_qty","sum"),
        avg_actual_time=("odpd_actual_time","mean"),
        total_employees=("odp_em_key","nunique")
    ).reset_index().rename(columns={ "created_at":"hour_timestamp" })
    # Use PKT timezone for consistent timestamp handling
    agg_sum["created_at"] = datetime.now(PKT)

    return {"odp_hourly_employee": agg_emp, "odp_hourly_summary": agg_sum}


def upsert_table(table_name, key_columns, df: pd.DataFrame):
    """Generic upsert"""
    if df.empty:
        return f"No data for {table_name}"

    data = df.where(pd.notnull(df), None).to_dict("records")
    params = create_connection_params_from_airflow("pg-ssg")

    ok = upsert_data_via_postgres(data, table_name, key_columns, params)
    return f"Upserted {len(data)} rows → {table_name}" if ok else f"Upsert failed → {table_name}"


# ---------------- AIRFLOW DAG ---------------- #
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 10, 3, tzinfo=PKT),
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

with DAG(
    "hourly_hangerline_summary",
    default_args=default_args,
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["ssg","hangerline","summary","hourly"]
) as dag:

    @task
    def start():
        logger.info("DAG started")

    @task
    def create_tables():
        conn = BaseHook.get_connection("pg-ssg")
        uri = f"postgresql://{conn.login}:{quote_plus(conn.password)}@{conn.host}:{conn.port}/{conn.schema}"
        engine = create_engine(uri)
        create_hourly_table_if_not_exists(engine)
        engine.dispose()
        logger.info("Tables ensured")

    @task
    def process():
        start_time = time.time()
        # Use PKT timezone for consistent process ID generation
        proc_id = f"hourly_hanger_line_{datetime.now(PKT).strftime('%Y%m%d_%H%M%S')}"

        df = fetch_recent_source_data()
        if df.empty:
            log_etl_extraction(proc_id,"pg-ssg",0,start_time,"no_data","No source data")
            return "No source data"

        aggs = perform_aggregations(df)

        # Define keys
        keys = {
            "odp_hourly_employee":["hour_timestamp","odp_date","shift","odp_em_key","odpd_stpo_key","source_connection"],
            "odp_hourly_summary":["hour_timestamp","odp_date","shift","st_id","oc_description","source_connection"],
        }

        results = []
        for table, agg_df in aggs.items():
            result = upsert_table(table, keys[table], agg_df)
            results.append(result)

        log_etl_extraction(proc_id,"pg-ssg",len(df),start_time,"success")
        return results

    @task
    def end():
        logger.info("DAG finished")

    start() >> create_tables() >> process() >> end()
