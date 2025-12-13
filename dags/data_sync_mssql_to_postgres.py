"""
ETL DAG: MSSQL → PostgreSQL Table Sync (Lowercase + Replace Mode)
------------------------------------------------------------------
✅ MSSQL + PostgreSQL connection checks with retry
✅ Dynamically lists tables from MSSQL
✅ Converts table names & fields to lowercase
✅ Replaces PostgreSQL tables each sync
✅ Loads data in safe chunks with type inference
✅ Logs progress & pushes XCom for summary
✅ Runs every 30 min, Mon–Sat (8 AM–2 AM PKT)
"""

from __future__ import annotations
import sys, os, logging, time, random
from datetime import datetime, timedelta
import pandas as pd
import pendulum, pyodbc
from airflow.decorators import dag, task
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.task_group import TaskGroup
from sqlalchemy import create_engine, text
from functools import wraps

# ---------------------------------------------------------------- #
# CONFIGURATION
# ---------------------------------------------------------------- #
PKT = pendulum.timezone("Asia/Karachi")
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    # "start_date": datetime(2025, 11, 5, 17, 0, tzinfo=PKT),
    "start_date":datetime.now(PKT) - timedelta(minutes=30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "catchup": False,
}

MSSQL_CONN_ID = "SilverStr"
POSTGRES_CONN_ID = "pg-ssg"
INCLUDED_VIEWS = ["StyleBasicInformation","LoadingInformation","OperationInformation","hangerline_emp"]


# INCLUDED_TABLES = ["Coa31", "Employees", "DefDepartments", "OperationBreakDown", "OperationBreakDown_Det"]
CHUNK_SIZE = 10000  # ✅ batch size for memory-safe processing


# ---------------------------------------------------------------- #
# RETRY DECORATOR (for MSSQL & PostgreSQL checks)
# ---------------------------------------------------------------- #
def retry_on_exception(max_retries=2, delay=3, backoff=1.5, label=None):
    """Retry function with exponential backoff and jitter"""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            retries = 0
            wait = delay
            context = label or func.__name__
            while retries < max_retries:
                try:
                    return func(*args, **kwargs)
                except Exception as e:
                    retries += 1
                    if retries >= max_retries:
                        logger.error(f"❌ {context} failed after {max_retries} retries: {e}")
                        raise
                    logger.warning(f"⚠️ {context} retry {retries}/{max_retries} in {wait:.1f}s ({e})")
                    time.sleep(wait + random.uniform(0, 1))
                    wait *= backoff
        return wrapper
    return decorator


# ---------------------------------------------------------------- #
# DB UTILITIES
# ---------------------------------------------------------------- #
def build_mssql_conn_str(conn):
    return (
        f"DRIVER={{FreeTDS}};SERVER={conn.host};PORT=1433;"
        f"DATABASE={conn.schema};UID={conn.login};PWD={conn.password};"
        "TDS_Version=7.0;Connect Timeout=60;Login Timeout=60;"
    )

def get_postgres_engine():
    """Return SQLAlchemy engine for PostgreSQL"""
    from urllib.parse import quote_plus
    try:
        c = BaseHook.get_connection(POSTGRES_CONN_ID)
        uri = f"postgresql://{c.login}:{quote_plus(c.password or '')}@{c.host}:{c.port}/{c.schema}"
        logger.info(f"[PG] Connected via Airflow: {c.host}/{c.schema}")
    except Exception as e:
        logger.warning(f"[PG] Airflow conn failed ({e}), using fallback")
        uri = f"postgresql://postgres:{quote_plus('P@kistan12')}@172.16.7.6:5432/ssg"

    return create_engine(uri, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600, echo=False)


def infer_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """Auto-cast numeric and datetime columns for cleaner Postgres schema."""
    for col in df.columns:
        series = df[col]
        # Convert numeric-looking strings to numbers
        if series.dtype == object:
            try:
                df[col] = pd.to_numeric(series, errors="ignore")
            except Exception:
                pass
        # Convert date-like strings to datetime
        if series.dtype == object:
            # Check if series has any non-null values before attempting conversion
            non_null_series = series.dropna()
            if not non_null_series.empty:
                # Check if the first non-null value looks like a date/time before conversion
                first_val = str(non_null_series.iloc[0]).lower()
                # Only attempt conversion if it looks like a date/time value
                if any(pattern in first_val for pattern in ['/', '-', ':', 'am', 'pm', 'gmt', 'utc']):
                    try:
                        # Use format='mixed' to prevent format inference warnings
                        df[col] = pd.to_datetime(series, errors="ignore", format='mixed')
                    except Exception:
                        # If mixed format doesn't work, use the default approach
                        try:
                            df[col] = pd.to_datetime(series, errors="ignore")
                        except Exception:
                            pass
    return df


# ---------------------------------------------------------------- #
# DAG DEFINITION
# ---------------------------------------------------------------- #
@dag(
    dag_id="data_sync_mssql_to_postgres",
    default_args=default_args,
    schedule="*/30 8-23,0-1 * * 1-6",  # Every 30 min Mon–Sat, 8AM–2AM PKT
    tags=["mssql", "postgres", "ssg", "sync"],
    max_active_runs=1,
)
def data_sync_mssql_to_postgres():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ---------------- SOURCE CHECK ---------------- #
    @task
    @retry_on_exception(label="MSSQL Source Check")
    def source_check(ti=None):
        conn = BaseHook.get_connection(MSSQL_CONN_ID)
        conn_str = build_mssql_conn_str(conn)
        with pyodbc.connect(conn_str, timeout=30) as c:
            c.cursor().execute("SELECT 1")
        msg = "✅ MSSQL Source reachable"
        logger.info(msg)
        ti.xcom_push(key="source_check", value=msg)
        return msg

    # ---------------- TARGET CHECK ---------------- #
    @task
    @retry_on_exception(label="PostgreSQL Target Check")
    def target_check(ti=None):
        engine = get_postgres_engine()
        with engine.connect().execution_options(timeout=30) as conn:
            conn.execute(text("SELECT 1"))
        msg = "✅ PostgreSQL Target reachable"
        logger.info(msg)
        ti.xcom_push(key="target_check", value=msg)
        return msg

    # ---------------- LIST MSSQL TABLES ---------------- #
    @task
    def list_tables() -> list:
        conn = BaseHook.get_connection(MSSQL_CONN_ID)
        logger.info(f"MSSQL Connection - Host: {conn.host}, Schema(Database): {conn.schema}, Login: {conn.login}")
        conn_str = build_mssql_conn_str(conn)
        logger.info(f"Full connection string (without password): DRIVER={{FreeTDS}};SERVER={conn.host};PORT=1433;DATABASE={conn.schema};UID={conn.login};...")
        
        with pyodbc.connect(conn_str, timeout=60) as c:
          
            cur = c.cursor()
            query = f"""
                SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS 
                WHERE TABLE_NAME IN ({','.join([f"'{t}'" for t in INCLUDED_VIEWS])}) 
                AND TABLE_SCHEMA = 'dbo';
            """
            logger.info(f"query: {query}")
            cur.execute(query)
            tables = [row[0] for row in cur.fetchall()]
        logger.info(f"✅ Found {len(tables)} tables to sync: {tables}")
        return tables

    # ---------------- TABLE DATA LOAD ---------------- #
    @task
    @retry_on_exception(label="PostgreSQL Data Load")
    def load_table(table_name: str, ti=None):
        """Load MSSQL → PostgreSQL with lowercase normalization & replace mode."""
        conn = BaseHook.get_connection(MSSQL_CONN_ID)
        conn_str = build_mssql_conn_str(conn)
        engine = get_postgres_engine()
        total_rows = 0
        target_table = table_name.lower()  # ✅ lowercase table name

        # Check if the table exists in the dbo schema before proceeding
        try:
            with pyodbc.connect(conn_str, timeout=30) as c:
                cur = c.cursor()
                check_query = f"""
                    SELECT COUNT(*)
                    FROM dbo.{table_name} ;
                 """
                logger.info(f"Checking existence: {check_query} ")
                cur.execute(check_query)
                table_exists = cur.fetchone()[0] > 0

                if not table_exists:
                    logger.info(f"⚠️ Table/view {table_name} has no data in dbo schema, skipping")
                    msg = f"⚠️ {table_name}: skipped (no data in dbo schema)"
                    ti.xcom_push(key=f"{target_table}_load", value=msg)
                    return msg
        except pyodbc.ProgrammingError as e:
            if 'Invalid object name' in str(e):
                logger.info(f"⚠️ Table/view {table_name} does not exist in dbo schema, skipping")
                msg = f"⚠️ {table_name}: skipped (not found in dbo schema)"
            else:
                logger.error(f"❌ Database error checking {table_name}: {e}")
                msg = f"❌ {table_name}: failed (database error)"
            ti.xcom_push(key=f"{target_table}_load", value=msg)
            return msg

        logger.info(f"🚀 Starting full replace for table: {target_table}")
        with pyodbc.connect(conn_str) as mssql_conn:
            # Use qualified table name (dbo.table_name) to ensure correct schema
            qualified_table_name = f"dbo.{table_name}"
            for chunk in pd.read_sql(f"SELECT * FROM {qualified_table_name}", mssql_conn, chunksize=CHUNK_SIZE):
                # ✅ Convert all columns to lowercase
                chunk.columns = [c.lower() for c in chunk.columns]

                # ✅ Infer data types
                chunk = infer_column_types(chunk)

                # ✅ Replace first batch, append subsequent ones
                mode = "replace" if total_rows == 0 else "append"
                chunk.to_sql(
                    name=target_table,
                    con=engine,
                    if_exists=mode,
                    index=False,
                    method="multi",
                )
                total_rows += len(chunk)
                logger.info(f"📦 {target_table}: Loaded {len(chunk)} rows (total {total_rows})")

        msg = f"✅ {target_table}: replaced table with {total_rows} rows"
        ti.xcom_push(key=f"{target_table}_load", value=msg)
        return msg

        # ---------------- SUMMARY ---------------- #
    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize_results(ti=None):
        """Aggregate and log ETL outcomes across all synced views.

        Note: load tasks run inside TaskGroup 'table_sync_group' so their runtime task_id
        is prefixed with the group's id. Build the full task_id when pulling XComs.
        """
        # get listed tables (runtime discovery) or fall back to configured list
        tables = ti.xcom_pull(task_ids="list_tables") or INCLUDED_VIEWS
        summary = {"success": 0, "fail": 0, "skipped": 0}
        report_lines = []
        separator = "─" * 80

        logger.info("\n" + separator)
        logger.info("📊 FINAL DATA SYNC SUMMARY")
        logger.info(separator)

        if not tables:
            logger.warning("⚠️ No tables were listed — nothing to summarize.")
            return summary

        tg_prefix = "table_sync_group"  # TaskGroup id used when creating load tasks

        for tbl in tables:
            # load task inside TaskGroup was created with task_id=f"load_{tbl}"
            # full runtime task id is "<group_id>.load_<tbl>"
            expected_task_id = f"{tg_prefix}.load_{tbl}"
            xcom_key = f"{str(tbl).lower()}_load"

            # try to pull XCom using the prefixed (TaskGroup) task id
            result = ti.xcom_pull(key=xcom_key, task_ids=expected_task_id)

            # fallback: try without task_ids to search globally
            if result is None:
                result = ti.xcom_pull(key=xcom_key)

            # Interpret result
            if not result:
                summary["fail"] += 1
                status_icon, status_label = "❌", "FAILED"
                msg = "No result found or task did not complete."
            elif "✅" in str(result):
                summary["success"] += 1
                status_icon, status_label = "\n" +"✅", "SUCCESS"
                msg = str(result)
            elif "⚠️" in str(result):
                summary["skipped"] += 1
                status_icon, status_label = "⚠️", "SKIPPED"
                msg = str(result)
            elif "❌" in str(result):
                summary["fail"] += 1
                status_icon, status_label = "❌", "FAILED"
                msg = str(result)
            else:
                summary["success"] += 1
                status_icon, status_label = "✅", "SUCCESS"
                msg = str(result)

            report_lines.append(f"{status_icon} {tbl:<30} → {status_label:<8} | {msg}")

        # Log formatted summary report
        logger.info("\n".join(report_lines))
        logger.info(separator)
        logger.info(f"✅ Success: {summary['success']}  | ⚠️ Skipped: {summary['skipped']}  | ❌ Failed: {summary['fail']}")
        logger.info(separator)

        ti.xcom_push(key="sync_summary", value=summary)
        return summary


    # ---------------------------------------------------------------- #
    # PIPELINE
    # ---------------------------------------------------------------- #
    src = source_check()
    tgt = target_check()
    tables = list_tables()

    # Create the load tasks but control execution based on whether the table exists
    with TaskGroup("table_sync_group", tooltip="Per-table ETL") as tg:
        results = []
        for tbl in INCLUDED_VIEWS:
            load = load_table.override(task_id=f"load_{tbl}")(tbl)
            results.append(load)
        summary = summarize_results.override(task_id="summary")()
        results >> summary

    # Ensure source_check runs before target_check as requested: source_check >> target_check
    [src,  tgt,  tables] >> tg 
    tg >> end


dag = data_sync_mssql_to_postgres()
