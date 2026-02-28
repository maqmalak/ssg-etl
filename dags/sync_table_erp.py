# ...existing code...
import re
import sys
import os
import io
import time
import random
import logging
from functools import wraps
from datetime import datetime, timedelta
from typing import List, Optional

import pandas as pd
import pendulum
import pyodbc
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError

from airflow.decorators import dag, task
from airflow.hooks.base import BaseHook
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# ---------------------------------------------------------------- #
# CONFIG
# ---------------------------------------------------------------- #
PKT = pendulum.timezone("Asia/Karachi")
DEFAULT_START = datetime(2025, 11, 1, 12, 10, tzinfo=PKT)

MSSQL_CONN_ID = "SilverStr"
POSTGRES_CONN_ID = "pg-ssg"
INCLUDED_TABLES = ["DefDepartments", "DefDesignations"]
CHUNK_SIZE = 10000

# ---------------------------------------------------------------- #
# UTILITIES
# ---------------------------------------------------------------- #
def retry_on_exception(max_retries: int = 3, delay: float = 2.0, backoff: float = 2.0, label: Optional[str] = None):
    """Simple retry decorator with exponential backoff and small jitter."""
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            attempts = 0
            wait = delay
            ctx = label or func.__name__
            while True:
                try:
                    return func(*args, **kwargs)
                except Exception as exc:
                    attempts += 1
                    if attempts > max_retries:
                        logger.exception("%s failed after %d retries", ctx, max_retries)
                        raise
                    sleep_for = wait + random.random() * 0.5
                    logger.warning("%s failed (attempt %d/%d): %s; retrying in %.1fs", ctx, attempts, max_retries, exc, sleep_for)
                    time.sleep(sleep_for)
                    wait *= backoff
        return wrapper
    return decorator

def build_mssql_conn_str(conn) -> str:
    return (
        f"DRIVER={{FreeTDS}};SERVER={conn.host};PORT=1433;"
        f"DATABASE={conn.schema};UID={conn.login};PWD={conn.password};"
        "TDS_Version=7.0;Connect Timeout=30;Login Timeout=30;"
    )

def get_postgres_engine():
    """Get SQLAlchemy engine via Airflow connection or fallback to env."""
    from urllib.parse import quote_plus
    try:
        c = BaseHook.get_connection(POSTGRES_CONN_ID)
        uri = f"postgresql+psycopg2://{c.login}:{quote_plus(c.password or '')}@{c.host}:{c.port}/{c.schema}"
        logger.info("[PG] Using Airflow connection")
    except Exception:
        pg_user = os.environ.get("PG_USER", "postgres")
        pg_pass = quote_plus(os.environ.get("PG_PASS", ""))
        pg_host = os.environ.get("PG_HOST", "127.0.0.1")
        pg_port = os.environ.get("PG_PORT", "5432")
        pg_db = os.environ.get("PG_DB", "ssg")
        uri = f"postgresql+psycopg2://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}"
        logger.warning("[PG] Falling back to env-based connection")
    return create_engine(uri, pool_size=5, max_overflow=10, pool_pre_ping=True, pool_recycle=3600, echo=False)

def _safe_table_name(name: str) -> str:
    """Allow only alnum + underscore; lower-case result."""
    if not re.match(r"^[A-Za-z0-9_]+$", name):
        raise ValueError("Invalid table name")
    return name.lower()

def infer_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """Lightweight inference: convert numeric-like and datetime-like columns."""
    for col in df.columns:
        ser = df[col]
        if ser.dtype == object:
            # numeric
            try:
                converted = pd.to_numeric(ser, errors="coerce")
                if converted.notna().sum() > len(ser) * 0.5:
                    df[col] = converted
                    continue
            except Exception:
                pass
            # datetime
            try:
                converted = pd.to_datetime(ser, errors="coerce", infer_datetime_format=True)
                if converted.notna().sum() > len(ser) * 0.5:
                    df[col] = converted
            except Exception:
                pass
    return df

def _copy_df_to_postgres(engine, df: pd.DataFrame, table: str) -> bool:
    """Try fast COPY via raw connection (psycopg2). Returns True on success."""
    if df.empty:
        return True
    try:
        raw = engine.raw_connection()
        cur = raw.cursor()
        buf = io.StringIO()
        df.to_csv(buf, index=False, header=False, sep="\t", na_rep="\\N")
        buf.seek(0)
        cols = tuple(df.columns)
        cur.copy_from(buf, table, sep="\t", null="\\N", columns=cols)
        raw.commit()
        cur.close()
        raw.close()
        return True
    except Exception as e:
        logger.debug("Fast COPY failed; falling back to to_sql: %s", e)
        try:
            raw.rollback()
            raw.close()
        except Exception:
            pass
        return False

# ---------------------------------------------------------------- #
# DAG
# ---------------------------------------------------------------- #
@dag(
    dag_id="data_sync_mssql_to_postgres_best_practice",
    start_date=DEFAULT_START,
    schedule="*/30 8-23,0-1 * * 1-6",
    catchup=False,
    max_active_runs=1,
    tags=["mssql", "postgres", "ssg", "sync"],
)
def data_sync_mssql_to_postgres():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    @task
    @retry_on_exception(label="MSSQL Source Check")
    def source_check():
        conn = BaseHook.get_connection(MSSQL_CONN_ID)
        conn_str = build_mssql_conn_str(conn)
        with pyodbc.connect(conn_str, timeout=30) as c:
            c.cursor().execute("SELECT 1")
        msg = "✅ MSSQL Source reachable"
        logger.info(msg)
        return msg

    @task
    @retry_on_exception(label="PostgreSQL Target Check")
    def target_check():
        engine = get_postgres_engine()
        with engine.connect().execution_options(timeout=30) as conn:
            conn.execute(text("SELECT 1"))
        msg = "✅ PostgreSQL Target reachable"
        logger.info(msg)
        return msg

    @task
    def list_tables() -> List[str]:
        """Return list of tables to sync (can be replaced by discovery query)."""
        tables = [t for t in INCLUDED_TABLES]
        logger.info("Discovered tables to sync: %s", tables)
        return tables

    @task
    @retry_on_exception(label="PostgreSQL Data Load")
    def load_table(table_name: str):
        """Per-table load. Designed for mapping via .expand"""
        try:
            target_table = _safe_table_name(table_name)
        except ValueError as e:
            msg = f"⚠️ Skipping invalid table '{table_name}': {e}"
            logger.warning(msg)
            return msg

        conn = BaseHook.get_connection(MSSQL_CONN_ID)
        conn_str = build_mssql_conn_str(conn)
        engine = get_postgres_engine()
        total = 0
        logger.info("Starting sync for table %s -> %s", table_name, target_table)

        try:
            with pyodbc.connect(conn_str, timeout=60) as mssql_conn:
                for chunk in pd.read_sql(f"SELECT * FROM {table_name}", mssql_conn, chunksize=CHUNK_SIZE):
                    chunk.columns = [c.lower() for c in chunk.columns]
                    chunk = infer_column_types(chunk)

                    if total == 0:
                        try:
                            chunk.iloc[0:0].to_sql(name=target_table, con=engine, if_exists="replace", index=False)
                        except SQLAlchemyError:
                            logger.debug("Could not create empty schema for %s; will rely on to_sql inserts", target_table)

                    copied = _copy_df_to_postgres(engine, chunk, target_table)
                    if not copied:
                        mode = "append" if total > 0 else "replace"
                        chunk.to_sql(name=target_table, con=engine, if_exists=mode, index=False, method="multi", chunksize=CHUNK_SIZE)

                    total += len(chunk)
                    logger.info("%s: loaded %d rows (total %d)", target_table, len(chunk), total)

            return f"✅ {target_table}: replaced table with {total} rows"
        except Exception as e:
            logger.exception("Failed to load table %s: %s", table_name, e)
            return f"❌ {target_table}: failed to load: {e}"

    @task(trigger_rule=TriggerRule.ALL_DONE)
    def summarize(results: List[str]):
        summary = {"success": 0, "fail": 0, "skipped": 0}
        for r in results or []:
            if not r:
                summary["skipped"] += 1
                continue
            if r.startswith("✅"):
                summary["success"] += 1
            elif r.startswith("⚠️"):
                summary["skipped"] += 1
            else:
                summary["fail"] += 1
            logger.info(r)
        logger.info("Summary: %s", summary)
        return summary

    # pipeline ordering
    src = source_check()
    tgt = target_check()
    tables = list_tables()

    load_results = load_table.expand(table_name=tables)
    summary = summarize(load_results)

    start >> src >> tgt >> tables >> load_results >> summary >> end

dag = data_sync_mssql_to_postgres()
