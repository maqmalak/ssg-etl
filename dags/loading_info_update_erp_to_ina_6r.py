"""
ETL DAG: MSSQL → MSSQL Table Sync (Lowercase + Upsert Mode)
------------------------------------------------------------------
✅ MSSQL + MSSQL connection checks with retry
✅ Dynamically lists tables from MSSQL
✅ Converts table names & fields to lowercase
✅ Upserts MSSQL tables each sync
✅ Loads data in safe chunks with type inference
✅ Logs progress & pushes XCom for summary
✅ Runs every 30 min, Mon-Sat (8 AM-2 AM PKT)
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
    "start_date": datetime.now(PKT) - timedelta(minutes=30),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(hours=1),
    "catchup": False,
}

# Project imports
sys.path.append(os.path.join(os.path.dirname(__file__), '..'))

# Load SQL script for creating views
with open(os.path.join(os.path.dirname(__file__), '..', 'scripts', 'SQL', 'create_view_ssg.sql'), 'r') as f:
    create_view_ssg = f.read()

from sqlalchemy import inspect

MSSQL_SOURCE_CONN_ID = "db-erp"
MSSQL_TARGET_CONN_ID = "SSG_INA"
INCLUDED_VIEWS = ["ina_planinfo", "ina_operationinfo", "ina_employee", "ina_operationinfo7a"]  # ✅ specify views to sync (case-sensitive)



# -----------------source and target same list use for full table sync with pk ---------------- #
TARGETS = [
    {"table": "ina_planinfo", "pk": ["id"]},
    {"table": "ina_operationinfo", "pk": ["id"]},
    {"table": "ina_employee", "pk": ["id"]},
    {"table": "ina_operationinfo7a", "pk": ["id"]},


]


# INCLUDED_TABLES = ["Coa31", "Employees", "DefDepartments", "OperationBreakDown", "OperationBreakDown_Det"]
CHUNK_SIZE = 50  # ✅ batch size for memory-safe processing and SQL Server parameter limit


# ---------------------------------------------------------------- #
# RETRY DECORATOR (for MSSQL checks)
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



def infer_column_types(df: pd.DataFrame) -> pd.DataFrame:
    """Auto-cast numeric and datetime columns for cleaner MSSQL schema."""
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
    dag_id="loading_info_update_erp_to_ina_6r",
    default_args=default_args,
    schedule='5,25 8-23 * * 1-6',  # Run every hour
    tags=["Loading", "erptoina", "ssg", "sync"],
    max_active_runs=1,
)
def data_sync_mssql_to_mssql():
    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    # ---------------- SOURCE CHECK ---------------- #
    @task
    @retry_on_exception(label="MSSQL Source Check")
    def source_check(ti=None):
        conn = BaseHook.get_connection(MSSQL_SOURCE_CONN_ID)
        conn_str = build_mssql_conn_str(conn)
        with pyodbc.connect(conn_str, timeout=30) as c:
            c.cursor().execute("SELECT 1")
        msg = "✅ MSSQL Source reachable"
        logger.info(msg)
        ti.xcom_push(key="source_check", value=msg)
        return msg

    # ---------------- TARGET CHECK ---------------- #
    @task
    @retry_on_exception(label="MSSQL Target Check")
    def target_check(ti=None):
        conn = BaseHook.get_connection(MSSQL_TARGET_CONN_ID)
        conn_str = build_mssql_conn_str(conn)
        with pyodbc.connect(conn_str, timeout=30) as c:
            c.cursor().execute("SELECT 1")
        msg = "✅ MSSQL Target reachable"
        logger.info(msg)
        ti.xcom_push(key="target_check", value=msg)
        return msg


# Function to create the transactions table
    # ---------------- LIST MSSQL TABLES ---------------- #
    @task
    def create_views() -> list:
        conn = BaseHook.get_connection(MSSQL_SOURCE_CONN_ID)
        logger.info(f"MSSQL Connection - Host: {conn.host}, Schema(Database): {conn.schema}, Login: {conn.login}")
        conn_str = build_mssql_conn_str(conn)
        logger.info(f"Full connection string (without password): DRIVER={{FreeTDS}};SERVER={conn.host};PORT=1433;DATABASE={conn.schema};UID={conn.login};...")
        
        with pyodbc.connect(conn_str, timeout=60) as c:
          
            cur = c.cursor()
            # Split SQL script on GO statements and execute each batch
            batches = [batch.strip() for batch in create_view_ssg.split('GO') if batch.strip()]
            for batch in batches:
                logger.info(f"Executing SQL batch: {batch[:100]}...")
                cur.execute(batch)
            tables = INCLUDED_VIEWS
        logger.info(f"✅ Found {len(tables)} tables to sync: {tables}")
        return tables


    # ---------------- LIST MSSQL TABLES ---------------- #
    @task
    def list_tables() -> list:
        conn = BaseHook.get_connection(MSSQL_SOURCE_CONN_ID)
        logger.info(f"MSSQL Connection - Host: {conn.host}, Schema(Database): {conn.schema}, Login: {conn.login}")
        conn_str = build_mssql_conn_str(conn)
        logger.info(f"Full connection string (without password): DRIVER={{FreeTDS}};SERVER={conn.host};PORT=1433;DATABASE={conn.schema};UID={conn.login};...")
        
        with pyodbc.connect(conn_str, timeout=60) as c:
          
            cur = c.cursor()
            query = f"""
                SELECT TABLE_NAME FROM INFORMATION_SCHEMA.VIEWS
                WHERE TABLE_NAME IN ({','.join([f"'{t}'" for t in INCLUDED_VIEWS])})
                AND TABLE_SCHEMA IN ('dbo') ;
            """
            logger.info(f"query: {query}")
            cur.execute(query)
            tables = [row[0] for row in cur.fetchall()]
        logger.info(f"✅ Found {len(tables)} tables to sync: {tables}")
        return tables

    # ---------------- TABLE DATA LOAD ---------------- #
    @task
    @retry_on_exception(label="MSSQL Data Load")
    def load_table(table_name: str, ti=None):
        """Load MSSQL → MSSQL with lowercase normalization & upsert mode."""
        source_conn = BaseHook.get_connection(MSSQL_SOURCE_CONN_ID)
        source_conn_str = build_mssql_conn_str(source_conn)
        target_conn = BaseHook.get_connection(MSSQL_TARGET_CONN_ID)
        target_conn_str = build_mssql_conn_str(target_conn)
        total_rows = 0
        target_table = table_name.lower()  # ✅ lowercase table name

        # Get primary key for this table
        pk = next((t['pk'] for t in TARGETS if t['table'] == table_name.lower()), None)
        if pk:
            pk = [k.lower() for k in pk]

        # Check if the source table exists and has data
        try:
            with pyodbc.connect(source_conn_str, timeout=30) as c:
                cur = c.cursor()
                check_query = f"""
                    SELECT COUNT(*)
                    FROM dbo.{table_name} ;
                 """
                logger.info(f"Checking source existence: {check_query} ")
                cur.execute(check_query)
                source_has_data = cur.fetchone()[0] > 0

                if not source_has_data:
                    logger.info(f"⚠️ Table/view {table_name} has no data in source schema, skipping")
                    msg = f"⚠️ {table_name}: skipped (no data in source)"
                    ti.xcom_push(key=f"{target_table}_load", value=msg)
                    return msg
        except pyodbc.ProgrammingError as e:
            if 'Invalid object name' in str(e):
                logger.info(f"⚠️ Table/view {table_name} does not exist in source schema, skipping")
                msg = f"⚠️ {table_name}: skipped (not found in source)"
            else:
                logger.error(f"❌ Database error checking {table_name}: {e}")
                msg = f"❌ {table_name}: failed (database error)"
            ti.xcom_push(key=f"{target_table}_load", value=msg)
            return msg

        # Check if target table exists
        with pyodbc.connect(target_conn_str) as conn:
            cur = conn.cursor()
            cur.execute("SELECT 1 FROM sys.tables WHERE name = ? AND schema_id = SCHEMA_ID('dbo')", (target_table,))
            target_table_exists = cur.fetchone() is not None

        logger.info(f"🚀 Starting upsert for table: {target_table}")

        with pyodbc.connect(source_conn_str) as mssql_conn:
            qualified_table_name = f"dbo.{table_name}"
            chunk_iter = pd.read_sql(f"SELECT * FROM {qualified_table_name}", mssql_conn, chunksize=CHUNK_SIZE)

            # Get first chunk to determine dtypes
            try:
                first_chunk = next(chunk_iter)
            except StopIteration:
                logger.info(f"No data for {table_name}, skipping")
                msg = f"⚠️ {table_name}: no data"
                ti.xcom_push(key=f"{target_table}_load", value=msg)
                return msg

            # Convert columns to lowercase
            first_chunk.columns = [c.lower() for c in first_chunk.columns]

            # Infer types
            first_chunk = infer_column_types(first_chunk)

            # Store dtypes for consistent type conversion
            dtypes = first_chunk.dtypes.copy()

            if not target_table_exists:
                # Create table SQL
                create_sql = f"CREATE TABLE dbo.{target_table} ("
                for col in first_chunk.columns:
                    dtype = first_chunk[col].dtype
                    if col.lower() == 'id':
                        sql_type = 'NVARCHAR(255) PRIMARY KEY'
                    elif dtype == 'int64':
                        sql_type = 'BIGINT'
                    elif dtype == 'float64':
                        sql_type = 'FLOAT'
                    elif dtype == 'object':
                        sql_type = 'NVARCHAR(255)'
                    elif pd.api.types.is_datetime64_any_dtype(first_chunk[col]):
                        sql_type = 'DATETIME'
                    else:
                        sql_type = 'NVARCHAR(255)'
                    create_sql += f"[{col}] {sql_type}, "
                create_sql = create_sql.rstrip(', ') + ")"

                with pyodbc.connect(target_conn_str) as conn:
                    cur = conn.cursor()
                    cur.execute(create_sql)
                    conn.commit()

                logger.info(f"Created table dbo.{target_table}")

            # Process the first chunk
            # Convert to records
            records = []
            for _, row in first_chunk.iterrows():
                record = {}
                for col in first_chunk.columns:
                    value = row[col]
                    if pd.isna(value):
                        record[col] = None
                    else:
                        record[col] = value
                # Skip if primary key is NULL
                if pk and any(record.get(k) is None for k in pk):
                    continue
                records.append(record)

            # Upsert the records
            if pk:
                columns = list(records[0].keys())
                quoted_cols = ", ".join([f"[{c}]" for c in columns])
                pk_cols = ", ".join([f"T.[{k}]" for k in pk])
                on_clause = " AND ".join([f"T.[{k}] = S.[{k}]" for k in pk])
                update_set = ", ".join([f"T.[{c}] = S.[{c}]" for c in columns if c not in pk])
                insert_cols = ", ".join([f"[{c}]" for c in columns])
                insert_values = ", ".join([f"S.[{c}]" for c in columns])

                merge_sql = f"""
                    MERGE dbo.{target_table} AS T
                    USING (VALUES {', '.join(['(' + ', '.join(['?'] * len(columns)) + ')'] * len(records))})
                        AS S ({quoted_cols})
                    ON {on_clause}
                    WHEN MATCHED THEN
                        UPDATE SET {update_set}
                    WHEN NOT MATCHED THEN
                        INSERT ({insert_cols})
                        VALUES ({insert_values});
                """

                # Flatten params
                params = []
                for rec in records:
                    params.extend([rec[c] for c in columns])

                with pyodbc.connect(target_conn_str, autocommit=False) as target_conn:
                    cur = target_conn.cursor()
                    try:
                        cur.execute(merge_sql, params)
                        target_conn.commit()
                    except Exception as e:
                        target_conn.rollback()
                        logger.error(f"❌ Merge failed for {target_table}: {e}")
                        msg = f"❌ {target_table}: upsert failed for first chunk"
                        ti.xcom_push(key=f"{target_table}_load", value=msg)
                        return msg

            total_rows += len(first_chunk)

            for chunk in chunk_iter:
                # ✅ Convert all columns to lowercase
                chunk.columns = [c.lower() for c in chunk.columns]

                # ✅ Handle 'NaT' strings before type conversion (replace with None for NULL)
                for col in chunk.columns:
                    chunk[col] = chunk[col].replace('NaT', None)

                # ✅ Infer data types
                chunk = infer_column_types(chunk)

                # ✅ Apply consistent types based on first chunk
                for col in chunk.columns:
                    if col in dtypes:
                        if dtypes[col] == 'int64' or str(dtypes[col]) == 'Int64':
                            chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype('Int64')
                        elif dtypes[col] == 'float64':
                            chunk[col] = pd.to_numeric(chunk[col], errors='coerce').astype('float64')
                        elif pd.api.types.is_datetime64_any_dtype(dtypes[col]):
                            chunk[col] = pd.to_datetime(chunk[col], errors='coerce')

                # ✅ Handle NaT values in datetime columns (replace with None for NULL)
                for col in chunk.columns:
                    if pd.api.types.is_datetime64_any_dtype(chunk[col]):
                        # Replace NaT with None explicitly
                        chunk[col] = chunk[col].apply(lambda x: None if pd.isna(x) else x)

                # Convert to dict records, ensuring NaT values become None
                records = []
                for _, row in chunk.iterrows():
                    record = {}
                    for col in chunk.columns:
                        value = row[col]
                        if pd.isna(value):
                            record[col] = None
                        else:
                            record[col] = value
                    # Skip if primary key is NULL
                    if pk and any(record.get(k) is None for k in pk):
                        continue
                    records.append(record)

                # Upsert the chunk using MSSQL MERGE
                if pk:
                    columns = list(records[0].keys())
                    quoted_cols = ", ".join([f"[{c}]" for c in columns])
                    pk_cols = ", ".join([f"T.[{k}]" for k in pk])
                    on_clause = " AND ".join([f"T.[{k}] = S.[{k}]" for k in pk])
                    update_set = ", ".join([f"T.[{c}] = S.[{c}]" for c in columns if c not in pk])
                    insert_cols = ", ".join([f"[{c}]" for c in columns])
                    insert_values = ", ".join([f"S.[{c}]" for c in columns])

                    merge_sql = f"""
                        MERGE dbo.{target_table} AS T
                        USING (VALUES {', '.join(['(' + ', '.join(['?'] * len(columns)) + ')'] * len(records))})
                            AS S ({quoted_cols})
                        ON {on_clause}
                        WHEN MATCHED THEN
                            UPDATE SET {update_set}
                        WHEN NOT MATCHED THEN
                            INSERT ({insert_cols})
                            VALUES ({insert_values});
                    """

                    # Flatten params
                    params = []
                    for rec in records:
                        params.extend([rec[c] for c in columns])

                    with pyodbc.connect(target_conn_str, autocommit=False) as target_conn:
                        cur = target_conn.cursor()
                        try:
                            cur.execute(merge_sql, params)
                            target_conn.commit()
                        except Exception as e:
                            target_conn.rollback()
                            logger.error(f"❌ Merge failed for {target_table}: {e}")
                            msg = f"❌ {target_table}: upsert failed for chunk"
                            ti.xcom_push(key=f"{target_table}_load", value=msg)
                            return msg

                total_rows += len(chunk)
                logger.info(f"📦 {target_table}: upserted {len(chunk)} rows (total {total_rows})")

        msg = f"✅ {target_table}: upserted table with {total_rows} rows"
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
    # views = create_views()
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
    [src >>  tgt >>  tables] >> tg 
    tg >> end


dag = data_sync_mssql_to_mssql()
