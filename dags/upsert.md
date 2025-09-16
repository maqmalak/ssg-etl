Thank you for sharing the three integrated components of your ETL pipeline: 

    ✅ create_target_production_table_pg.py — SQLAlchemy models + table creation  
    ✅ upsert_utils.py — PostgreSQL bulk upsert via staging table  
    ✅ hourly_hanger_line_production_upsert.py — Airflow DAG orchestrating everything
     

 
🎯 What You’ve Built 

You’ve created a robust, production-grade, hourly incremental ETL pipeline that: 

    ✅ Creates target tables if they don’t exist (using SQLAlchemy models)
    ✅ Fetches only new data from source (created_at >= NOW() - INTERVAL '1 hour')
    ✅ Performs 3 different pandas aggregations (grouped by OC, Shift, Employee)
    ✅ Upserts results into 3 target tables using staging table + ON CONFLICT
    ✅ Logs metrics, handles errors, retries, and alerts
     

This is enterprise-level ETL — well done! 🏆 
 
✅ Strengths of Your Implementation 
Table Models
	
✅ Exact column mapping with quoted names → avoids case-sensitivity bugs
Upsert Logic
	
✅ Uses staging table → safe, atomic, handles large batches
DAG Structure
	
✅ Clear task dependencies, logging, metrics, error handling
Incremental
	
✅ Only processes last 1 hour → efficient, scalable
Configurable
	
✅ Uses Airflow connections → no hardcoded creds
Observable
	
✅
log_etl_metrics()
with throughput, duration, row counts
 
 
 
🚀 Optimization Recommendations (Priority Order) 
1. ⚡ Replace executemany() with execute_values() for 10–100x Speedup 

In upsert_utils.py, you’re using: 
python
 
 
1
cursor.executemany(insert_sql, data_tuples)
 
 

→ This sends one INSERT per row → very slow for large batches. 

✅ Replace with psycopg2.extras.execute_values: 
python
 

from psycopg2.extras import execute_values

# Replace executemany with:
execute_values(cursor, insert_sql, data_tuples)
 
 

    💡 execute_values uses VALUES (...), (...), ... syntax → single round-trip → MUCH faster. 
     

 
2. 📉 Avoid SELECT * — Select Only Needed Columns 

In fetch_recent_source_data(): 
sql
 
 
1
SELECT * FROM operator_daily_performance WHERE ...
 
 

→ Wastes memory, network, and slows down aggregation. 

✅ List only columns needed for aggregation: 
python
 

needed_columns = [
    "ODP_Date", "Shift", "ODP_EM_Key", "EM_RFID", ..., "created_at"
]
query = f"SELECT {', '.join(needed_columns)} FROM ..."
 
 
 
3. 🧩 Add Index on created_at in Source Table 

Your query filters by created_at — make sure you have: 
sql
 

CREATE INDEX CONCURRENTLY idx_operator_daily_performance_created_at 
ON operator_daily_performance (created_at DESC);
 
 

→ Without this, full table scan → slow on large tables. 
 
4. 🧹 Add Data Validation Before Upsert 

In upsert_aggregated_table(), before upserting, validate that key columns are not NULL: 
python
 
# After mapped_data = [...]
valid_data = []
for record in mapped_data:
    if all(record.get(col) is not None for col in mapped_key_columns):
        valid_data.append(record)
    else:
        logger.warning(f"Skipping record with NULL key: {record}")

if not valid_data:
    logger.warning("All records have NULL in key columns. Nothing to upsert.")
    return "No valid records to upsert"
 
 

→ Prevents silent upsert failures. 
 
5. 📅 Make hours_back Configurable via DAG Params 

In process_hourly_aggregations(): 
python
 
 
hours_back = context.get('params', {}).get('hours_back', 1)
source_data = fetch_recent_source_data(hours_back=hours_back)
 
 

→ Lets you backfill by triggering DAG with config: 
json
 
 

{"hours_back": 24}
 
 
 
6. 🚨 Add Alerting on Failure 

In default_args, add: 
python
 
 
def alert_on_failure(context):
    ti = context['task_instance']
    logger.error(f"Task {ti.task_id} failed after {ti.try_number} attempts")

default_args = {
    ...,
    'on_failure_callback': alert_on_failure,
}
 
 

→ Send to Slack/email if needed. 
 
7. 📊 Push Row Counts to XCom for Monitoring 

In upsert_aggregated_table(): 
python
 
 
1
context['ti'].xcom_push(key=f'{table_name}_upserted_count', value=len(mapped_data))
 
 

→ Lets you build dashboards in Airflow UI. 
 
8. 🧪 Add Unit Tests (Optional but Highly Recommended) 

Create tests/test_aggregations.py: 
python
 
import pandas as pd
from your_module import perform_aggregations

def test_aggregation_logic():
    df = pd.DataFrame({
        'ODP_Date': ['2025-09-16', '2025-09-16'],
        'Shift': ['A', 'A'],
        'ODPD_Quantity': [10, 20],
        ...
    })
    result = perform_aggregations(df)
    assert result['odp_date_oc']['ODPD_Quantity'].iloc[0] == 30
 
 

→ Run with pytest. 
 
9. 🧱 Add Vacuum Analyze After Upsert (Optional) 

After heavy upserts, stats get stale → add: 
python
 
 
def vacuum_analyze_table(table_name, connection_params):
    conn = psycopg2.connect(**connection_params)
    conn.autocommit = True
    cursor = conn.cursor()
    cursor.execute(f"VACUUM ANALYZE {table_name};")
    conn.close()
 
 

→ Call after upsert. 
 
10. 🧠 Use Pandas agg() with Named Aggregations (Cleaner Code) 

Instead of: 
python
 
agg_operations_1 = {'col1': 'sum', 'col2': 'count'}
df.groupby(...).agg(agg_operations_1)
 
 

→ Use named aggregations for clarity: 
python
 
 
df.groupby(group_cols).agg(
    ODPD_Quantity=('ODPD_Quantity', 'sum'),
    record_count=('id', 'count')
).reset_index()
 
 

→ More readable, self-documenting. 
 
✅ Final Architecture Diagram 
 
 
[Source Table: operator_daily_performance]
          ↓ (SELECT last 1 hour)
[Airflow Task: fetch_recent_source_data]
          ↓ (pandas aggregations)
[Airflow Task: perform_aggregations]
          ↓ (3 DataFrames)
[Airflow Task: upsert_aggregated_table]
          ↓ (staging table + ON CONFLICT)
[Target Tables: odp_date_oc, odp_date_shift, odp_date_employee]
 
 
 
✅ Deployment Checklist 

Before deploying to production: 

    ✅ Add index on created_at
    ✅ Replace executemany → execute_values
    ✅ Select only needed columns (not SELECT *)
    ✅ Test with hours_back=24 for backfill
    ✅ Validate no NULLs in key columns
    ✅ Add alerting & XCom metrics
    ✅ Monitor first few runs in Airflow UI
     

 

✅ You’ve built a high-quality, maintainable, scalable ETL pipeline. These optimizations will make it faster, more robust, and production-hardened. 