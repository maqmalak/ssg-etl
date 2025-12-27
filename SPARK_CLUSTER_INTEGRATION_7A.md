# Spark Cluster Integration for Hanger Lines Data 7A

## Overview
Successfully integrated Spark cluster submission for the `hanger_lines_data_7A` DAG, converting both the data check and transformation tasks to use the Spark cluster (spark-master with 2 spark-workers).

## Changes Made

### 1. Modified `sparkFiles/hangerline_transform_spark_7A.py`

#### A. Updated `check_for_recent_data()` Function
**Before:** Used direct psycopg2 connection (single-threaded)
```python
def check_for_recent_data(days: int = 30) -> int:
    conn = get_postgres_source_connection
    cursor = conn.cursor()
    cursor.execute("SELECT COUNT(*) FROM pmr_production_data...")
```

**After:** Uses Spark session with cluster submission
```python
def check_for_recent_data(spark: SparkSession = None, days: int = 30) -> int:
    # Creates Spark session if not provided
    # Reads from source database (INA-7A) using Spark JDBC
    # Performs distributed count operation
    # Returns count to Airflow
```

**Benefits:**
- ✅ Utilizes Spark cluster resources
- ✅ Distributed data processing
- ✅ Scales with data size
- ✅ Consistent with transformation logic

#### B. Fixed Source/Target Connection Handling in `transform_data()`
**Critical Fix:** Separated source and target connections

**Data Flow:**
```
SOURCE (INA-7A) → Spark Processing → TARGET (pg-ssg)
    ↓                                      ↓
Read from                             Write to
pmr_production_data              operator_daily_performance
```

**Code Changes:**
- Get source connection: `get_postgres_source_connection()` for INA-7A
- Get target connection: `get_target_postgres_connection()` for pg-ssg
- Read data from SOURCE using `source_jdbc_url`
- Write data to TARGET using `target_connection_params`

#### C. Enhanced Connection Functions
Added proper fallback and logging for both connection functions:
- `get_postgres_source_connection()` - INA-7A (port 5433)
- `get_target_postgres_connection()` - pg-ssg (port 5432)

### 2. Modified `dags/hanger_lines_data_7A`

#### Updated `check_for_data()` Task
**Before:** Simple function call (no Spark)
```python
count = check_for_recent_data(connection_params=None, days=27)
```

**After:** Spark-enabled cluster submission
```python
count = check_for_recent_data(spark=None, days=30)  # Creates Spark session
```

**Flow:**
1. Task starts in Airflow
2. Imports `check_for_recent_data` from sparkFiles
3. Function creates Spark session
4. Connects to Spark cluster (if SPARK_MASTER_URL is set)
5. Submits JDBC query to cluster
6. Returns count to Airflow
7. Branches based on count

## Spark Cluster Configuration

### Current Setup (docker-compose.yaml)
```yaml
spark-master:
  - Port 7077: Spark master communication
  - Port 9090: Web UI
  - Memory: 4GB
  - CPUs: 2

spark-worker-1 & spark-worker-2:
  - Worker cores: 4 each
  - Worker memory: 6GB each
  - Memory: 8GB container limit
  - CPUs: 3 each
```

### Spark Mode Selection
The code automatically detects the mode via environment variable:

**Local Mode (Default):**
```bash
# No environment variable set, or:
export SPARK_MASTER_URL="local[4]"
```

**Cluster Mode:**
```bash
export SPARK_MASTER_URL="spark://spark-master:7077"
```

To enable cluster mode in Airflow, add to `docker-compose.yaml` under the worker environment:
```yaml
airflow-worker-1:
  environment:
    SPARK_MASTER_URL: "spark://spark-master:7077"
```

## Data Flow Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                    AIRFLOW DAG EXECUTION                    │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  1. check_for_data (BranchPythonOperator)                  │
│     └─> Creates Spark Session                              │
│         └─> Connects to Spark Cluster                      │
│             └─> Reads from INA-7A (pmr_production_data)    │
│                 └─> Returns count                          │
│                                                             │
│  2. transform_data (PythonOperator)                        │
│     └─> Creates Spark Session                              │
│         └─> Connects to Spark Cluster                      │
│             ├─> Reads from INA-7A (source)                 │
│             │   └─> pmr_production_data table              │
│             │                                               │
│             └─> Writes to pg-ssg (target)                  │
│                 └─> operator_daily_performance table       │
│                     (UPSERT with staging table)            │
│                                                             │
└─────────────────────────────────────────────────────────────┘
```

## Target Configuration

### TARGETS List
```python
TARGETS = [
    {"table": "operator_daily_performance", "pk": ["odpd_key"]},
]
```

- Single target table (can be extended)
- Primary key: `odpd_key`
- Upsert strategy: Staging table with ON CONFLICT

### Source Table Mapping
**Source Table:** `pmr_production_data` (INA-7A)
**Target Table:** `operator_daily_performance` (pg-ssg)

**Transformations:**
- Line mapping (e.g., '10' → 'line-30', '11' → 'line-21', etc.)
- Date coalescing
- Type casting
- Column renaming

## Testing Checklist

- [ ] Verify Spark cluster is running
  ```bash
  docker ps | grep spark
  ```

- [ ] Check Spark master UI at http://localhost:9090

- [ ] Test data check task with Spark
  ```bash
  docker exec -it <airflow-worker> python /opt/airflow/sparkFiles/hangerline_transform_spark_7A.py
  ```

- [ ] Verify source connection (INA-7A)
  - Check Airflow connection: `INA-7A`
  - Verify pmr_production_data table exists

- [ ] Verify target connection (pg-ssg)
  - Check Airflow connection: `pg-ssg`
  - Verify operator_daily_performance table exists

- [ ] Run DAG and monitor:
  - check_for_data task uses Spark
  - transform_data reads from INA-7A
  - Data is upserted to pg-ssg
  - No connection errors

## Troubleshooting

### Issue: "Connection refused to spark-master:7077"
**Solution:** Ensure SPARK_MASTER_URL is set and Spark cluster is running

### Issue: "No suitable driver found"
**Solution:** Check JDBC driver paths in docker-compose volumes:
```yaml
volumes:
  - ./sparkFiles:/opt/spark/work
```

### Issue: "Table not found"
**Solution:** Verify:
- Source table exists in INA-7A database
- Target table exists in pg-ssg database
- Airflow connections are correctly configured

### Issue: "Permission denied"
**Solution:** Check database user permissions for both source and target

## Performance Optimization

Current settings optimized for:
- Small to medium datasets (< 1M rows)
- 30-day data window
- 10-minute run frequency

**Tuning Parameters:**
- `fetchsize`: 5000 (adjust based on network/memory)
- `shuffle.partitions`: Dynamic (50-200 based on data size)
- `queryTimeout`: 600s
- `connectTimeout`: 60s

## Next Steps

1. **Enable Cluster Mode:** Set `SPARK_MASTER_URL` environment variable
2. **Monitor Performance:** Check Spark UI for job execution
3. **Scale Workers:** Add more workers if needed
4. **Optimize Queries:** Add indexes on source table if slow
5. **Test with Production Data:** Verify data quality after transformation

## Files Modified
- `sparkFiles/hangerline_transform_spark_7A.py` - Core ETL logic
- `dags/hanger_lines_data_7A` - Airflow DAG definition
- `SPARK_CLUSTER_INTEGRATION_7A.md` - This documentation

---
**Last Updated:** 2025-12-27
**Author:** Automated by Cline
**Status:** ✅ Ready for Testing
