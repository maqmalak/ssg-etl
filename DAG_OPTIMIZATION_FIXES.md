# DAG Container Resource Optimization - Summary

## Problem Identified
Your DAGs were harming Docker containers due to:
1. **High Worker Concurrency**: 8 concurrent tasks per worker causing memory exhaustion
2. **Excessive Spark Resource Allocation**: Driver using 2GB + executors using 4GB each in same container
3. **High-Frequency Scheduling**: DAGs running every 7-10 minutes without recovery time
4. **No Resource Monitoring**: No circuit breakers to prevent OOM conditions

## Changes Applied

### 1. Reduced Worker Concurrency (docker-compose.yaml)
**Before:**
```yaml
AIRFLOW__CELERY__WORKER_CONCURRENCY: '8'
```

**After:**
```yaml
AIRFLOW__CELERY__WORKER_CONCURRENCY: '3'
```

**Impact:** Limits each worker to 3 concurrent tasks instead of 8, preventing memory exhaustion

---

### 2. Optimized Spark Resources (dag_odp_etl.py)
**Before:**
```python
driver_memory="2g"
executor_memory="4g"
total_executor_cores=8
```

**After:**
```python
driver_memory="1g"          # Reduced from 2g
executor_memory="2g"        # Reduced from 4g
total_executor_cores=4      # Reduced from 8
conf={
    "spark.executor.memoryOverhead": "512m",
    "spark.memory.fraction": "0.6",
    "spark.sql.shuffle.partitions": "16",
}
```

**Impact:** Reduces Spark memory footprint by 50%, prevents container OOM

---

### 3. Reduced DAG Scheduling Frequency
**Before:**
- `dag_odp_etl.py`: Every 10 minutes
- `hanger_line_daily_transform.py`: Every 7 minutes

**After:**
- Both DAGs: Every 15 minutes

**Impact:** Provides 8 more minutes between runs for resource recovery

---

### 4. Added Memory Monitoring (hanger_line_daily_transform.py)
**New Features:**
```python
# Memory monitoring before transformation
import psutil
process = psutil.Process()
memory_mb = process.memory_info().rss / 1024 / 1024

# Circuit breaker
if memory_mb > 3000:  # 3GB threshold
    logger.warning(f"High memory usage ({memory_mb:.2f} MB). Skipping transformation.")
    return {"success": False, "message": f"Skipped due to high memory"}

# Proper Spark session cleanup
finally:
    if spark is not None:
        spark.stop()
```

**Impact:** Prevents transformation when memory is already high, ensures cleanup

---

## How to Apply Changes

### Step 1: Restart Docker Services
```bash
cd /home/maqmalak/ETL/ssg-etl
docker compose restart airflow-worker-1 airflow-worker-2 airflow-scheduler airflow-dag-processor
```

### Step 2: Verify Worker Concurrency
```bash
docker exec ssg-etl-airflow-worker-1-1 env | grep WORKER_CONCURRENCY
# Should show: AIRFLOW__CELERY__WORKER_CONCURRENCY=3
```

### Step 3: Monitor Resource Usage
```bash
# Check memory usage of workers
docker stats ssg-etl-airflow-worker-1-1 ssg-etl-airflow-worker-2-1 --no-stream

# Watch for improvements
watch -n 5 'docker stats --no-stream | grep airflow-worker'
```

### Step 4: Check DAG Schedules in UI
- Navigate to Airflow UI (http://localhost:8088)
- Check both DAGs show schedule: `*/15 8-23,0-1 * * 1-6`
- Verify `max_active_runs=1` is respected

---

## Expected Results

### Before Optimization
- Worker Memory: 8-10GB (approaching limit)
- Concurrent Tasks: Up to 8 per worker (16 total)
- DAG Frequency: 7-10 minutes
- Resource Conflicts: Frequent OOM, container crashes

### After Optimization
- Worker Memory: 5-7GB (comfortable margin)
- Concurrent Tasks: Max 3 per worker (6 total)
- DAG Frequency: 15 minutes
- Resource Conflicts: Minimal, with circuit breakers

---

## Monitoring Checklist

✅ **Worker Memory** - Should stay below 8GB
✅ **Spark Driver Memory** - Max 1GB per job
✅ **Task Concurrency** - Max 3 per worker
✅ **DAG Run Frequency** - 15 minute intervals
✅ **Failed Tasks** - Check for "High memory usage" skips

---

## Additional Recommendations

### Short Term (Next Steps)
1. **Create Airflow Pools**:
   ```python
   # In Airflow UI: Admin -> Pools
   Pool Name: spark_pool
   Slots: 2
   Description: Limit concurrent Spark jobs
   ```

2. **Add Resource Requirements to Tasks**:
   ```python
   run_spark = SparkSubmitOperator(
       ...
       pool='spark_pool',
       pool_slots=1,
   )
   ```

### Medium Term (Consider for Future)
1. **Use Cluster Mode**: Deploy Spark driver on cluster, not in Airflow worker
2. **Kubernetes Executor**: Isolate each task in its own pod
3. **External Spark Cluster**: Dedicated Spark cluster separate from Airflow
4. **Resource Quotas**: Implement per-DAG memory limits

### Long Term (Architecture Improvements)
1. **Separate Compute from Orchestration**: Airflow schedules, Spark clusters execute
2. **Auto-scaling Workers**: Add/remove workers based on load
3. **Monitoring & Alerting**: Prometheus/Grafana for resource tracking

---

## Troubleshooting

### If Workers Still Crash
1. Further reduce concurrency to 2
2. Lower Spark executor memory to 1.5g
3. Increase scheduling interval to 20-30 minutes

### If DAGs Don't Pick Up New Schedule
```bash
# Clear DAG cache
docker exec ssg-etl-airflow-scheduler-1 airflow dags reserialize

# Or restart scheduler
docker compose restart airflow-scheduler
```

### If Memory Monitoring Doesn't Work
```bash
# Check if psutil is installed
docker exec ssg-etl-airflow-worker-1-1 python -c "import psutil; print(psutil.__version__)"

# If not, add to requirements.txt and rebuild
```

---

## Files Modified
1. `docker-compose.yaml` - Worker concurrency reduced to 3
2. `dags/dag_odp_etl.py` - Spark resources optimized, schedule increased to 15 min
3. `dags/hanger_line_daily_transform.py` - Memory monitoring added, schedule increased to 15 min

**Date:** December 30, 2025
**Version:** 1.0
