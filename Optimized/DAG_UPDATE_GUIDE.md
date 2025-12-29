# DAG Update Troubleshooting Guide

## Problem
After making changes to DAG code, Airflow shows the old version.

## Quick Fix Steps

### 1. Clear Python Cache
```bash
find ./dags -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find ./dags -type f -name "*.pyc" -delete 2>/dev/null || true
```

### 2. Update File Timestamp
```bash
touch dags/your_dag_file.py
```

### 3. Restart Critical Services
```bash
docker compose restart airflow-dag-processor airflow-scheduler
```

### 4. Force DAG Refresh (if needed)
Wait 20-30 seconds for services to start, then:
```bash
docker compose exec -T airflow-scheduler airflow dags reserialize
```

## Verification

### Check DAG Processor Logs
```bash
docker compose logs --tail=50 airflow-dag-processor | grep "your_dag_id"
```

### Check Scheduler Logs
```bash
docker compose logs --tail=50 airflow-scheduler | grep "your_dag_id"
```

### View Service Status
```bash
docker compose ps | grep -E "scheduler|dag-processor"
```

## Common Issues

### Issue 1: Import Errors
**Symptom:** DAG not showing up at all
**Solution:** Check logs for Python import errors:
```bash
docker compose logs airflow-dag-processor | grep -i "error\|exception"
```

### Issue 2: Syntax Errors
**Symptom:** DAG marked with red error icon
**Solution:** Airflow UI will show the error. Fix Python syntax issues.

### Issue 3: Slow Refresh
**Symptom:** Takes several minutes to update
**Solution:** 
- Check `AIRFLOW__CORE__DAGBAG_IMPORT_TIMEOUT` setting
- Check `AIRFLOW__SCHEDULER__DAG_DIR_LIST_INTERVAL` (default is every few seconds)
- Use the force refresh command above

## Prevention Tips

1. **Always test DAG syntax** before deploying:
   ```bash
   docker compose exec airflow-scheduler airflow dags test your_dag_id
   ```

2. **Use DAG file validation**:
   ```bash
   docker compose exec airflow-scheduler python /opt/airflow/dags/your_dag_file.py
   ```

3. **Enable auto-refresh in Airflow UI** (Browser auto-refresh every 30 seconds)

4. **Monitor DAG processor logs** when deploying changes

## Emergency Full Reset (Use with caution)

If nothing else works:
```bash
# Stop all services
docker compose down

# Clear all Python cache
find . -type d -name "__pycache__" -exec rm -rf {} + 2>/dev/null || true
find . -type f -name "*.pyc" -delete 2>/dev/null || true

# Restart services
docker compose up -d
```

## Notes

- DAG processor runs independently (Airflow 2.11+)
- Changes should be picked up within 30-60 seconds normally
- Airflow UI may cache - do a hard refresh (Ctrl+Shift+R or Cmd+Shift+R)
- Use `docker compose` not `docker-compose` (newer Docker versions)
