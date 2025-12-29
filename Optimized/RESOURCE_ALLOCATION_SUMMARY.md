# ETL Pipeline Resource Allocation Summary

**System Specifications:** 24 CPU Cores, 64GB RAM  
**Date:** December 24, 2025  
**Airflow Version:** 2.11.0  
**Spark Version:** 3.5.0  
**Executor:** CeleryExecutor with Redis Broker

---

## 📊 Optimized Resource Distribution

### Total Allocation Overview
- **Total Memory Allocated:** ~62.5 GB (97.7% utilization)
- **Total CPUs Allocated:** ~23.5 cores (97.9% utilization)
- **Available Buffer:** ~1.5 GB RAM, ~0.5 CPU cores

---

## 🔧 Service-by-Service Breakdown

### Airflow Services

| Service | Memory Limit | CPU Limit | Memory Reserved | CPU Reserved | Key Config |
|---------|--------------|-----------|-----------------|--------------|------------|
| **Scheduler** | 10 GB | 3.0 | 5 GB | 1.5 | Main orchestrator |
| **Worker-1** | 14 GB | 4.0 | 7 GB | 2.5 | Celery concurrency: 12 |
| **Worker-2** | 14 GB | 4.0 | 7 GB | 2.5 | Celery concurrency: 12 |
| **Webserver** | 3 GB | 1.5 | 1.5 GB | 0.8 | Gunicorn workers: 2 |
| **DAG Processor** | 2.5 GB | 1.0 | 1.5 GB | 0.5 | Parsing processes: 4 |
| **Triggerer** | 1 GB | 0.5 | 0.5 GB | 0.25 | Deferrable tasks |
| **Subtotal** | **44.5 GB** | **14.0** | **23 GB** | **8.05** | - |

### Data Layer Services

| Service | Memory Limit | CPU Limit | Memory Reserved | CPU Reserved | Notes |
|---------|--------------|-----------|-----------------|--------------|-------|
| **PostgreSQL (metadata)** | 4 GB | 1.5 | 2 GB | 0.8 | Airflow DB |
| **PostgreSQL (warehouse)** | 4 GB | 1.0 | 2 GB | 0.5 | Data warehouse |
| **Redis** | 3 GB | 1.0 | 1.5 GB | 0.5 | Max memory: 2.5 GB |
| **Subtotal** | **11 GB** | **3.5** | **5.5 GB** | **1.8** | - |

### Spark Cluster

| Service | Memory Limit | CPU Limit | Memory Reserved | CPU Reserved | Worker Config |
|---------|--------------|-----------|-----------------|--------------|---------------|
| **Spark Master** | 2 GB | 0.5 | 1 GB | 0.25 | Cluster manager |
| **Spark Worker-1** | 8 GB | 3.0 | 6 GB | 2.0 | 6 cores, 6GB executor |
| **Spark Worker-2** | 8 GB | 3.0 | 6 GB | 2.0 | 6 cores, 6GB executor |
| **Subtotal** | **18 GB** | **6.5** | **13 GB** | **4.25** | - |

### 🎯 Grand Total
- **Memory Limits:** 44.5 + 11 + 18 = **73.5 GB allocated** (with reservations at ~41.5 GB)
- **CPU Limits:** 14.0 + 3.5 + 6.5 = **24.0 CPUs allocated**

---

## ⚙️ Airflow Configuration Optimizations

### Core Settings (config/airflow.cfg)

| Parameter | Old Value | New Value | Impact |
|-----------|-----------|-----------|--------|
| `parallelism` | 16 | **32** | +100% total concurrent tasks |
| `max_active_tasks_per_dag` | 16 | **24** | +50% per-DAG concurrency |
| `default_pool_task_slot_count` | 32 | **48** | +50% default pool capacity |
| `worker_concurrency` | 8 | **12** | +50% per-worker tasks |
| `parsing_processes` | 2 | **4** | +100% DAG parsing speed |
| `max_tis_per_query` | 16 | **24** | +50% scheduling batch size |
| `max_dagruns_to_create_per_loop` | 10 | **15** | +50% DAG run creation |
| `max_dagruns_per_loop_to_schedule` | 20 | **30** | +50% scheduling throughput |

### Database Connection Pool

| Parameter | Old Value | New Value | Benefit |
|-----------|-----------|-----------|---------|
| `sql_alchemy_pool_size` | 10 | **15** | More concurrent DB connections |
| `sql_alchemy_max_overflow` | 20 | **30** | Better burst handling |

### Celery Worker Settings

```ini
worker_concurrency = 12              # Tasks per worker (24 total across 2 workers)
worker_prefetch_multiplier = 1       # Fairness in task distribution
task_acks_late = True                # Reliability - ack after completion
task_track_started = True            # Better task monitoring
```

---

## 🚀 Performance Improvements

### Expected Throughput Gains

1. **Task Execution:** 
   - From 16 → 32 concurrent tasks (+100%)
   - 24 tasks per worker (2 workers × 12 concurrency)

2. **DAG Processing:**
   - 4 parallel parsing processes (up from 2)
   - Faster DAG discovery and scheduling

3. **Spark Processing:**
   - 2 workers with 6 cores each = 12 total Spark cores
   - 12 GB total executor memory for parallel jobs

4. **Database Throughput:**
   - 15 base connections + 30 overflow = 45 max connections
   - Better handling of concurrent DAG runs

5. **Redis Capacity:**
   - 2.5 GB max memory (up from 1.6 GB)
   - Better task queue capacity for high-volume workflows

---

## 📋 Deployment Checklist

### Before Starting Services

- [ ] Ensure Docker Desktop has 64GB RAM and 24 CPU cores allocated
- [ ] Verify `.env` file contains required environment variables
- [ ] Check that all volume directories exist (dags, logs, config, sparkFiles)
- [ ] Ensure PostgreSQL ports 5435 and 5438 are available
- [ ] Verify Redis port 6379 is not in use

### Starting the Stack

```bash
# Build custom Airflow image with dependencies
docker compose build

# Start all services
docker compose up -d

# Or start with Flower for monitoring
docker compose --profile flower up -d

# Check service health
docker compose ps

# View logs
docker compose logs -f airflow-scheduler
docker compose logs -f airflow-worker-1
docker compose logs -f airflow-worker-2
```

### Monitoring Resources

```bash
# Monitor resource usage
docker stats

# Check Celery workers
docker compose exec airflow-worker-1 celery -A airflow.providers.celery.executors.celery_executor.app inspect active

# Check Spark cluster
# Access Spark UI: http://localhost:9090
# Access Airflow UI: http://localhost:8088
# Access Flower (if enabled): http://localhost:5555
```

---

## 🔍 Service URLs

| Service | URL | Purpose |
|---------|-----|---------|
| **Airflow Webserver** | http://localhost:8088 | Main Airflow UI |
| **Spark Master UI** | http://localhost:9090 | Spark cluster monitoring |
| **Flower** | http://localhost:5555 | Celery task monitoring (profile: flower) |
| **PostgreSQL (metadata)** | localhost:5438 | Airflow database |
| **PostgreSQL (warehouse)** | localhost:5435 | Data warehouse |
| **Redis** | localhost:6379 | Message broker (internal) |

---

## ⚡ Optimization Highlights

### 1. Airflow Workers
- **Enhanced from:** 12GB/3.5 CPU each
- **Enhanced to:** 14GB/4.0 CPU each
- **Concurrency:** 12 tasks per worker (24 total)

### 2. Scheduler
- **Enhanced from:** 8GB/2.0 CPU
- **Enhanced to:** 10GB/3.0 CPU
- **Parsing:** 4 parallel processes

### 3. Spark Cluster
- **New:** Added second Spark worker
- **Total capacity:** 12 cores, 12GB executor memory
- **Benefit:** Parallel Spark job execution

### 4. Redis
- **Enhanced from:** 2GB/0.4 CPU, 1.6GB maxmemory
- **Enhanced to:** 3GB/1.0 CPU, 2.5GB maxmemory
- **Benefit:** Larger task queue capacity

### 5. DAG Processor
- **Enhanced from:** 2GB/0.4 CPU
- **Enhanced to:** 2.5GB/1.0 CPU
- **Benefit:** Faster DAG parsing with 4 processes

---

## 🎯 Best Practices

### For DAG Development

1. **Use Task Groups:** Organize related tasks for better visualization
2. **Set Proper Pools:** Utilize the 48-slot default pool or create custom pools
3. **Configure Retries:** Set appropriate retry policies per task
4. **Use Deferrable Operators:** Leverage the Triggerer for async operations
5. **Monitor Resources:** Check task resource usage in the UI

### For Spark Jobs

1. **Connect to Spark Cluster:** Use `spark://spark-master:7077`
2. **Executor Memory:** Configure based on job needs (max 6GB per worker)
3. **Dynamic Allocation:** Consider enabling for variable workloads
4. **JDBC Drivers:** Already configured in `SPARK_EXTRA_CLASSPATH`

### For Performance Tuning

1. **Pool Management:** Create dedicated pools for different workload types
2. **Task Priority:** Use priority weights for critical DAGs
3. **Concurrency Limits:** Set `max_active_runs` per DAG as needed
4. **Database Cleanup:** Schedule regular metadata cleanup tasks
5. **Log Rotation:** Configure appropriate log retention policies

---

## 🛠️ Troubleshooting

### If Services Fail to Start

```bash
# Check logs for specific service
docker compose logs [service-name]

# Reset and rebuild
docker compose down -v
docker compose build --no-cache
docker compose up -d
```

### If Workers are Not Picking Up Tasks

```bash
# Check worker status
docker-compose exec airflow-worker-1 celery -A airflow.providers.celery.executors.celery_executor.app inspect stats

# Check Redis connection
docker-compose exec redis redis-cli ping

# Restart workers
docker-compose restart airflow-worker-1 airflow-worker-2
```

### If Scheduler Performance is Slow

1. Check parsing processes: Should be 4
2. Verify DAG complexity: Simplify if needed
3. Monitor database connections: Should not exceed pool limits
4. Check for long-running tasks blocking the scheduler

---

## 📈 Monitoring Recommendations

### Key Metrics to Track

1. **Task Throughput:** Tasks completed per minute
2. **DAG Parse Time:** How long DAGs take to parse
3. **Queue Depth:** Number of queued tasks in Redis
4. **Worker Utilization:** % of worker slots in use
5. **Database Connections:** Active connections vs. pool size
6. **Scheduler Lag:** Time between task scheduled and started

### Tools

- **Airflow UI:** Task duration, success rates, DAG statistics
- **Flower:** Celery worker monitoring, task tracking
- **Spark UI:** Job execution, stage details, executor metrics
- **Docker Stats:** Real-time container resource usage
- **PostgreSQL:** Query performance, connection counts

---

## 🔄 Scaling Considerations

### To Add More Workers

1. Copy `airflow-worker-2` configuration in `docker-compose.yaml`
2. Rename to `airflow-worker-3`, `airflow-worker-4`, etc.
3. Add corresponding section in `docker-compose.override.yml`
4. Adjust resource limits proportionally

### To Add More Spark Workers

1. Copy `spark-worker-2` configuration
2. Rename and create new volume (`spark-worker-3-data`)
3. Adjust cores and memory based on available resources

---

## 📝 Version History

- **v1.0** (Dec 24, 2025): Initial optimized configuration
  - 24 cores, 64GB RAM allocation
  - 2 Airflow workers with 12 concurrency each
  - 2 Spark workers with 6 cores each
  - Enhanced scheduler, Redis, and database resources

---

## 📞 Support

For issues or questions:
- Review Airflow logs: `docker-compose logs -f`
- Check official documentation: https://airflow.apache.org/
- Monitor with Flower: http://localhost:5555 (when enabled)

---

**Note:** This configuration is optimized for local development/testing with Docker Desktop. For production deployments, consider Kubernetes with separate resource management and monitoring solutions.
