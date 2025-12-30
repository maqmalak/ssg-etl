# Spark Cluster ETL Guide - Hanger Line Data Processing

## Overview
This guide covers the complete ETL pipeline for Hanger Line data processing using Apache Spark in cluster mode with 1 master and 2 worker nodes.

## Architecture

### Cluster Configuration
- **Spark Master**: 1 node (coordinator)
- **Spark Workers**: 2 nodes (executors)
- **Airflow**: Orchestrates the ETL pipeline
- **PostgreSQL**: Source (INA-7A) and Target (pg-ssg) databases

### ETL Pipeline Phases
The pipeline is divided into 5 sequential tasks:

1. **source_check** - Validates source data availability
2. **target_check** - Validates target database and tables
3. **load_data** - Extracts data from source using Spark
4. **transform_data** - Transforms and upserts data to target
5. **summary** - Generates comprehensive execution metrics

## Resource Allocation

### Spark Master
- **CPU**: 4 cores
- **Memory**: 8GB RAM
- **Role**: Cluster coordination, job scheduling, UI hosting

### Spark Workers (2 instances)
- **CPU**: 4 cores each
- **Memory**: 12GB RAM each
- **Total Capacity**: 8 cores, 24GB RAM

### Executor Configuration
- **Executors**: 2 (one per worker)
- **Cores per Executor**: 3
- **Memory per Executor**: 8GB
- **Memory Overhead**: 2GB per executor

## Installation & Setup

### 1. Docker Compose Configuration

Create or update `docker-compose.spark.yml`:

```yaml
version: '3.8'

services:
  spark-master:
    image: bitnami/spark:3.5.0
    container_name: spark-master
    hostname: spark-master
    environment:
      - SPARK_MODE=master
      - SPARK_MASTER_HOST=spark-master
      - SPARK_MASTER_PORT=7077
      - SPARK_MASTER_WEBUI_PORT=8080
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - "7077:7077"   # Spark master port
      - "8080:8080"   # Spark master UI
      - "4040:4040"   # Spark application UI
    volumes:
      - ./sparkFiles:/opt/spark/work
      - ./sparkFiles/jdbc-drivers:/opt/spark/work/jdbc-drivers
    networks:
      - etl-network

  spark-worker-1:
    image: bitnami/spark:3.5.0
    container_name: spark-worker-1
    hostname: spark-worker-1
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=12g
      - SPARK_WORKER_CORES=4
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - "8081:8081"   # Worker 1 UI
    volumes:
      - ./sparkFiles:/opt/spark/work
      - ./sparkFiles/jdbc-drivers:/opt/spark/work/jdbc-drivers
    networks:
      - etl-network

  spark-worker-2:
    image: bitnami/spark:3.5.0
    container_name: spark-worker-2
    hostname: spark-worker-2
    depends_on:
      - spark-master
    environment:
      - SPARK_MODE=worker
      - SPARK_MASTER_URL=spark://spark-master:7077
      - SPARK_WORKER_MEMORY=12g
      - SPARK_WORKER_CORES=4
      - SPARK_RPC_AUTHENTICATION_ENABLED=no
      - SPARK_RPC_ENCRYPTION_ENABLED=no
      - SPARK_LOCAL_STORAGE_ENCRYPTION_ENABLED=no
      - SPARK_SSL_ENABLED=no
    ports:
      - "8082:8081"   # Worker 2 UI
    volumes:
      - ./sparkFiles:/opt/spark/work
      - ./sparkFiles/jdbc-drivers:/opt/spark/work/jdbc-drivers
    networks:
      - etl-network

networks:
  etl-network:
    external: true
```

### 2. Start Spark Cluster

```bash
# Start Spark cluster
docker-compose -f docker-compose.spark.yml up -d

# Verify cluster status
docker ps | grep spark

# Check logs
docker logs spark-master
docker logs spark-worker-1
docker logs spark-worker-2
```

### 3. Configure Airflow Connection

Add Spark connection in Airflow:

```bash
# Access Airflow web UI
# Navigate to Admin > Connections > Add Connection

Connection ID: spark_default
Connection Type: Spark
Host: spark://spark-master
Port: 7077
Extra: {}
```

Or via CLI:

```bash
docker exec -it airflow-webserver airflow connections add \
    spark_default \
    --conn-type spark \
    --conn-host spark://spark-master \
    --conn-port 7077
```

### 4. Environment Variables

Set in `.env` or `docker-compose.yaml`:

```bash
# Spark Configuration
SPARK_MASTER_URL=spark://spark-master:7077
SPARK_UI_PORT=4040
SPARK_UI_BIND=0.0.0.0

# Source Database (INA-7A)
INA_7A_HOST=your-source-host
INA_7A_PORT=5433
INA_7A_DATABASE=your-source-db
INA_7A_USER=your-user
INA_7A_PASSWORD=your-password

# Target Database (pg-ssg)
TARGET_PG_HOST=172.16.7.6
TARGET_PG_PORT=5432
TARGET_PG_DATABASE=ssg
TARGET_PG_USER=postgres
TARGET_PG_PASSWORD=your-password
```

## Usage

### Running the DAG

#### Method 1: Airflow Web UI
1. Navigate to Airflow UI: http://localhost:8080
2. Find DAG: `hanger_lines_data_7A_cluster`
3. Click "Trigger DAG"
4. Monitor execution in Graph or Gantt view

#### Method 2: Airflow CLI
```bash
# Trigger DAG
docker exec -it airflow-webserver airflow dags trigger hanger_lines_data_7A_cluster

# Check status
docker exec -it airflow-webserver airflow dags list-runs -d hanger_lines_data_7A_cluster

# View logs
docker exec -it airflow-webserver airflow tasks logs hanger_lines_data_7A_cluster source_check_task.run_source_check <execution_date>
```

### Monitoring

#### Spark Master UI
- URL: http://localhost:8080
- Shows: Active workers, running applications, cluster resources

#### Spark Worker UIs
- Worker 1: http://localhost:8081
- Worker 2: http://localhost:8082
- Shows: Executor details, memory usage, task execution

#### Spark Application UI (during execution)
- URL: http://localhost:4040
- Shows: Job stages, SQL queries, storage, environment

#### Airflow UI
- URL: http://localhost:8080
- Shows: DAG runs, task status, logs, metrics

### Metrics and Logs

Metrics are stored in: `/opt/airflow/logs/etl_metrics/`

#### Per-task Metrics Files
```
metrics__hanger_lines_data_7A_cluster__source_check__20250130T011000.json
metrics__hanger_lines_data_7A_cluster__target_check__20250130T011100.json
metrics__hanger_lines_data_7A_cluster__load_data__20250130T011200.json
metrics__hanger_lines_data_7A_cluster__transform_data__20250130T011500.json
summary__hanger_lines_data_7A_cluster__20250130T011800.json
```

#### Metric Structure
```json
{
  "success": true,
  "task": "load_data",
  "duration_sec": 45.2,
  "record_count": 150000,
  "column_count": 28,
  "throughput_rps": 3318,
  "lookback_days": 30
}
```

## Performance Tuning

### Spark Configuration Adjustments

#### For Larger Datasets (>1M records)
```python
# In DAG configuration
.config("spark.sql.shuffle.partitions", "64")
.config("spark.executor.memory", "10g")
```

#### For Smaller Datasets (<100K records)
```python
# In DAG configuration
.config("spark.sql.shuffle.partitions", "16")
.config("spark.executor.memory", "6g")
```

### JDBC Optimization

#### Increase Fetch Size
```python
.option("fetchsize", "20000")  # Default: 10000
```

#### Increase Batch Size for Writes
```python
.option("batchsize", "10000")  # Default: 5000
```

#### Parallel Partitioning
```python
.option("numPartitions", "16")  # Default: 8
```

### Memory Management

#### Monitor Memory Usage
```bash
# Check container memory
docker stats spark-master spark-worker-1 spark-worker-2

# If OOM errors occur, increase worker memory
SPARK_WORKER_MEMORY=16g
```

## Troubleshooting

### Issue: Spark Workers Not Connecting
```bash
# Check network connectivity
docker exec spark-worker-1 ping spark-master

# Restart workers
docker-compose -f docker-compose.spark.yml restart spark-worker-1 spark-worker-2
```

### Issue: JDBC Driver Not Found
```bash
# Verify driver exists
docker exec spark-master ls -la /opt/spark/work/jdbc-drivers/

# Copy driver if missing
docker cp sparkFiles/jdbc-drivers/postgresql-42.7.3.jar spark-master:/opt/spark/work/jdbc-drivers/
```

### Issue: Out of Memory Errors
```bash
# Increase executor memory
SPARK_EXECUTOR_MEMORY=10g

# Reduce shuffle partitions
spark.sql.shuffle.partitions=16

# Increase memory overhead
spark.executor.memoryOverhead=3g
```

### Issue: Connection Timeout
```bash
# Increase timeouts in Spark config
.config("spark.network.timeout", "800s")
.config("spark.rpc.askTimeout", "800s")
```

### Issue: Task Failures
```bash
# Enable retry mechanism
.config("spark.task.maxFailures", "4")

# Check logs
docker logs spark-master
docker logs spark-worker-1
```

## Best Practices

### 1. Data Validation
- Always run `source_check` before loading data
- Verify target table structure with `target_check`
- Monitor metrics files for anomalies

### 2. Resource Management
- Scale workers based on data volume
- Monitor cluster utilization in Spark UI
- Adjust executor memory for workload

### 3. Error Handling
- Review task logs immediately on failure
- Check metrics files for error details
- Verify database connectivity before rerun

### 4. Scheduling
- Run during off-peak hours for large datasets
- Use `schedule_interval='@daily'` for regular runs
- Set appropriate `lookback_days` parameter

### 5. Monitoring
- Set up alerts for DAG failures
- Monitor execution times for degradation
- Track data volume trends

## Performance Benchmarks

### Expected Performance (Sample Data)
- **50K records**: ~15-20 seconds (load + transform)
- **200K records**: ~45-60 seconds (load + transform)
- **500K records**: ~2-3 minutes (load + transform)
- **1M records**: ~5-7 minutes (load + transform)

### Cluster vs Local Mode Comparison
| Metric | Local Mode | Cluster Mode | Improvement |
|--------|------------|--------------|-------------|
| 500K records | ~4 min | ~2.5 min | 37% faster |
| Memory usage | 8GB | 24GB | 3x capacity |
| Parallel tasks | 4 | 6 | 50% more |
| Scalability | Limited | High | Much better |

## Security Considerations

### 1. Database Credentials
- Use Airflow Connections (encrypted)
- Never hardcode passwords
- Rotate credentials regularly

### 2. Network Security
- Use internal Docker network
- Restrict external access to Spark UI
- Enable SSL for production

### 3. Data Protection
- Use VPN for remote databases
- Enable encryption at rest
- Implement access controls

## Maintenance

### Daily Tasks
- Check DAG execution status
- Review metrics files
- Monitor cluster health

### Weekly Tasks
- Analyze performance trends
- Review and clean old logs
- Update documentation

### Monthly Tasks
- Review resource allocation
- Optimize Spark configurations
- Update dependencies

## Support

### Logs Location
- **Airflow Logs**: `/opt/airflow/logs/`
- **Metrics**: `/opt/airflow/logs/etl_metrics/`
- **Spark Logs**: Check container logs

### Common Commands
```bash
# View DAG logs
docker exec -it airflow-scheduler tail -f /opt/airflow/logs/scheduler/latest/*.log

# Check Spark cluster status
docker exec spark-master /opt/bitnami/spark/bin/spark-class org.apache.spark.deploy.master.Master --help

# Restart entire stack
docker-compose -f docker-compose.spark.yml restart
```

### Contact & Support
For issues or questions:
1. Check this documentation first
2. Review Airflow task logs
3. Check Spark UI for job details
4. Contact ETL team with:
   - DAG run ID
   - Task name
   - Error message
   - Metrics file

---

## Quick Start Checklist

- [ ] Start Spark cluster: `docker-compose -f docker-compose.spark.yml up -d`
- [ ] Verify workers connected (check Spark UI)
- [ ] Configure Airflow connection `spark_default`
- [ ] Set environment variables in `.env`
- [ ] Copy JDBC driver to sparkFiles/jdbc-drivers/
- [ ] Trigger DAG from Airflow UI
- [ ] Monitor execution in Spark UI
- [ ] Review metrics files after completion

---

**Last Updated**: December 30, 2025
**Version**: 1.0
**Author**: ETL Team
