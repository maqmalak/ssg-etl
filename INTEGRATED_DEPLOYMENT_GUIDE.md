# Integrated Airflow + Spark Deployment Guide

## Overview

This guide covers the **integrated deployment** of Apache Airflow 2.11.0 with Apache Spark 3.5.0 cluster (1 Master + 2 Workers) using a single `docker-compose.yaml` file.

### Architecture Benefits

✅ **Single deployment command** - Start entire environment with one command  
✅ **Consistent networking** - All services in same Docker network  
✅ **Easier monitoring** - Unified resource management and logging  
✅ **Production-optimized** - Resource limits and health checks configured  

---

## Table of Contents

1. [Prerequisites](#prerequisites)
2. [Quick Start](#quick-start)
3. [Resource Allocation](#resource-allocation)
4. [Service Architecture](#service-architecture)
5. [Deployment Options](#deployment-options)
6. [Monitoring & Verification](#monitoring--verification)
7. [Configuration](#configuration)
8. [Troubleshooting](#troubleshooting)
9. [Best Practices](#best-practices)

---

## Prerequisites

### System Requirements

- **CPU**: 24 cores (minimum 16 cores)
- **RAM**: 64 GB (minimum 32 GB)
- **Disk**: 50 GB free space
- **OS**: Linux (tested on Ubuntu/Debian)

### Software Requirements

- Docker 20.10+
- Docker Compose 2.0+
- bash
- curl

### Verify Prerequisites

```bash
# Check Docker
docker --version
docker info

# Check Docker Compose
docker compose version

# Check system resources
free -h
nproc
df -h
```

---

## Quick Start

### 1. Full Environment Deployment

Start everything (Airflow + Spark + PostgreSQL + Redis):

```bash
# From project root
docker compose up -d

# Monitor startup
docker compose ps
docker compose logs -f
```

### 2. Spark Cluster Only

If Airflow is already running, start only Spark:

```bash
# Using the startup script (recommended)
chmod +x start-spark-cluster.sh
./start-spark-cluster.sh

# Or manually
docker compose up -d spark-master spark-worker-1 spark-worker-2
```

### 3. Verify Deployment

```bash
# Check all services
docker compose ps

# Check Spark cluster
docker exec spark-master curl -s http://localhost:8080/json/ | grep aliveworkers
```

### 4. Access UIs

| Service | URL | Credentials |
|---------|-----|-------------|
| Airflow UI | http://localhost:8088 | airflow / airflow |
| Spark Master UI | http://localhost:9090 | - |
| Spark Worker 1 UI | http://localhost:8081 | - |
| Spark Worker 2 UI | http://localhost:8082 | - |
| Spark Application UI | http://localhost:4040 | (when job running) |

---

## Resource Allocation

### Total Resource Budget (64 GB / 24 Cores)

#### Airflow Services (~30 GB / ~14 cores)

```yaml
PostgreSQL:      4 GB  / 1.5 cores
Redis:           3 GB  / 1.0 core
API Server:      3 GB  / 1.5 cores
Scheduler:       10 GB / 3.0 cores
DAG Processor:   2.5 GB / 1.0 core
Worker 1:        10 GB / 4.0 cores
Worker 2:        10 GB / 4.0 cores
Triggerer:       1 GB  / 0.5 cores
```

#### Spark Services (~34 GB / ~10 cores)

```yaml
Spark Master:    6 GB  / 2.0 cores
Spark Worker 1:  14 GB / 4.0 cores
Spark Worker 2:  14 GB / 4.0 cores
```

**Note**: These are limits. Actual usage will be lower, with reservations ensuring minimum guaranteed resources.

---

## Service Architecture

### Network Topology

```
┌─────────────────────────────────────────────────────────────┐
│                    Docker Network: airflow_network          │
│                                                             │
│  ┌──────────────┐        ┌──────────────────────┐          │
│  │   Airflow    │        │    Spark Cluster     │          │
│  │  Services    │───────▶│  1 Master + 2 Workers│          │
│  │              │        │                      │          │
│  └──────────────┘        └──────────────────────┘          │
│         │                         │                         │
│         │                         │                         │
│  ┌──────▼──────┐          ┌──────▼──────┐                 │
│  │ PostgreSQL  │          │   Volumes   │                 │
│  │   Redis     │          │  (persist)  │                 │
│  └─────────────┘          └─────────────┘                 │
└─────────────────────────────────────────────────────────────┘
```

### Service Communication

- **Airflow → Spark**: Uses SparkSubmitOperator via `spark://spark-master:7077`
- **Spark Workers → Master**: Register at startup via `spark://spark-master:7077`
- **JDBC Connections**: Spark can access both source and target databases

---

## Deployment Options

### Option 1: Full Stack (Recommended)

Deploy everything together:

```bash
# Start all services
docker compose up -d

# Wait for initialization (2-3 minutes)
sleep 180

# Verify
docker compose ps
curl http://localhost:8088/health
curl http://localhost:9090
```

**Use Case**: Fresh deployment, development, testing

### Option 2: Airflow First, Spark Later

Start Airflow, then add Spark when needed:

```bash
# Step 1: Start Airflow stack
docker compose up -d postgres redis airflow-apiserver airflow-scheduler airflow-worker-1 airflow-worker-2

# Step 2: Later, add Spark cluster
./start-spark-cluster.sh
```

**Use Case**: Gradual rollout, resource-constrained environments

### Option 3: Spark Cluster Only

If Airflow is already running:

```bash
# Start Spark services only
docker compose up -d spark-master spark-worker-1 spark-worker-2

# Or use the helper script
./start-spark-cluster.sh
```

**Use Case**: Restart Spark after configuration changes

---

## Monitoring & Verification

### Health Checks

All services have built-in health checks:

```bash
# Check health status
docker compose ps

# View specific service health
docker inspect spark-master | grep -A 10 Health
```

### Resource Monitoring

```bash
# Real-time resource usage
docker stats

# Spark-specific monitoring
docker stats spark-master spark-worker-1 spark-worker-2

# All services
docker compose top
```

### Log Monitoring

```bash
# All services
docker compose logs -f

# Specific service
docker logs -f spark-master
docker logs -f airflow-scheduler

# Last 100 lines
docker logs --tail 100 spark-worker-1
```

### Spark Cluster Status

```bash
# Check alive workers
docker exec spark-master curl -s http://localhost:8080/json/ | grep aliveworkers

# Detailed cluster info
docker exec spark-master curl -s http://localhost:8080/json/ | jq .
```

### Application Metrics

Check the Spark application UI at http://localhost:4040 when a job is running to see:
- Job stages and tasks
- Memory usage
- Executor status
- DAG visualization

---

## Configuration

### Environment Variables

Key environment variables in `docker-compose.yaml`:

#### Spark Master
```yaml
SPARK_MASTER_HOST: spark-master
SPARK_MASTER_PORT: 7077
SPARK_DRIVER_MEMORY: 4g
SPARK_DRIVER_CORES: 2
SPARK_NETWORK_TIMEOUT: 600s
```

#### Spark Workers
```yaml
SPARK_WORKER_MEMORY: 12G
SPARK_EXECUTOR_MEMORY: 10G
SPARK_EXECUTOR_CORES: 3
SPARK_NETWORK_TIMEOUT: 600s
```

### Airflow Spark Connection

The connection is auto-configured by `start-spark-cluster.sh`, but you can manually add it:

**Via CLI:**
```bash
docker exec airflow-apiserver airflow connections add \
  spark_default \
  --conn-type spark \
  --conn-host spark://spark-master \
  --conn-port 7077
```

**Via UI:**
1. Go to Admin → Connections
2. Click "+"
3. Fill in:
   - Connection Id: `spark_default`
   - Connection Type: `Spark`
   - Host: `spark://spark-master`
   - Port: `7077`

### Tuning Resource Limits

Edit `docker-compose.yaml` to adjust resources:

```yaml
services:
  spark-worker-1:
    deploy:
      resources:
        limits:
          memory: 14G      # Adjust as needed
          cpus: '4.0'      # Adjust as needed
        reservations:
          memory: 10G
          cpus: '3.0'
```

**After changes:**
```bash
docker compose down
docker compose up -d
```

---

## Troubleshooting

### Issue: Spark Master Not Accessible

**Symptoms:**
- Cannot access http://localhost:9090
- Workers not registering

**Solution:**
```bash
# Check if container is running
docker ps | grep spark-master

# Check logs
docker logs spark-master

# Verify port binding
docker port spark-master

# Restart
docker compose restart spark-master
```

### Issue: Workers Not Registering

**Symptoms:**
- Spark UI shows 0 alive workers
- Workers container running but not connected

**Solution:**
```bash
# Check worker logs
docker logs spark-worker-1
docker logs spark-worker-2

# Verify network connectivity
docker exec spark-worker-1 ping -c 3 spark-master

# Check master URL in worker logs
docker logs spark-worker-1 | grep "Master URL"

# Restart workers
docker compose restart spark-worker-1 spark-worker-2
```

### Issue: Out of Memory

**Symptoms:**
- Spark jobs failing with OOM
- Container restarts
- Slow performance

**Solution:**
```bash
# Check memory usage
docker stats --no-stream

# Increase worker memory in docker-compose.yaml
# Reduce executor memory or cores per task in DAG

# Restart with new limits
docker compose down
docker compose up -d
```

### Issue: Connection Timeout

**Symptoms:**
- Spark jobs timeout
- "Lost task" errors

**Solution:**
```bash
# Increase timeout in docker-compose.yaml
SPARK_NETWORK_TIMEOUT: 900s  # Increase from 600s

# Check network latency
docker exec spark-master ping -c 10 spark-worker-1

# Restart cluster
docker compose restart spark-master spark-worker-1 spark-worker-2
```

### Issue: Port Conflicts

**Symptoms:**
- "Port already in use" error
- Services fail to start

**Solution:**
```bash
# Find process using port
sudo lsof -i :9090
sudo lsof -i :8081

# Kill process or change port in docker-compose.yaml
# Example: Change Spark Master UI port
ports:
  - "9091:8080"  # Changed from 9090 to 9091
```

### Issue: Image Build Failures

**Symptoms:**
- "failed to build" error
- Missing dependencies

**Solution:**
```bash
# Rebuild without cache
docker compose build --no-cache spark-master

# Check Dockerfile.spark
cat Dockerfile.spark

# Ensure base image is available
docker pull apache/spark:3.5.0
```

---

## Best Practices

### 1. Production Deployment

```bash
# Use specific image tags (not latest)
# Set resource limits appropriate to your hardware
# Enable health checks (already configured)
# Use volumes for persistence (already configured)
# Monitor logs and metrics
# Set up alerting (use Prometheus/Grafana)
```

### 2. Development vs Production

**Development:**
```yaml
# Lower resource limits
spark-worker-1:
  deploy:
    resources:
      limits:
        memory: 8G
        cpus: '2.0'
```

**Production:**
```yaml
# Full resource allocation
spark-worker-1:
  deploy:
    resources:
      limits:
        memory: 14G
        cpus: '4.0'
```

### 3. Backup & Recovery

```bash
# Backup volumes
docker run --rm -v ssg-etl_spark-master-data:/data -v $(pwd):/backup \
  alpine tar czf /backup/spark-master-backup.tar.gz -C /data .

# Restore volumes
docker run --rm -v ssg-etl_spark-master-data:/data -v $(pwd):/backup \
  alpine tar xzf /backup/spark-master-backup.tar.gz -C /data
```

### 4. Scaling Workers

To add a third worker:

```yaml
# Add to docker-compose.yaml
spark-worker-3:
  image: spark-custom:3.5.0
  container_name: spark-worker-3
  hostname: spark-worker-3
  command: /opt/spark/bin/spark-class org.apache.spark.deploy.worker.Worker spark://spark-master:7077 --memory 12G --cores 4
  # ... (copy config from worker-1, change ports)
  ports:
    - "8083:8081"
```

### 5. Security Considerations

```bash
# Change default Airflow credentials
# Set in .env file
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=<strong-password>

# Enable SSL for Spark
# Configure authentication for Spark UI
# Use secrets management for database credentials
# Restrict network access with firewall rules
```

### 6. Performance Tuning

**For large datasets:**
```yaml
# Increase executor memory
SPARK_EXECUTOR_MEMORY: 12G

# Increase driver memory
SPARK_DRIVER_MEMORY: 6g

# Adjust parallelism in Spark job
spark.sql.shuffle.partitions: 200
```

**For many small tasks:**
```yaml
# Reduce executor memory
SPARK_EXECUTOR_MEMORY: 4G

# Increase executor count by reducing cores
SPARK_EXECUTOR_CORES: 2
```

---

## Maintenance Commands

### Regular Maintenance

```bash
# Clean up unused images
docker image prune -a

# Clean up volumes (CAUTION: removes data)
docker volume prune

# View disk usage
docker system df

# Full cleanup (CAUTION: removes everything)
docker system prune -a --volumes
```

### Updates and Upgrades

```bash
# Pull latest base images
docker compose pull

# Rebuild custom images
docker compose build --no-cache

# Restart with new images
docker compose down
docker compose up -d
```

### Backup Before Maintenance

```bash
# Stop services
docker compose stop

# Backup volumes
docker run --rm \
  -v ssg-etl_postgres-db-volume:/data \
  -v $(pwd):/backup \
  alpine tar czf /backup/postgres-backup-$(date +%Y%m%d).tar.gz -C /data .

# Resume services
docker compose start
```

---

## Next Steps

1. **Test the deployment**: Run the sample DAG `hanger_lines_data_7A_cluster`
2. **Monitor performance**: Check Spark UI during job execution
3. **Optimize configuration**: Adjust resources based on actual usage
4. **Set up monitoring**: Integrate with Prometheus/Grafana (see monitoring/*)
5. **Configure alerting**: Set up alerts for failures and resource issues

---

## Support & Documentation

- **Spark Documentation**: https://spark.apache.org/docs/3.5.0/
- **Airflow Documentation**: https://airflow.apache.org/docs/apache-airflow/2.11.0/
- **Docker Compose**: https://docs.docker.com/compose/

## Related Files

- `docker-compose.yaml` - Main deployment configuration
- `start-spark-cluster.sh` - Spark startup helper script
- `Dockerfile` - Airflow custom image with Spark client
- `Dockerfile.spark` - Spark custom image
- `dags/hanger_lines_data_7A.py` - Sample Spark ETL DAG
- `sparkFiles/hangerline_transform_spark_7A.py` - Spark transformation logic

---

**Last Updated**: December 30, 2024  
**Version**: 1.0  
**Deployment Type**: Integrated Airflow + Spark
