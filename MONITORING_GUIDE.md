# ETL Pipeline Monitoring Guide

**Monitoring Stack:** Prometheus + Grafana + Exporters  
**Date:** December 24, 2025  
**Version:** 1.0

---

## 📊 Monitoring Stack Overview

Your ETL pipeline now includes a comprehensive monitoring solution with:

- **Prometheus** - Metrics collection and storage
- **Grafana** - Visualization dashboards
- **AlertManager** - Alert routing and management
- **Node Exporter** - System metrics (CPU, RAM, disk)
- **PostgreSQL Exporters** - Database performance metrics
- **Redis Exporter** - Cache and queue metrics
- **StatsD Exporter** - Airflow application metrics
- **cAdvisor** - Docker container metrics

---

## 🚀 Quick Start

### Start Monitoring Stack

```bash
# Start main ETL pipeline + monitoring (includes override file)
docker compose -f docker compose.yaml -f docker compose.override.yml -f docker compose.monitoring.yml up -d

# Or start with AlertManager for alerting
docker compose -f docker compose.yaml -f docker compose.override.yml -f docker compose.monitoring.yml --profile alerting up -d

# Check monitoring services status
docker compose -f docker compose.monitoring.yml ps

# Check all services including ETL
docker compose -f docker compose.yaml -f docker compose.override.yml -f docker compose.monitoring.yml ps
```

### Stop Monitoring Stack

```bash
# Stop only monitoring services
docker compose -f docker compose.monitoring.yml down

# Stop everything (ETL + monitoring)
docker compose -f docker compose.yaml -f docker compose.override.yml -f docker compose.monitoring.yml down
```

---

## 🔍 Access Monitoring Services

| Service | URL | Credentials | Purpose |
|---------|-----|-------------|---------|
| **Grafana** | http://localhost:3000 | admin / admin | Dashboards & visualization |
| **Prometheus** | http://localhost:9091 | None | Metrics query & explore |
| **AlertManager** | http://localhost:9093 | None | Alert management |
| **cAdvisor** | http://localhost:8080 | None | Container metrics |
| **Node Exporter** | http://localhost:9100/metrics | None | System metrics (raw) |
| **StatsD Exporter** | http://localhost:9102/metrics | None | Airflow metrics (raw) |

---

## 📈 Resource Allocation

### Monitoring Services Resources

| Service | Memory | CPU | Notes |
|---------|--------|-----|-------|
| Prometheus | 1GB | 0.5 | 30 days retention |
| Grafana | 512MB | 0.25 | Includes plugins |
| Node Exporter | 128MB | 0.1 | System metrics |
| PostgreSQL Exporters (2x) | 256MB each | 0.1 each | DB metrics |
| Redis Exporter | 256MB | 0.1 | Queue metrics |
| StatsD Exporter | 256MB | 0.15 | Airflow metrics |
| cAdvisor | 256MB | 0.2 | Container metrics |
| AlertManager | 256MB | 0.15 | Alert routing |
| **Total** | **~2.7GB** | **~1.7 CPUs** | - |

**Total System Usage:** 62.5GB (ETL) + 2.7GB (Monitoring) = **65.2GB / 64GB**  
**Note:** Monitoring adds 2.7GB RAM and 1.7 CPU overhead

---

## 📊 Key Metrics Collected

### System Metrics (Node Exporter)

- **CPU:** Usage per core, load averages
- **Memory:** Used, available, buffers, cache
- **Disk:** Usage, I/O rates, IOPS
- **Network:** Bandwidth, packets, errors

### Airflow Metrics (StatsD Exporter)

- **DAG Metrics:** Parse time, import errors, processing duration
- **Task Metrics:** Success/failure rates, duration, queued tasks
- **Scheduler Metrics:** Heartbeat, task slots, starving tasks
- **Executor Metrics:** Open slots, queued/running tasks
- **Pool Metrics:** Slot utilization per pool
- **Celery Metrics:** Worker status, task timeouts

### Database Metrics (PostgreSQL Exporters)

- **Connections:** Active, idle, max connections
- **Performance:** Query duration, cache hit ratio
- **Database Size:** Table/index sizes, bloat
- **Transactions:** Commits, rollbacks, deadlocks
- **Replication:** Lag, streaming status

### Redis Metrics (Redis Exporter)

- **Memory:** Used, peak, fragmentation
- **Commands:** Ops/sec per command type
- **Connections:** Active, rejected
- **Keys:** Total keys, expiring keys
- **Persistence:** RDB/AOF status

### Container Metrics (cAdvisor)

- **Per Container:** CPU, memory, network, disk I/O
- **Resource Limits:** Usage vs limits
- **Restarts:** Container restart counts

### Spark Metrics

- **Jobs:** Running, succeeded, failed
- **Executors:** Active, memory usage
- **Stages:** Duration, tasks per stage

---

## 🎯 Pre-Built Dashboards

### 1. System Overview Dashboard

**Metrics:**
- Overall CPU and memory usage
- Disk space and I/O
- Network traffic
- Container resource usage

**Use Case:** General system health monitoring

### 2. Airflow Performance Dashboard

**Metrics:**
- Task success/failure rates
- DAG processing times
- Scheduler performance
- Queue depths and worker utilization
- Pool slot usage

**Use Case:** Monitor Airflow pipeline performance

### 3. Database Performance Dashboard

**Metrics:**
- Connection pool usage
- Query performance (slow queries)
- Cache hit ratios
- Database size growth
- Transaction rates

**Use Case:** Database health and optimization

### 4. Redis Dashboard

**Metrics:**
- Memory usage and trends
- Command throughput
- Connection statistics
- Key statistics

**Use Case:** Cache and queue monitoring

### 5. Container Resources Dashboard

**Metrics:**
- Per-container CPU/memory usage
- Container restarts
- Resource limit breaches
- Network I/O per container

**Use Case:** Docker resource optimization

---

## 🚨 Alert Rules

### Critical Alerts

| Alert | Condition | Action Required |
|-------|-----------|-----------------|
| **PostgreSQL Down** | Database unreachable for 1min | Immediate investigation |
| **Redis Down** | Redis unreachable for 1min | Check Redis service |
| **Airflow Scheduler Down** | No metrics for 2min | Restart scheduler |
| **Airflow Worker Down** | Worker unresponsive for 3min | Restart worker |
| **Disk Space Low** | < 15% free | Clean up disk space |
| **Spark Master Down** | Master unreachable for 2min | Check Spark service |

### Warning Alerts

| Alert | Condition | Action |
|-------|-----------|--------|
| **High CPU Usage** | > 80% for 5min | Investigate load |
| **High Memory Usage** | > 85% for 5min | Check for memory leaks |
| **Task Failure Rate High** | > 10% failures | Review failed tasks |
| **Queue Backlog** | > 100 queued tasks for 10min | Scale workers |
| **DB Connection Pool High** | > 80% connections | Review queries |
| **Redis Memory High** | > 90% used | Increase memory or evict keys |
| **Slow Queries** | Avg > 1000ms | Optimize queries |

---

## 🔧 Configuration Files

### Prometheus Configuration

**File:** `monitoring/prometheus/prometheus.yml`

```yaml
# Key settings:
scrape_interval: 15s       # How often to scrape metrics
evaluation_interval: 15s   # How often to evaluate rules
retention: 30d             # How long to keep metrics
```

**Alert Rules:** `monitoring/prometheus/alerts.yml`

### Grafana Configuration

**Datasources:** `monitoring/grafana/provisioning/datasources/prometheus.yml`  
**Dashboards:** `monitoring/grafana/provisioning/dashboards/`

**Default Credentials:**
- Username: `admin`
- Password: `admin`

### StatsD Mapping

**File:** `monitoring/statsd_mapping.yml`

Maps Airflow StatsD metrics to Prometheus format.

### AlertManager Configuration

**File:** `monitoring/alertmanager/config.yml`

Configure notification channels (email, Slack, PagerDuty, etc.)

---

## 📝 Creating Custom Dashboards

### 1. Access Grafana

```bash
# Open browser
http://localhost:3000

# Login with admin/admin
# You'll be prompted to change password
```

### 2. Create New Dashboard

1. Click **"+"** → **"Dashboard"**
2. Click **"Add new panel"**
3. Select **Prometheus** as data source
4. Enter PromQL query

### 3. Useful PromQL Queries

**System CPU Usage:**
```promql
100 - (avg by(instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)
```

**Memory Usage:**
```promql
(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100
```

**Airflow Task Success Rate:**
```promql
rate(airflow_task_instance_successes[5m])
```

**Airflow Queue Depth:**
```promql
airflow_executor_queued_tasks
```

**Database Connections:**
```promql
pg_stat_database_numbackends
```

**Redis Memory Usage:**
```promql
redis_memory_used_bytes / redis_memory_max_bytes * 100
```

**Container CPU:**
```promql
rate(container_cpu_usage_seconds_total{name!=""}[5m]) * 100
```

**Container Memory:**
```promql
container_memory_usage_bytes{name!=""} / container_spec_memory_limit_bytes{name!=""} * 100
```

---

## 🔔 Setting Up Notifications

### Email Notifications

Edit `monitoring/alertmanager/config.yml`:

```yaml
receivers:
  - name: 'email-alerts'
    email_configs:
      - to: 'your-email@example.com'
        from: 'alertmanager@example.com'
        smarthost: 'smtp.gmail.com:587'
        auth_username: 'your-email@example.com'
        auth_password: 'your-app-password'
        headers:
          Subject: 'ETL Pipeline Alert: {{ .GroupLabels.alertname }}'
```

### Slack Notifications

```yaml
receivers:
  - name: 'slack-alerts'
    slack_configs:
      - api_url: 'https://hooks.slack.com/services/YOUR/SLACK/WEBHOOK'
        channel: '#etl-alerts'
        title: 'ETL Alert: {{ .GroupLabels.alertname }}'
        text: '{{ range .Alerts }}{{ .Annotations.summary }}{{ end }}'
```

### Webhook Notifications

```yaml
receivers:
  - name: 'webhook-alerts'
    webhook_configs:
      - url: 'http://your-webhook-url/alerts'
        send_resolved: true
```

---

## 🧪 Testing Monitoring

### 1. Verify Prometheus Targets

```bash
# Open Prometheus UI
http://localhost:9091/targets

# All targets should show "UP" status:
# ✓ prometheus
# ✓ node-exporter
# ✓ postgres-airflow
# ✓ postgres-warehouse
# ✓ redis
# ✓ airflow-statsd
# ✓ cadvisor
```

### 2. Check Metrics in Prometheus

```bash
# Open Prometheus graph
http://localhost:9091/graph

# Test queries:
up                                    # All services up
node_memory_MemAvailable_bytes        # System memory
airflow_executor_queued_tasks         # Airflow queue
pg_stat_database_numbackends          # DB connections
redis_connected_clients               # Redis clients
```

### 3. Verify Grafana Datasource

```bash
# Login to Grafana
http://localhost:3000

# Go to Configuration → Data Sources
# Click "Test" on Prometheus datasource
# Should show: "Data source is working"
```

### 4. Test Alerts

Trigger a test alert:

```bash
# Simulate high CPU (run in container)
docker exec -it airflow-scheduler sh -c "yes > /dev/null &"

# Wait 5 minutes, then check Prometheus alerts:
http://localhost:9091/alerts

# Check AlertManager:
http://localhost:9093/#/alerts

# Stop the test:
docker exec -it airflow-scheduler pkill yes
```

---

## 📊 Monitoring Best Practices

### 1. Regular Review

- **Daily:** Check dashboard for anomalies
- **Weekly:** Review alert trends
- **Monthly:** Analyze capacity trends

### 2. Capacity Planning

- Monitor growth trends
- Plan scaling before hitting 80% capacity
- Archive old metrics if storage grows large

### 3. Alert Tuning

- Adjust thresholds based on actual usage patterns
- Reduce noise from non-actionable alerts
- Ensure critical alerts wake you up

### 4. Dashboard Organization

- Create role-specific dashboards (ops, dev, business)
- Use folders to organize dashboards
- Share dashboards with team

### 5. Data Retention

- Prometheus: 30 days (configurable)
- Consider long-term storage (e.g., Thanos, Victoria Metrics)
- Archive critical metrics for compliance

---

## 🛠️ Troubleshooting

### Prometheus Not Scraping Targets

```bash
# Check Prometheus logs
docker compose -f docker compose.monitoring.yml logs prometheus

# Verify network connectivity
docker exec prometheus wget -O- http://node-exporter:9100/metrics

# Check configuration syntax
docker exec prometheus promtool check config /etc/prometheus/prometheus.yml
```

### Grafana Not Showing Data

```bash
# Check Grafana logs
docker compose -f docker compose.monitoring.yml logs grafana

# Verify Prometheus datasource
# Grafana → Configuration → Data Sources → Test

# Check time range in dashboard (top right)
# Ensure it includes recent data
```

### Missing Airflow Metrics

```bash
# Verify StatsD exporter is receiving metrics
curl http://localhost:9102/metrics | grep airflow

# Check Airflow services have StatsD enabled
docker compose -f docker compose.yaml -f docker compose.monitoring.yml config | grep STATSD

# Verify StatsD mapping configuration
docker exec statsd-exporter cat /etc/statsd/statsd_mapping.yml
```

### High Resource Usage

```bash
# Check Prometheus metrics size
du -sh /var/lib/docker/volumes/ssg-etl_prometheus-data

# Reduce scrape interval if needed
# Edit monitoring/prometheus/prometheus.yml
# Change scrape_interval from 15s to 30s or 60s

# Reduce retention period
# Edit prometheus command in docker compose.monitoring.yml
# Change --storage.tsdb.retention.time from 30d to 15d
```

---

## 📈 Scaling Monitoring

### Add More Exporters

```yaml
# Add to docker compose.monitoring.yml
services:
  custom-exporter:
    image: your-custom-exporter:latest
    ports:
      - "9999:9999"
    networks:
      - airflow_network
```

### Add to Prometheus Config

```yaml
# Add to monitoring/prometheus/prometheus.yml
scrape_configs:
  - job_name: 'custom-service'
    static_configs:
      - targets: ['custom-exporter:9999']
        labels:
          service: 'custom'
```

---

## 🎓 Learning Resources

### Prometheus

- Official Docs: https://prometheus.io/docs/
- Query Language: https://prometheus.io/docs/prometheus/latest/querying/basics/
- Best Practices: https://prometheus.io/docs/practices/

### Grafana

- Official Docs: https://grafana.com/docs/
- Dashboard Best Practices: https://grafana.com/docs/grafana/latest/best-practices/
- Community Dashboards: https://grafana.com/grafana/dashboards/

### Airflow Metrics

- Airflow Monitoring: https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/metrics.html
- StatsD Integration: https://airflow.apache.org/docs/apache-airflow/stable/logging-monitoring/metrics.html#statsd

---

## 📞 Support

**For Issues:**
1. Check service logs: `docker compose logs [service-name]`
2. Verify configurations in `monitoring/` directory
3. Review Prometheus targets: http://localhost:9091/targets
4. Check alert rules: http://localhost:9091/alerts

**Useful Commands:**
```bash
# Restart monitoring stack
docker compose -f docker compose.monitoring.yml restart

# View all monitoring logs
docker compose -f docker compose.monitoring.yml logs -f

# Check resource usage
docker stats
```

---

**Last Updated:** December 24, 2025  
**Version:** 1.0  
**Maintainer:** ETL Team
