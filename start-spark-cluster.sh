#!/bin/bash

################################################################################
# Spark Cluster Startup Script - Integrated Deployment
# Starts Spark Master + 2 Workers from docker-compose.yaml
# Part of the integrated Airflow + Spark setup
################################################################################

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

echo "================================================================================"
echo "         Spark Cluster Setup - Integrated Airflow + Spark"
echo "================================================================================"

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

print_step() {
    echo -e "${BLUE}[STEP]${NC} $1"
}

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    print_error "Docker is not running. Please start Docker first."
    exit 1
fi

print_info "Docker is running ✓"

# Check if docker-compose.yaml exists
if [ ! -f "docker-compose.yaml" ]; then
    print_error "docker-compose.yaml not found in current directory!"
    exit 1
fi

print_info "docker-compose.yaml found ✓"

# Check if Spark custom image exists
if ! docker images | grep -q "apache/spark:3.5.0"; then
    print_warn "Spark custom image not found. Building it now..."
    docker compose build spark-master
    print_info "Spark custom image built ✓"
else
    print_info "Spark custom image exists ✓"
fi

# Stop existing Spark containers if any
print_step "Stopping existing Spark containers (if any)..."
docker compose stop spark-master spark-worker-1 spark-worker-2 2>/dev/null || true
docker compose rm -f spark-master spark-worker-1 spark-worker-2 2>/dev/null || true

# Start Spark cluster (only Spark services)
print_step "Starting Spark cluster (1 Master + 2 Workers)..."
docker compose up -d spark-master spark-worker-1 spark-worker-2

# Wait for Spark Master to start
print_step "Waiting for Spark Master to start..."
sleep 5

# Check if containers are running
print_step "Checking container status..."

MASTER_STATUS=$(docker inspect -f '{{.State.Running}}' spark-master 2>/dev/null || echo "false")
WORKER1_STATUS=$(docker inspect -f '{{.State.Running}}' spark-worker-1 2>/dev/null || echo "false")
WORKER2_STATUS=$(docker inspect -f '{{.State.Running}}' spark-worker-2 2>/dev/null || echo "false")

echo ""
echo "================================================================================"
echo "                        Container Status"
echo "================================================================================"

if [ "$MASTER_STATUS" = "true" ]; then
    print_info "✓ Spark Master: Running"
else
    print_error "✗ Spark Master: Not running"
fi

if [ "$WORKER1_STATUS" = "true" ]; then
    print_info "✓ Spark Worker 1: Running"
else
    print_error "✗ Spark Worker 1: Not running"
fi

if [ "$WORKER2_STATUS" = "true" ]; then
    print_info "✓ Spark Worker 2: Running"
else
    print_error "✗ Spark Worker 2: Not running"
fi

echo ""
echo "================================================================================"
echo "                        Access URLs"
echo "================================================================================"
echo "Spark Master UI:    http://localhost:9090"
echo "Spark Worker 1 UI:  http://localhost:8081"
echo "Spark Worker 2 UI:  http://localhost:8082"
echo "Spark App UI:       http://localhost:4040 (when job is running)"
echo "================================================================================"

# Wait for Spark Master to be healthy
print_step "Waiting for Spark Master to be ready..."
for i in {1..30}; do
    if curl -s http://localhost:9090 > /dev/null 2>&1; then
        print_info "Spark Master UI is accessible ✓"
        break
    fi
    if [ $i -eq 30 ]; then
        print_error "Spark Master did not become ready in time"
        print_error "Check logs with: docker logs spark-master"
        exit 1
    fi
    sleep 2
done

# Wait for workers to register
print_step "Waiting for workers to register with master..."
sleep 8

# Check worker registration
print_info "Checking worker registration..."
ALIVE_WORKERS=$(docker exec spark-master curl -s http://localhost:8080/json/ 2>/dev/null | grep -o '"aliveworkers":[0-9]*' | grep -o '[0-9]*' || echo "0")

if [ "$ALIVE_WORKERS" -eq 2 ]; then
    print_info "✓ All 2 workers successfully registered with master"
elif [ "$ALIVE_WORKERS" -eq 1 ]; then
    print_warn "⚠ Only 1 worker registered. Waiting a bit longer..."
    sleep 10
    ALIVE_WORKERS=$(docker exec spark-master curl -s http://localhost:8080/json/ 2>/dev/null | grep -o '"aliveworkers":[0-9]*' | grep -o '[0-9]*' || echo "0")
    if [ "$ALIVE_WORKERS" -eq 2 ]; then
        print_info "✓ All 2 workers now registered"
    else
        print_warn "⚠ Still only $ALIVE_WORKERS worker(s) registered. Check Spark Master UI."
    fi
else
    print_warn "⚠ Workers may still be registering. Check Spark Master UI for status."
fi

echo ""
echo "================================================================================"
echo "                        Useful Commands"
echo "================================================================================"
echo "View logs:"
echo "  docker logs spark-master"
echo "  docker logs spark-worker-1"
echo "  docker logs spark-worker-2"
echo ""
echo "Stop Spark cluster only:"
echo "  docker compose stop spark-master spark-worker-1 spark-worker-2"
echo ""
echo "Restart Spark cluster:"
echo "  docker compose restart spark-master spark-worker-1 spark-worker-2"
echo ""
echo "Stop entire environment (Airflow + Spark):"
echo "  docker compose down"
echo ""
echo "Start entire environment:"
echo "  docker compose up -d"
echo ""
echo "Check cluster status:"
echo "  docker compose ps | grep spark"
echo ""
echo "Monitor resources:"
echo "  docker stats spark-master spark-worker-1 spark-worker-2"
echo ""
echo "View all services:"
echo "  docker compose ps"
echo "================================================================================"

# Check if Airflow is running
print_step "Checking Airflow integration..."
if docker ps --format '{{.Names}}' | grep -q "airflow-apiserver"; then
    print_info "Airflow is running ✓"
    
    # Wait a moment for Airflow to be fully ready
    sleep 3
    
    # Try to configure Airflow Spark connection
    print_info "Configuring Airflow Spark connection..."
    
    # Delete existing connection if present
    docker exec airflow-apiserver airflow connections delete spark_default 2>/dev/null || true
    
    # Add new connection
    if docker exec airflow-apiserver airflow connections add \
        spark_default \
        --conn-type spark \
        --conn-host spark://spark-master \
        --conn-port 7077 2>/dev/null; then
        print_info "Airflow connection 'spark_default' configured ✓"
    else
        print_warn "Could not auto-configure Airflow connection."
        print_warn "You can add it manually via Airflow UI:"
        print_warn "  Connection ID: spark_default"
        print_warn "  Connection Type: Spark"
        print_warn "  Host: spark://spark-master"
        print_warn "  Port: 7077"
    fi
else
    print_info "Airflow is not running yet."
    print_info "To start the full environment: docker compose up -d"
    print_warn "Remember to configure 'spark_default' connection in Airflow UI after startup."
fi

echo ""
echo "================================================================================"
echo "                     🚀 Spark Cluster Ready!"
echo "================================================================================"
print_info "Spark Master UI:    http://localhost:9090"
print_info "Spark Worker 1 UI:  http://localhost:8081"
print_info "Spark Worker 2 UI:  http://localhost:8082"

# Check if Airflow webserver is accessible
if docker ps --format '{{.Names}}' | grep -q "airflow-apiserver"; then
    if curl -s http://localhost:8088/health > /dev/null 2>&1; then
        print_info "Airflow UI:         http://localhost:8088"
    fi
fi

echo ""

# Show cluster resources
echo "================================================================================"
echo "                        Cluster Resources Summary"
echo "================================================================================"

# Get resource info from Spark Master
RESOURCE_INFO=$(docker exec spark-master curl -s http://localhost:8080/json/ 2>/dev/null)

if [ -n "$RESOURCE_INFO" ]; then
    ALIVE_WORKERS=$(echo "$RESOURCE_INFO" | grep -o '"aliveworkers":[0-9]*' | grep -o '[0-9]*' || echo "0")
    TOTAL_CORES=$(echo "$RESOURCE_INFO" | grep -o '"cores":[0-9]*' | head -1 | grep -o '[0-9]*' || echo "N/A")
    USED_CORES=$(echo "$RESOURCE_INFO" | grep -o '"coresused":[0-9]*' | head -1 | grep -o '[0-9]*' || echo "0")
    TOTAL_MEM=$(echo "$RESOURCE_INFO" | grep -o '"memory":[0-9]*' | head -1 | grep -o '[0-9]*' || echo "N/A")
    
    echo "Alive Workers:    $ALIVE_WORKERS / 2"
    echo "Total CPU Cores:  $TOTAL_CORES cores"
    echo "Used CPU Cores:   $USED_CORES cores"
    if [ "$TOTAL_MEM" != "N/A" ]; then
        TOTAL_MEM_GB=$(echo "scale=1; $TOTAL_MEM/1024" | bc 2>/dev/null || echo "$TOTAL_MEM MB")
        echo "Total Memory:     ${TOTAL_MEM_GB} GB"
    fi
else
    echo "Resource information not available yet. Check Spark Master UI."
fi

echo "================================================================================"
echo ""
print_info "✓ Setup complete! You can now submit Spark jobs via Airflow."
print_info "  DAG Example: hanger_lines_data_7A_cluster"
echo ""

exit 0
