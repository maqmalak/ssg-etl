#!/bin/bash
# Start ETL Pipeline with Monitoring Stack
# This script starts the complete stack including monitoring services

set -e

echo "========================================"
echo "Starting ETL Pipeline with Monitoring"
echo "========================================"
echo ""

# Check if docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Error: Docker is not running. Please start Docker Desktop."
    exit 1
fi

echo "✓ Docker is running"
echo ""

# Check if network exists, create if not
if ! docker network inspect ssg-etl_airflow_network > /dev/null 2>&1; then
    echo "Creating Docker network: ssg-etl_airflow_network"
    docker network create ssg-etl_airflow_network
    echo "✓ Network created"
else
    echo "✓ Network exists"
fi
echo ""

# Parse command line arguments
ALERTING=""
if [[ "$1" == "--with-alerting" ]]; then
    ALERTING="--profile alerting"
    echo "Starting with AlertManager enabled"
fi

# Start the stack
echo "Starting services..."
echo "This may take a few minutes on first run..."
echo ""

docker compose \
    -f docker-compose.yaml \
    -f docker-compose.override.yml \
    -f docker-compose.monitoring.yml \
    $ALERTING \
    up -d

echo ""
echo "========================================"
echo "✅ Startup Complete!"
echo "========================================"
echo ""
echo "📊 Monitoring Services:"
echo "   Grafana:      http://localhost:3000 (admin/admin)"
echo "   Prometheus:   http://localhost:9091"
echo "   AlertManager: http://localhost:9093 (if enabled)"
echo "   cAdvisor:     http://localhost:8080"
echo ""
echo "🚀 Airflow Services:"
echo "   Webserver:    http://localhost:8088"
echo "   Spark Master: http://localhost:9091"
echo "   Flower:       http://localhost:5555 (with --profile flower)"
echo ""
echo "📈 Check service status:"
echo "   docker compose -f docker-compose.yaml -f docker-compose.override.yml -f docker-compose.monitoring.yml ps"
echo ""
echo "📝 View logs:"
echo "   docker compose -f docker-compose.monitoring.yml logs -f [service-name]"
echo ""
echo "🛑 Stop services:"
echo "   ./stop-monitoring.sh"
echo ""
