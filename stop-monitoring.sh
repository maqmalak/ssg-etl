#!/bin/bash
# Stop ETL Pipeline and Monitoring Stack
# This script stops all services including monitoring

set -e

echo "========================================"
echo "Stopping ETL Pipeline and Monitoring"
echo "========================================"
echo ""

# Parse command line arguments
REMOVE_VOLUMES=""
if [[ "$1" == "--remove-volumes" ]] || [[ "$1" == "-v" ]]; then
    REMOVE_VOLUMES="-v"
    echo "⚠️  WARNING: This will remove all data volumes!"
    echo "   Including: Prometheus data, Grafana dashboards, logs, etc."
    read -p "   Are you sure? (yes/no): " confirm
    if [[ "$confirm" != "yes" ]]; then
        echo "Cancelled."
        exit 0
    fi
    echo ""
fi

echo "Stopping services..."
echo ""

docker compose \
    -f docker-compose.yaml \
    -f docker-compose.override.yml \
    -f docker-compose.monitoring.yml \
    down $REMOVE_VOLUMES

echo ""
echo "========================================"
echo "✅ Services Stopped"
echo "========================================"
echo ""

if [[ -n "$REMOVE_VOLUMES" ]]; then
    echo "📦 Volumes removed. All data has been deleted."
else
    echo "📦 Data volumes preserved. Use --remove-volumes to delete data."
fi

echo ""
echo "🚀 To start again:"
echo "   ./start-monitoring.sh"
echo ""
