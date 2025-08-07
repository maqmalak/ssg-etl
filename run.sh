#!/bin/bash

set -e

DETACH=true
BUILD=false

# Parse CLI args
for arg in "$@"
do
  case $arg in
    --no-detach)
      DETACH=false
      shift
      ;;
    --build)
      BUILD=true
      shift
      ;;
    *)
      echo "❌ Unknown option: $arg"
      echo "Usage: $0 [--no-detach] [--build]"
      exit 1
      ;;
  esac
done

# Optional: Load .env file if it exists
if [ -f .env ]; then
  export $(grep -v '^#' .env | xargs)
fi

# Build only if --build is passed
if [ "$BUILD" = true ]; then
  echo "🔨 Building Airflow image..."
  docker compose build
else
  echo "📦 Using existing prebuilt image (no build)."
fi

# Up with or without detach mode
if [ "$DETACH" = true ]; then
  echo "🚀 Starting Airflow in detached mode..."
  docker compose up -d
else
  echo "🚀 Starting Airflow in attached mode..."
  docker compose up
fi
