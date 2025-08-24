#!/bin/bash

# Set environment variables for local testing
export POSTGRES_HOST=172.16.7.6
export POSTGRES_PORT=5432
export POSTGRES_DB=ssg
export POSTGRES_USER=postgres
export POSTGRES_PASSWORD=postgres

echo "Environment variables set for local testing:"
echo "POSTGRES_HOST: $POSTGRES_HOST"
echo "POSTGRES_PORT: $POSTGRES_PORT"
echo "POSTGRES_DB: $POSTGRES_DB"
echo "POSTGRES_USER: $POSTGRES_USER"
echo "POSTGRES_PASSWORD: ********"

echo ""
echo "To run the Python script, execute:"
echo "python /home/maqmalak/ETL/ssg-etl/create_and_save_data.py"