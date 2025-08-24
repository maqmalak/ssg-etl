# SSG ETL Pipeline

## Overview
This project implements an ETL pipeline using Apache Airflow to extract data from multiple SQL Server databases and load it into a PostgreSQL database.

## Files
- `dags/dynamic_db_etl.py` - Main DAG file that orchestrates the ETL process
- `scripts/create_general_ledger_table.sql` - SQL script to create the target table
- `scripts/create_general_ledger_table.py` - Python script to create the target table
- `scripts/test_dag_functions.py` - Test script to verify DAG functions

## Setup Instructions

### 1. Database Setup
Before running the ETL pipeline, you need to create the target table in PostgreSQL:

#### Using SQL:
```sql
-- Connect to your PostgreSQL database and run:
\i scripts/create_general_ledger_table.sql
```

#### Using Python:
```bash
# Run the Python script to create the table
python3 scripts/create_general_ledger_table.py
```

### 2. Airflow Connections
Make sure you have configured the following connections in Airflow:
- `postgres_grafana` - Connection to the target PostgreSQL database
- `ssg-finance-1` - Connection to the first source SQL Server database
- `ssg-finance-2` - Connection to the second source SQL Server database

### 3. Running the Pipeline
The DAG is scheduled to run every 30 minutes. You can also trigger it manually from the Airflow UI.

## DAG Details
- **DAG ID**: `dynamic_multi_db_etl`
- **Schedule**: Every 30 minutes (`*/30 * * * *`)
- **Tasks**:
  1. `get_active_connections` - Gets the list of active database connections
  2. `extract_from_source` - Extracts data from each source (runs in parallel)
  3. `consolidate_and_load` - Consolidates data from all sources and loads to PostgreSQL

## Troubleshooting
- If you get connection errors, verify that all Airflow connections are properly configured
- Check the logs in the `logs/` directory for detailed error information
- Ensure that the source databases are accessible and the queries are correct