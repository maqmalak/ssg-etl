# Dynamic Multi-Database ETL Pipeline

This Airflow DAG dynamically extracts data from multiple database sources and loads it into a centralized PostgreSQL database.

## How It Works

The DAG uses Airflow connections to define database sources. To add a new source:

1. Create a new Airflow connection in the Airflow UI or CLI
2. Add the connection ID to the `DB_CONNECTIONS` list in the DAG file
3. The DAG will automatically create a new task for the new connection

## Supported Databases

- PostgreSQL
- MySQL
- Microsoft SQL Server

## Setup

### 1. Create Airflow Connections

In the Airflow UI, go to Admin > Connections and create connections for each of your database sources.

For PostgreSQL:
- Connection Id: `your_postgres_source`
- Connection Type: `Postgres`
- Host: `your-host.com`
- Schema: `your_database`
- Login: `username`
- Password: `password`
- Port: `5432`

For MySQL:
- Connection Id: `your_mysql_source`
- Connection Type: `MySQL`
- Host: `your-host.com`
- Schema: `your_database`
- Login: `username`
- Password: `password`
- Port: `3306`

For MSSQL:
- Connection Id: `your_mssql_source`
- Connection Type: `Microsoft SQL Server`
- Host: `your-host.com`
- Schema: `your_database`
- Login: `username`
- Password: `password`
- Port: `1433`

### 2. Update Connection List

Add your connection IDs to the `DB_CONNECTIONS` list in `dynamic_db_etl.py`:

```python
DB_CONNECTIONS = [
    'your_postgres_source',
    'your_mysql_source',
    'your_mssql_source',
    # Add more connection IDs here
]
```

### 3. Customize Queries

Update the `fetch_data_from_source` function to match your source database schema:

```python
query = """
SELECT 
    user_id,
    timestamp,
    amount,
    currency,
    city,
    country,
    merchant_name,
    payment_method,
    ip_address,
    voucher_code,
    affiliate_id
FROM transactions 
WHERE timestamp >= NOW() - INTERVAL '1 DAY'
"""
```

## Database Schema

The destination PostgreSQL table will be created automatically with the following schema:

```sql
CREATE TABLE transactions (
    id SERIAL PRIMARY KEY,
    transaction_id UUID UNIQUE NOT NULL,
    user_id VARCHAR(255),
    timestamp TIMESTAMP,
    amount NUMERIC,
    currency VARCHAR(10),
    city VARCHAR(255),
    country VARCHAR(255),
    merchant_name VARCHAR(255),
    payment_method VARCHAR(50),
    ip_address VARCHAR(45),
    voucher_code VARCHAR(50),
    affiliate_id UUID,
    source_connection VARCHAR(255),
    created_at TIMESTAMP DEFAULT NOW()
);
```

## DAG Configuration

- **Schedule**: Every 30 minutes (`*/30 * * * *`)
- **Tags**: `ssg`, `multi_db_etl`
- **Timeout**: 120 minutes

## Troubleshooting

### Connection Issues
- Verify connection details in Airflow Connections
- Ensure network connectivity to database sources
- Check database credentials and permissions

### Data Issues
- Validate SQL query syntax for each database type
- Check column mappings between source and destination
- Ensure data types are compatible

## Extending Functionality

To add support for other database types:
1. Update the `get_db_engine` function to handle the new connection type
2. Install required database drivers in your Airflow environment
3. Update the requirements.txt file with new dependencies