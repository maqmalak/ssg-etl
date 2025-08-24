import pyodbc
from datetime import datetime

# DSN connection strings for source and target databases
source_conn_str = (
    'DSN=SourceDSN;'
    'UID=sa;'
    'PWD=P@kistan12;'
    'TDS_Version=7.0;'
    'Encrypt=no;'
    'MARS_Connection=no;'
    'UseLegacyDateTime=1'
)
target_conn_str = (
    'DSN=TargetDSN;'
    'UID=sa;'
    'PWD=P@kistan12;'
    'TDS_Version=7.0;'
    'Encrypt=no;'
    'MARS_Connection=no;'
    'UseLegacyDateTime=1'
)

# Connect to source and target databases
try:
    source_conn = pyodbc.connect(source_conn_str)
    target_conn = pyodbc.connect(target_conn_str)
    source_cursor = source_conn.cursor()
    target_cursor = target_conn.cursor()
    print("Connected to both databases successfully!")
except pyodbc.Error as e:
    print(f"Connection error: {e}")
    exit(1)

# Function to create target table if it doesn't exist
def create_target_table(table_name, create_query):
    try:
        target_cursor.execute(f"""
            IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
            {create_query}
        """)
        target_conn.commit()
        print(f"Ensured {table_name} exists in target database.")
    except pyodbc.Error as e:
        print(f"Error creating {table_name}: {e}")
        raise

# Function to replicate a table
def replicate_table(table_name, columns):
    print(f"Replicating {table_name}...")
    try:
        # Extract data from source
        source_cursor.execute(f"SELECT {', '.join(columns)} FROM {table_name}")
        rows = source_cursor.fetchall()

        # Clear target table (optional, comment out for incremental replication)
        target_cursor.execute(f"DELETE FROM {table_name}")

        # Insert data into target
        placeholders = ', '.join(['?' for _ in columns])
        insert_query = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})"
        for row in rows:
            # Transform datetime to SQL Server 2000-compatible format
            transformed_row = [
                value if not isinstance(value, datetime) else value.strftime('%Y-%m-%d %H:%M:%S')
                for value in row
            ]
            target_cursor.execute(insert_query, transformed_row)

        target_conn.commit()
        print(f"Replicated {len(rows)} rows to {table_name}")
    except pyodbc.Error as e:
        print(f"Replication error for {table_name}: {e}")
        raise

# Define table schemas
tables = {
    'sales_master': {
        'columns': ['id', 'date', 'customer_id', 'customer_name'],
        'create_query': """
            CREATE TABLE sales_master (
                id INT PRIMARY KEY,
                date DATETIME,
                customer_id VARCHAR(50),
                customer_name VARCHAR(255)
            )
        """
    },
    'sales_items': {
        'columns': ['id', 'item_code', 'item_name', 'qty', 'rate', 'amount'],
        'create_query': """
            CREATE TABLE sales_items (
                id INT PRIMARY KEY,
                item_code VARCHAR(50),
                item_name VARCHAR(255),
                qty INT,
                rate FLOAT,
                amount FLOAT
            )
        """
    }
}

# Main replication process
try:
    # Create target tables
    for table_name, config in tables.items():
        create_target_table(table_name, config['create_query'])

    # Replicate tables
    for table_name, config in tables.items():
        replicate_table(table_name, config['columns'])

except Exception as e:
    print(f"Error during replication: {e}")
finally:
    # Cleanup
    source_cursor.close()
    target_cursor.close()
    source_conn.close()
    target_conn.close()
    print("Connections closed.")
