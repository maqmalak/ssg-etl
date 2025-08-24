import os
import json
from kafka import KafkaConsumer
import pyodbc
from datetime import datetime

# Environment variables
KAFKA_BROKER = os.getenv('KAFKA_BROKER', 'kafka:9092')
SOURCE_DSN = 'DRIVER={FreeTDS};SERVER=172.16.7.6;PORT=1433;DATABASE=SilverStr;UID=sa;PWD=P@kistan12;TDS_Version=7.0;Encrypt=no;MARS_Connection=no;UseLegacyDateTime=1'
TARGET_DSN = 'DRIVER={FreeTDS};SERVER=172.16.7.4;PORT=1433;DATABASE=SilverStr;UID=sa;PWD=P@kistan12;TDS_Version=7.0;Encrypt=no;MARS_Connection=no;UseLegacyDateTime=1'

# Connect to databases
source_conn = pyodbc.connect(SOURCE_DSN)
target_conn = pyodbc.connect(TARGET_DSN)
target_cursor = target_conn.cursor()

# Create target tables
tables = {
    'sales_master': """
        CREATE TABLE sales_master (
            id INT PRIMARY KEY,
            date DATETIME,
            customer_id VARCHAR(50),
            customer_name VARCHAR(255)
        )
    """,
    'sales_items': """
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
for table_name, create_query in tables.items():
    target_cursor.execute(f"""
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='{table_name}' AND xtype='U')
        {create_query}
    """)
target_conn.commit()

# Kafka consumer for both tables
consumer = KafkaConsumer(
    'sqlserver.silverstr.sales_master', 'sqlserver.silverstr.sales_items',
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset='earliest',
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

# Replicate data
for message in consumer:
    table_name = message.topic.split('.')[-1]
    record = message.value
    if table_name == 'sales_master':
        columns = ['id', 'date', 'customer_id', 'customer_name']
        values = (
            record.get('id'),
            datetime.strptime(record.get('date', '2000-01-01 00:00:00'), '%Y-%m-%d %H:%M:%S') if record.get('date') else datetime.now(),
            record.get('customer_id', ''),
            record.get('customer_name', '')
        )
    elif table_name == 'sales_items':
        columns = ['id', 'item_code', 'item_name', 'qty', 'rate', 'amount']
        values = (
            record.get('id'),
            record.get('item_code', ''),
            record.get('item_name', ''),
            record.get('qty', 0),
            record.get('rate', 0.0),
            record.get('amount', 0.0)
        )
    else:
        continue

    placeholders = ', '.join(['?' for _ in columns])
    target_cursor.execute(
        f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({placeholders})",
        values
    )
    target_conn.commit()

# Cleanup
target_cursor.close()
source_conn.close()
target_conn.close()
