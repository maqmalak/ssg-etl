from faker import Faker
import random
import psycopg2
from datetime import datetime, timedelta
from airflow.decorators import dag, task

fake = Faker()

# Function to generate a transaction
def generate_transaction():
    user = fake.simple_profile()
    return {
        "transactionId": str(fake.uuid4()),
        "userId": user['username'],
        "timestamp": datetime.utcnow(),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(['USD', 'GBP']),
        'city': fake.city(),
        "country": fake.country(),
        "merchantName": fake.company(),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer']),
        "ipAddress": fake.ipv4(),
        "voucherCode": random.choice(['', 'DISCOUNT10', '']),
        'affiliateId': str(fake.uuid4())
    }

# Function to create the transactions table
def create_table(conn):
    cursor = conn.cursor()
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS transactions (
            transaction_id VARCHAR(255) PRIMARY KEY,
            user_id VARCHAR(255),
            timestamp TIMESTAMP,
            amount DECIMAL,
            currency VARCHAR(255),
            city VARCHAR(255),
            country VARCHAR(255),
            merchant_name VARCHAR(255),
            payment_method VARCHAR(255),
            ip_address VARCHAR(255),
            voucher_code VARCHAR(255),
            affiliateId VARCHAR(255)
        )
        """)
    cursor.close()
    conn.commit()

# Airflow DAG definition
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=24*60)
}

@dag(
    dag_id='sales_data_etl',
    default_args=default_args,
    schedule="*/5 * * * *",  # Run 1 minus
    tags=["ssg", "sales"],
    catchup=False,
)
def sales_data():
    @task
    def initialize_task():
        # Generate a transaction
        transaction = generate_transaction()
        print(f"Generated transaction: {transaction}")
        return transaction

    @task
    def insert_transaction(transaction):
        # Connect to PostgreSQL
        conn = psycopg2.connect(
            host='postgres_grafana',
            database='ssg',
            user='postgres',
            password='postgres',
            port=5432
        )
        try:
            # Create table if it doesn't exist
            create_table(conn)
            # Insert transaction
            cur = conn.cursor()
            cur.execute(
                """
                INSERT INTO transactions(transaction_id, user_id, timestamp, amount, currency, city, country, merchant_name, payment_method, 
                ip_address, affiliateId, voucher_code)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    transaction["transactionId"],
                    transaction["userId"],
                    transaction["timestamp"],
                    transaction["amount"],
                    transaction["currency"],
                    transaction["city"],
                    transaction["country"],
                    transaction["merchantName"],
                    transaction["paymentMethod"],
                    transaction["ipAddress"],
                    transaction["affiliateId"],
                    transaction["voucherCode"]
                )
            )
            cur.close()
            conn.commit()
            print(f"Inserted transaction: {transaction['transactionId']}")
        except Exception as e:
            print(f"Error inserting transaction: {e}")
            raise
        finally:
            conn.close()

    # Define task dependencies
    transaction_data = initialize_task()
    insert_transaction(transaction_data)

# Instantiate the DAG
dag = sales_data()