import faker
# import psycopg2
import random
import uuid
# from datetime import datetime
from datetime import datetime, timedelta

# from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.decorators import  dag, task


fake = faker.Faker()

def generate_transaction():
    user = fake.simple_profile()

    return {
        "transactionId": fake.uuid4(),
        "userId": user['username'],
        "timestamp": datetime.utcnow().timestamp(),
        "amount": round(random.uniform(10, 1000), 2),
        "currency": random.choice(['USD', 'GBP']),
        'city': fake.city(),
        "country": fake.country(),
        "merchantName": fake.company(),
        "paymentMethod": random.choice(['credit_card', 'debit_card', 'online_transfer']),
        "ipAddress": fake.ipv4(),
        "voucherCode": random.choice(['', 'DISCOUNT10', '']),
        'affiliateId': fake.uuid4()
    }

transaction = generate_transaction()
# print(transaction)


# def create_table(conn):
#     cursor = conn.cursor()

#     cursor.execute(
#         """
#         CREATE TABLE IF NOT EXISTS transactions (
#             transaction_id VARCHAR(255) PRIMARY KEY,
#             user_id VARCHAR(255),
#             timestamp TIMESTAMP,
#             amount DECIMAL,
#             currency VARCHAR(255),
#             city VARCHAR(255),
#             country VARCHAR(255),
#             merchant_name VARCHAR(255),
#             payment_method VARCHAR(255),
#             ip_address VARCHAR(255),
#             voucher_code VARCHAR(255),
#             affiliateId VARCHAR(255)
#         )
#         """)

#     cursor.close()
#     conn.commit()

# if __name__ == "__main__":
#     conn = psycopg2.connect(
#         host='localhost',
#         database='ssg',
#         user='postgres',
#         password='postgres',
#         port=5432
#     )

#     create_table(conn)

#     transaction = generate_transaction()
#     cur = conn.cursor()
#     print(transaction)

#     cur.execute(
#         """
#         INSERT INTO transactions(transaction_id, user_id, timestamp, amount, currency, city, country, merchant_name, payment_method, 
#         ip_address, affiliateId, voucher_code)
#         VALUES (%s, %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
#         """, (transaction["transactionId"], transaction["userId"], datetime.fromtimestamp(transaction["timestamp"]).strftime('%Y-%m-%d %H:%M:%S'),
#               transaction["amount"], transaction["currency"], transaction["city"], transaction["country"],
#               transaction["merchantName"], transaction["paymentMethod"], transaction["ipAddress"],
#               transaction["affiliateId"], transaction["voucherCode"])
#     )

#     cur.close()
#     conn.commit()


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=24*60)
}

@dag(
    'sales_data_etl',
    default_args=default_args,
    schedule="0 * * * *",
    tags=["ssg", "sales"],
    catchup=False,
)
def sales_data():

    @task()
    def initailize_task():
        print (f'{transaction}')
        return 6

    initailize_task()


sales_data()