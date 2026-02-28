import uuid
from datetime import datetime, timedelta
import logging
from airflow import DAG
from airflow.operators.python import PythonOperator
from faker import Faker   # <-- NEW

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2026, 2, 28),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=5)
}

fake = Faker()   # <-- Initialize once

def get_data():
    """Generate fake user using local Faker (no API call)"""
    import random
    # Build exact same structure as randomuser.me so format_data works without change
    res = {
        "name": {
            "first": fake.first_name(),
            "last": fake.last_name()
        },
        "gender": random.choice(['male', 'female']),
        "location": {
            "street": {
                "number": int(fake.building_number()),
                "name": fake.street_name()
            },
            "city": fake.city(),
            "state": fake.state(),
            "country": fake.country(),
            "postcode": fake.postcode()
        },
        "email": fake.email(),
        "login": {
            "username": fake.user_name()
        },
        "dob": {
            "date": fake.date_of_birth().isoformat() + "T00:00:00.000Z"
        },
        "registered": {
            "date": fake.date_this_decade().isoformat() + "T00:00:00.000Z"
        },
        "phone": fake.phone_number(),
        "picture": {
            "medium": fake.image_url(width=200, height=200)  # random nice photo
        }
    }
    return res

def format_data(res):
    """Your original formatter - works unchanged"""
    location = res['location']
    data = {}
    data['id'] = str(uuid.uuid4())
    data['first_name'] = res['name']['first']
    data['last_name'] = res['name']['last']
    data['gender'] = res['gender']
    data['address'] = f"{str(location['street']['number'])} {location['street']['name']}, " \
                      f"{location['city']}, {location['state']}, {location['country']}"
    data['post_code'] = location['postcode']
    data['email'] = res['email']
    data['username'] = res['login']['username']
    data['dob'] = res['dob']['date']
    data['registered_date'] = res['registered']['date']
    data['phone'] = res['phone']
    data['picture'] = res['picture']['medium']
    return data

def stream_data():
    import json
    from kafka import KafkaProducer
    import time
    import os

    logging.basicConfig(level=logging.INFO)  # Ensure logging is configured
    kafka_broker = os.getenv('KAFKA_BROKER', 'broker:29092')  # Default to broker container for containerized setup
    try:
        producer = KafkaProducer(bootstrap_servers=[kafka_broker], max_block_ms=10000)
        logging.info(f"Connected to Kafka broker: {kafka_broker}")
    except Exception as e:
        logging.error(f"Kafka broker not available at {kafka_broker}: {e}")
        raise e
    
    curr_time = time.time()
    while time.time() < curr_time + 60:   # Run for 1 minute (test)
        try:
            res = get_data()
            formatted = format_data(res)
            logging.info(f"Sending data: {formatted}")
            print(f'data: {formatted}')
            
            producer.send('users_created', json.dumps(formatted).encode('utf-8'))  # uncomment later
            time.sleep(1)
        except Exception as e:
            producer.flush()  # Ensure all messages are sent            
            logging.error(f'Error: {e}')
    
    logging.info("Data streaming completed.")

with DAG('user_automation',
         default_args=default_args,
         schedule=None,
         tags=["test", "fake-data", "silver-star"],
         catchup=False) as dag:
    
    streaming_task = PythonOperator(
        task_id='stream_data_from_fake_users',
        python_callable=stream_data
    )

# if __name__ == "__main__":
#     print("Testing stream_data...")
#     stream_data()