import uuid
# from datetime import datetime
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator
# from airflow.decorators import dag, task


default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=5)
}

def get_data():
    import requests

    res = requests.get("https://randomuser.me/api/")
    res = res.json()
    res = res['results'][0]
    return res

def format_data(res):
    data = {}
    location = res['location']
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
    # from kafka import KafkaProducer
    import time
    import logging

    logging.basicConfig(level=logging.INFO)  # Ensure logging is configured
    try:
        # producer = KafkaProducer(bootstrap_servers=['localhost:9092'], max_block_ms=10000)
        curr_time = time.time()
        while time.time() < curr_time + 60:  # Run for 1 minute
            try:
                res = get_data()
                res = format_data(res)
                logging.info(f"Sending data: {res}")
                print(f'data: {res}')
                # producer.send('users_created', json.dumps(res).encode('utf-8'))
                time.sleep(1)  # Add delay to avoid overwhelming the producer
            except Exception as e:
                logging.error(f'Error in loop: {e}')
        # producer.flush()  # Ensure all messages are sent
    except Exception as e:
        logging.error(f'Kafka connection error: {e}')
        raise
    
stream_data()
# def stream_data():
#     import json
#     from kafka import KafkaProducer
#     import time
#     import logging

#     producer = KafkaProducer(bootstrap_servers=['localhost:29092'], max_block_ms=10000)
#     curr_time = time.time()

#     while True:
#         if time.time() > curr_time + 60: #1 minute
#             break
#         try:
#             res = get_data()
#             res = format_data(res)
#             logging.info(f"Sending data: {res}")
#             producer.send('users_created', json.dumps(res).encode('utf-8'))
#         except Exception as e:
#             logging.error(f'An error occured: {e}')
#             continue

# if __name__ == "__main__":
#     print("Running stream_data function for testing...")
#     stream_data()


# with DAG('user_automation',
#          default_args=default_args,
#         #  schedule_interval='@daily',
#          schedule="0 * * * *",
#          tags=["ssg", "api-getdata"],
#          catchup=False) as dag:

#     streaming_task = PythonOperator(
#         task_id='stream_data_from_api',
#         python_callable=stream_data
#     )
