from datetime import datetime, timedelta

from airflow.decorators import dag, task

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=24*60)
}

@dag(
    'hanger_lane_etl',
    default_args=default_args,
    schedule_interval="0 * * * *",
    tags=["ssg", "hanger"],
    catchup=False,
)
def process_hanger_lane():

    @task()
    def initailize_task():
        print ("Maibn chl gya hnn")
        return 6

    initailize_task()


process_hanger_lane()