from datetime import datetime, timedelta
from airflow.decorators import dag, task
import pyodbc

# FreeTDS connection string
CON_STR = (
    "DRIVER={FreeTDS};"
    "SERVER=172.16.7.4;"
    "PORT=1433;"
    "DATABASE=SilverStr;"
    "UID=sa;"
    "PWD=P@kistan12;"
    "TDS_Version=7.0;"
)

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "execution_timeout": timedelta(minutes=24*60),
}

@dag(
    dag_id='Test_hanger_lane',
    default_args=default_args,
    schedule_interval="0 * * * *",  # Every hour at minute 0
    tags=["ssg", "hanger"],
    catchup=False,
)
def process_hanger_lane():

    @task()
    def test_db_connection():
        try:
            conn = pyodbc.connect(CON_STR)
            cursor = conn.cursor()
            cursor.execute("SELECT @@VERSION")
            version = cursor.fetchone()[0]
            print(f"✅ Connected to Target (172.16.7.4) successfully.")
            print(f"SQL Server Version: {version}")
            cursor.close()
            conn.close()
        except pyodbc.Error as e:
            print(f"❌ Error connecting to Target (172.16.7.4): {e}")
            raise  # Ensure Airflow marks the task as failed

    test_db_connection()

process_hanger_lane()
