from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime, timedelta

my_file = Dataset("/tmp/my_file.txt")

default_args = {
    'owner':'airflow',
    'retries':2,
    'retry_delay' : timedelta(seconds=30)
}

with DAG(
    dag_id = 'consumer',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval=[my_file],
    start_date=datetime(2024,1,1),
    catchup = False,
    tags=['example'],
) as dag:
    @task()
    def read_my_file():
        with open(my_file.uri,"r") as f:
            print(f.read())
    read_my_file()