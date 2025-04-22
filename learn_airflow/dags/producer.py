from airflow import DAG, Dataset
from airflow.decorators import task
from datetime import datetime,timedelta

my_file = Dataset("/tmp/my_file.txt")

default_args = {
    'owner':'airflow',
    'retries':2,
    'retry_delay' : timedelta(seconds=30)
}

with DAG(
    dag_id = 'producer',
    default_args=default_args,
    description='A simple tutorial DAG',
    schedule_interval='@daily',
    start_date=datetime(2024,1,1),
    tags=['example'],
    catchup = False
) as dag:
    @task(outlets = [my_file] )
    def update_my_file():
        with open(my_file.uri,"a+") as f:
            f.write("producer update")
    update_my_file()


    