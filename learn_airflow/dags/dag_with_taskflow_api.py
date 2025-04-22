from airflow.decorators import dag,task
from datetime import datetime, timedelta


default_args = {
    'owner':'Sushankhya',
    'retries': 5,
    'retry_delay': timedelta(minutes=2)
}

@dag(
        dag_id="dag_with_taskflow_api_v2",
        start_date = datetime(2021,10,6),
        schedule_interval = "@daily",
        catchup = False
)
def hello_world_etl():

    # def task(fync):
    #   do some initialization to show on dag UI
    #   def wrapper():
            # do some work
    #       func()
            # do another work
        # return wrapper

    @task(multiple_outputs = True)
    def get_name():
        return {
            'firstname':'Sushankhya',
            'lastname':'Chapagain'
        }
    
    @task()
    def get_age():
        return 18
    
    @task()
    def greet(firstname,lastname,age):
        print(f"Hello My name is {firstname} {lastname} and I am {age} years old")
    
    name = get_name()
    age = get_age()
    greet(firstname = name['firstname'],lastname = name['lastname'], age = age)
    
greet_dag = hello_world_etl()


