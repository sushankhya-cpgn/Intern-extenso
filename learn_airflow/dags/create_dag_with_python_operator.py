from airflow import DAG
from datetime import datetime,timedelta
from airflow.operators.python import PythonOperator

default_args = {
    'owner':'Sushankhya',
    'retries':5,
    'retry_delay': timedelta(minutes=2)
}

def getname(ti):
    ti.xcom_push(key='firstName',value='Sushankhya')
    ti.xcom_push(key='lastName',value= 'Chapagain')

def getage(ti):
    ti.xcom_push(key='age',value='19')

def greet(ti):
    first_name = ti.xcom_pull(task_ids = 'name',key='firstName')
    last_name = ti.xcom_pull(task_ids = 'name', key='lastName')
    age = ti.xcom_pull(task_ids = 'age', key= 'age')
    print(f"Hello world, My name is {first_name} {last_name} and I am {age} years old")



with DAG(
    dag_id = 'dag_with_python_operator_v4',
    default_args = default_args,
    description = 'Our first DAG with python operator',
    schedule_interval = '@daily',
    start_date = datetime(2025,1,1,2),
    tags=['example'],
    catchup= False
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable = greet,
    )

    task2 = PythonOperator(
        task_id = 'name',
        python_callable = getname
    )
    
    task3 = PythonOperator(
        task_id = 'age',
        python_callable = getage
    )

task2 >> task3  >> task1