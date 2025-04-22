from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.bash import BashOperator


default_args = {
    'owner':'Sushankhya',
    'retries': 5,
    'retry_delay':  timedelta(minutes=2)
}

with DAG(
     dag_id = 'my_first_dag_v5',
     default_args = default_args,
     description= 'This is my first airflow dag that I have wrote',
     start_date = datetime(2025, 1,1,2),
     schedule_interval = '@daily',
     catchup=False

) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command = "echo hello world, this is first task"
    )

task2 = BashOperator(
    task_id = 'second_task',
    bash_command = "echo hey, this is second task "
)
task3 = BashOperator(
    task_id = 'third_task',
    bash_command = "echo hey,this is third task"
)


# task1.set_downstream(task2)
# task1.set_downstream(task3)

# task1 >> task2
# task2 >> task3

task1 >> [task2,task3]