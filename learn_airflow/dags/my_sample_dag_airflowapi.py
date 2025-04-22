from airflow.decorators import dag,task
from airflow import DAG
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime, timedelta

default_args = {
    'owner':'airflow',
    'retries': 2,
    'retry_delay': timedelta(seconds=30)
}

with DAG(
    dag_id = "model_apiflow_dag",
    default_args = default_args,
    schedule_interval=timedelta(days=1),
    start_date= datetime(2024,1,1),
    tags=['example'],
    catchup = False
):
    @task()

    def trainingModel(accuracy):
        return accuracy
    
    @task.branch()

    def chooseBestModel(accuracies):
        best_accuracy = max(accuracies)
        if (best_accuracy>8):
            return 'accurate'
        return 'inaccurate'
    
    accurate  = BashOperator(
        task_id = 'accurate',
        bash_command = "echo accurate"
    )

    inaccurate = BashOperator(
        task_id = 'inaccurate',
        bash_command = "echo inaccurate"
    )


    chooseBestModel(trainingModel.expand(accuracy=[5,10,15])) >> [accurate,inaccurate]
    

    


