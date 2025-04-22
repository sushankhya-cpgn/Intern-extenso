from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from random import randint
from datetime import datetime, timedelta

default_args = {
    'owner':'airflow',
    'retries':2,
    'retry_delay' : timedelta(seconds=30)
}

def chooseBestModel(ti):
    accuracies = ti.xcom_pull(task_ids = [
        'trainingmodelA',
        'trainingmodelB',
        'trainingmodelC'
    ])

    best_accuracy = max(accuracies)
    if(best_accuracy > 8):
        return 'accurate'
    
    return 'inaccurate'



def trainingModel():
    return randint(1,10)



with DAG(
    'model_dag',
    default_args=default_args,
    description='A simple DAG',
    schedule_interval='@daily',
    start_date=datetime(2024,1,1),
    tags=['example'],
    catchup=False
) as dag:
    t1  = PythonOperator(
        task_id = "trainingmodelA",
        python_callable=trainingModel,
    )

    t2  = PythonOperator(
        task_id = "trainingmodelB",
        python_callable=trainingModel,
    )

    t3  = PythonOperator(
        task_id = "trainingmodelC",
        python_callable=trainingModel,
    )

    bestModel = BranchPythonOperator(
        task_id = 'choose_best_model',
        python_callable = chooseBestModel
    )

    accurate = BashOperator(
        task_id = "accurate",
        bash_command = "echo accurate"
    )
    inaccurate = BashOperator(
        task_id = "inaccurate",
        bash_command = "echo inaccurate"
    )


[t1,t2,t3] >> bestModel >> [accurate,inaccurate]