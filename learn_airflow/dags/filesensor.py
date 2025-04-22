# from airflow.models import DAG
# from airflow.sensors.filesystem import FileSensor
# from datetime import datetime,timedelta

# default_args = {
#     'owner':'airflow',
#     'retries':2,
#     'retry_delay': timedelta(seconds=30)
# }

# with DAG(
#     'dag_sensor',
#     default_args=default_args,
#     description='A simple tutorial DAG',
#     schedule_interval='@daily',
#     start_date=datetime(2021,1,1),
#     tags=['example'],
#     catchup = False
# ) as dag:
    
#     waiting_for_file = FileSensor(
#         task_id = 'waiting_for_file',
#         poke_interval = 30,
#         timeout = 60*5,
#         mode = 'reschedule',
#         soft_fail = True
#     )
