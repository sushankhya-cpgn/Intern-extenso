from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.email import EmailOperator 
from airflow.sensors.filesystem import FileSensor
from datetime import datetime, timedelta
import os
import zipfile
import mimetypes

UPLOAD_DIR = '/usr/local/airflow/dags/input'
COMPRESS_DIR = '/usr/local/airflow/dags/output'
EMAIL_TO = 'sushankhya41@gmail.com'

default_args = {
    'owner': 'airflow',
    'retries': 2,
    'retry_delay': timedelta(seconds=30)
}

def compress_file(file_name, **kwargs):
    zip_file_name = os.path.join(COMPRESS_DIR, os.path.splitext(file_name)[0] + ".zip")
    print(zip_file_name)
    file_path = os.path.join(UPLOAD_DIR, file_name)
    # new_file = file_name.join
    # zip_path = os.path.join(COMPRESS_DIR, file_name + '.zip')
    with zipfile.ZipFile(zip_file_name, 'w') as zipf:
        zipf.write(file_path, arcname=file_name)
    kwargs['ti'].xcom_push(key='zip_path', value=zip_file_name)

def get_file_specs(file_name, **kwargs):
    file_path = os.path.join(UPLOAD_DIR, file_name)
    zip_path = kwargs['ti'].xcom_pull(key='zip_path')

    original_size = os.path.getsize(file_path)
    compressed_size = os.path.getsize(zip_path)
    original_type = mimetypes.guess_type(file_path)[0] or "Unknown"

    specs = f"""
    ✅ File Compression Report ✅

    📤 Original File:
    - Name: {file_name}
    - Type: {original_type}
    - Size: {original_size} bytes

    📦 Compressed File:
    - Name: {os.path.basename(zip_path)}
    - Size: {compressed_size} bytes
    """
    kwargs['ti'].xcom_push(key='email_body', value=specs)

def main(**kwargs):
  #line changed
  variable = kwargs['dag_run'].conf.get('rohit')
  print(variable)

with DAG(
    'event_driven_compress_notify',
    default_args=default_args,
    description='Event Driven DAG that compresses uploaded file and sends email',
    schedule_interval=None,
    start_date=datetime(2024, 1, 1),
    catchup=False
) as dag:

    t1 = PythonOperator(task_id = 'main',
        python_callable = main,
        dag = dag
    )

    wait_for_file = FileSensor(
        task_id='waiting_for_file',
        fs_conn_id='fs_default',  # Make sure this maps to the base path `/Users/sushankhyachapagain/learn_airflow`
        filepath='sample.txt',
        poke_interval=10,
        timeout=600,
        mode='poke'
    )

    compress_task = PythonOperator(
        task_id='compress_file',
        python_callable=compress_file,
        op_kwargs={'file_name': 'sample.txt'}
    )

    generate_specs = PythonOperator(
        task_id='generate_file_specs',
        python_callable=get_file_specs,
        op_kwargs={'file_name': 'sample.txt'}
    )

    send_email = EmailOperator(
        task_id='send_email',
        to=EMAIL_TO,
        subject='Airflow Compression Report',
        html_content="{{ ti.xcom_pull(task_ids='generate_file_specs', key='email_body') }}"
    )

    wait_for_file >> compress_task >> generate_specs >> send_email
