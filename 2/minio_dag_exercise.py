from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import os

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def transfer_minio_file(**context):
    # TODO: Initialisiere den 
    s3_hook = S3Hook(aws_conn_id='YOUR_MINIO_CONN_ID')
    
    # TODO: Define your source and destination details
    source_bucket = 'YOUR_SOURCE_BUCKET'  # The bucket to read from
    source_key = 'YOUR_SOURCE_FILE'       # The file to transfer
    temp_dir = '/tmp'                     # Where to save the file locally
    dest_bucket = 'YOUR_DEST_BUCKET'      # The bucket to write to
    dest_key = 'YOUR_DEST_FILE'           # The destination file name
    
    # Download file
    # TODO: Verwende s3_hook.download_file mit den korrekten Parametern (Link: https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html#airflow.providers.amazon.aws.hooks.s3.S3Hook.download_file)
    temp_file = s3_hook.'YOUR_CODE'
    print(f"File downloaded successfully to {temp_file}")
    
    # Erstelle das Ziel-Bucket, falls es nicht existiert
    if not s3_hook.check_for_bucket(dest_bucket):
        s3_hook.create_bucket(bucket_name=dest_bucket)
        print(f"Created new bucket: {dest_bucket}")

    # TODO: Implementiere die Datei-Upload
    # Hinweis: Verwende s3_hook.load_file mit den korrekten Parametern (Link: https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/hooks/s3/index.html#airflow.providers.amazon.aws.hooks.s3.S3Hook.load_file)
    
    
    print(f"File uploaded successfully to {dest_bucket}/{dest_key}")

    # Bereinige die temporäre Datei
    os.remove(temp_file)

# TODO: Erstelle den DAG
# Hinweis: Verwende den 'with DAG()' Kontext-Manager (https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/dags.html#declaring-a-dag) 
# https://cloud.google.com/composer/docs/how-to/using/writing-dags?hl=de
# Verwende die Parameter dag_id, default_args, description, schedule_interval, and catchup
with DAG('YOUR_CODE') as dag:

    # TODO: Erstelle die PythonOperator-Aufgabe
    # Hinweis: Verwende PythonOperator mit der transfer_minio_file-Funktion
    transfer_task = PythonOperator('YOUR_CODE')

    # transfer_task ausführen
    transfer_task