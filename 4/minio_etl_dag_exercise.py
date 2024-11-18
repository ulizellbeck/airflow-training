from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from datetime import datetime, timedelta
import pandas as pd
import io

# Standard-Argumente definieren
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

# Funktion um Daten aus MinIO zu laden
def load_from_minio(**context):
    s3_hook = S3Hook(aws_conn_id='my_s3_conn')
    bucket_name = 'testbucket'
    key = 'sample_data.csv'
    
    # Lese Daten aus MinIO
    data = s3_hook.read_key(key, bucket_name)
    df = pd.read_csv(io.StringIO(data))
    
    # Todo: Übergeben Sie den DataFrame an den nächsten Task
    # Hinweis: Sie müssen die context-Variable erstellen und die xcom_push-Methode verwenden.
    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
    context['task_instance']('YOUR_CODE')

# Funktion um Daten zu transformieren    
def transform_data(**context):
    # Todo: Laden der Daten aus dem vorherigen Task
    # Hinweis: Sie müssen die context-Variable erstellen und die xcom_pull-Methode verwenden.
    # Wir müssen die 'from_dict'-Methode verwenden, um die aus xcom_pulled Daten in ein DataFrame umzuwandeln.
    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
    df = pd.DataFrame.from_dict('YOUR_CODE_HERE')
    
    df['total_sales'] = df['quantity'] * df['price']
    df['price'] = df['price'].round(2)
    df['total_sales'] = df['total_sales'].round(2)
    
    # Todo: Übergeben Sie den transformierten DataFrame an den nächsten Task
    # Hinweis: Sie müssen die context-Variable erstellen und die xcom_push-Methode verwenden.
    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
    context['task_instance']('YOUR_CODE')

# Funktion um Daten wieder nach MinIO zu speichern
def store_to_minio(**context):
    s3_hook = S3Hook(aws_conn_id='my_s3_conn')
    bucket_name = 'processed'
    key = f'transformed_data_{context["ds"]}.csv'
    
    # Todo: Laden der transformierten Daten
    # Hinweis: Sie müssen die context-Variable erstellen und die xcom_pull-Methode verwenden.
    # Wir müssen die 'from_dict'-Methode verwenden, um die aus xcom_pulled Daten in ein DataFrame umzuwandeln.
    # https://airflow.apache.org/docs/apache-airflow/stable/core-concepts/xcoms.html
    df = pd.DataFrame.from_dict('YOUR_CODE_HERE')
    
    # Wandeln Sie das DataFrame in das CSV-Format um
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, index=False)
    
    # Laden Sie die Daten wieder nach MinIO
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key=key,
        bucket_name=bucket_name
    )

# Create DAG
with DAG(
    'minio_etl_pipeline',
    default_args=default_args,
    description='ETL pipeline using MinIO',
    schedule_interval='@daily',
    catchup=False
) as dag:

    # Task 1: Laden der Daten aus MinIO
    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_from_minio,
        provide_context=True
    )

    # Task 2: Transformieren der Daten
    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    # Task 3: Speichern der Daten wieder nach MinIO
    store_data = PythonOperator(
        task_id='store_data',
        python_callable=store_to_minio,
        provide_context=True
    )

    # Set task dependencies
    load_data >> transform_data >> store_data 