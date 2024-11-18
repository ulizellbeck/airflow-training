from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
import pandas as pd
import io

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def transform_data():
    # Initialisiere S3-Hook
    s3_hook = S3Hook(aws_conn_id='my_s3_conn')
    
    # Lese Daten aus MinIO
    data = s3_hook.read_key(
        key='sample_data.csv',
        bucket_name='testbucket'
    )
    
    # Wandle in DataFrame um
    # Erstelle ein pandas DataFrame aus den Daten (Verwende read_csv und io.StringIO um die Daten in einen String umzuwandeln)
    # https://docs.python.org/3/library/io.html#text-i-o
    # https://pandas.pydata.org/docs/reference/api/pandas.read_csv.html
    df = 
    
    # Füge Transformationen hinzu
    # Füge eine neue Spalte total_sales hinzu, die das Produkt von quantity und price ist
    # https://www.statology.org/pandas-multiply-two-columns/
    df['total_sales'] = 
    # Wandle die date-Spalte in datetime um
    # https://pandas.pydata.org/docs/reference/api/pandas.to_datetime.html
    df['date'] = 
    # Runde die price- und total_sales-Spalten auf 2 Dezimalstellen
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.round.html
    df['price'] = 
    df['total_sales'] = 
    
    # Wandle wieder in CSV-String um
    # Verwenden Sie io.StringIO um das DataFrame in einen String umzuwandeln
    # https://docs.python.org/3/library/io.html#text-i-o
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_csv.html
    csv_buffer = 
    df.to_csv(csv_buffer, index=False)
    
    # Lade die transformierten Daten wieder nach MinIO
    s3_hook.load_string(
        string_data=csv_buffer.getvalue(),
        key='transformed_data.csv',
        bucket_name='processed',
        replace=True
    )
    
    return "Data transformed and saved to MinIO"

with DAG(
    'data_transformation_exercise',
    default_args=default_args,
    description='Transform sample sales data from MinIO',
    schedule_interval=timedelta(days=1),
    catchup=False
) as dag:

    transform_task = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
    )

transform_task 