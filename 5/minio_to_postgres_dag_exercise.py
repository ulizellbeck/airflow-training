from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
# ToDo: Importieren Sie den PostgreSQL-Hook
from datetime import datetime, timedelta
import pandas as pd
import io

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

def load_from_minio(**context):
    s3_hook = S3Hook(aws_conn_id='my_s3_conn')
    bucket_name = 'testbucket'
    key = 'sample_data.csv'
    
    data = s3_hook.read_key(key, bucket_name)
    df = pd.read_csv(io.StringIO(data))
    context['task_instance'].xcom_push(key='raw_data', value=df.to_dict())

def transform_data(**context):
    df = pd.DataFrame.from_dict(context['task_instance'].xcom_pull(task_ids='load_data', key='raw_data'))
    
    df['total_sales'] = df['quantity'] * df['price']
    df['price'] = df['price'].round(2)
    df['total_sales'] = df['total_sales'].round(2)
    
    context['task_instance'].xcom_push(key='transformed_data', value=df.to_dict())

def load_to_postgres(**context):
    # Hole die transformierten Daten
    df = pd.DataFrame.from_dict(context['task_instance'].xcom_pull(task_ids='transform_data', key='transformed_data'))
    
    # ToDo: Erstellen Sie eine Verbindung zur PostgreSQL-Datenbank
    postgres_hook = 'YOUR CODE HERE'
    
    # Die Tabelle soll nur erstellt werden, falls sie nicht existiert
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS sales_data (
        date DATE,
        product_id VARCHAR(50),
        category VARCHAR(50),
        quantity INTEGER,
        price DECIMAL(10,2),
        customer_id VARCHAR(50),
        region VARCHAR(50),
        total_sales DECIMAL(10,2)
    );
    """
    # ToDo: Führen Sie den SQL-Befehl aus, um die Tabelle zu erstellen
    # Es gibt mehrere Möglichkeiten, dies zu tun. 
    # Aus Konsistenz-Gründen verwenden wir hier nicht den SQLExecuteQueryOperator, sondern den PostgreSQL-Hook.
    # Sie können dazu postgres_hook.run('SQL-Befehl') verwenden, um die Tabelle zu erstellen.
    
    # Daten in PostgreSQL laden
    # Verwendung von pandas to_sql mit dem PostgreSQL-Hook
    # Um einen DataFrame in SQL zu laden, können Sie die Funktion df.to_sql() verwenden.
    # https://pandas.pydata.org/docs/reference/api/pandas.DataFrame.to_sql.html
    # Wichtig: Die Funktion to_sql erwartet eine SQLAlchemy-Engine als connection.

    
    df.to_sql(
        'Your Code Here',
    )

with DAG(
    'minio_to_postgres_pipeline',
    default_args=default_args,
    description='ETL pipeline from MinIO to PostgreSQL',
    schedule_interval='@daily',
    catchup=False
) as dag:

    load_data = PythonOperator(
        task_id='load_data',
        python_callable=load_from_minio,
        provide_context=True
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data,
        provide_context=True
    )

    postgres_load = PythonOperator(
        task_id='postgres_load',
        python_callable=load_to_postgres,
        provide_context=True
    )

    load_data >> transform_data >> postgres_load 