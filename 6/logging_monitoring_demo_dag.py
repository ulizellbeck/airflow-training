from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowException
from datetime import datetime, timedelta
import logging
import random

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,  # Enable email notifications on failure
    'email_on_retry': True,    # Enable email notifications on retry
    'retries': 2,
    'retry_delay': timedelta(minutes=1),
}

def task_with_basic_logging(**context):
    # Get task instance
    task_instance = context['task_instance']
    
    # Different logging levels
    logging.info("This is a standard info message")
    logging.warning("This is a warning message")
    logging.error("This is an error message")
    
    # Log with task instance
    task_instance.log.info("Logging using task instance logger")
    
    # Store some metrics
    task_instance.xcom_push(key='execution_time', value=datetime.now().isoformat())

def task_with_custom_metrics(**context):
    task_instance = context['task_instance']
    
    # Simulate some metrics
    processing_time = random.uniform(0.1, 2.0)
    records_processed = random.randint(100, 1000)
    
    # Log metrics
    task_instance.log.info(f"Processing time: {processing_time:.2f} seconds")
    task_instance.log.info(f"Records processed: {records_processed}")
    
    # Store metrics in XCom
    context['task_instance'].xcom_push(key='processing_metrics', value={
        'processing_time': processing_time,
        'records_processed': records_processed
    })

def task_with_failure(**context):
    task_instance = context['task_instance']
    
    # Log some information before failure
    task_instance.log.info("About to simulate a task failure...")
    
    # Randomly decide if task should fail
    if random.random() < 0.7:  # 70% chance of failure
        task_instance.log.error("Task is about to fail!")
        raise AirflowException("This is a simulated task failure")
    
    task_instance.log.info("Task completed successfully!")

def task_with_performance_monitoring(**context):
    import time
    task_instance = context['task_instance']
    
    # Start timing
    start_time = time.time()
    
    # Simulate some work
    time.sleep(random.uniform(0.5, 2))
    
    # End timing
    execution_time = time.time() - start_time
    
    # Log performance metrics
    task_instance.log.info(f"Task execution time: {execution_time:.2f} seconds")
    
    if execution_time > 1.5:
        task_instance.log.warning("Task execution time exceeded threshold!")

with DAG(
    'logging_monitoring_demo',
    default_args=default_args,
    description='Demo DAG for logging and monitoring features',
    schedule_interval='@hourly',
    catchup=False
) as dag:

    basic_logging = PythonOperator(
        task_id='basic_logging',
        python_callable=task_with_basic_logging,
        provide_context=True
    )

    custom_metrics = PythonOperator(
        task_id='custom_metrics',
        python_callable=task_with_custom_metrics,
        provide_context=True
    )

    failing_task = PythonOperator(
        task_id='failing_task',
        python_callable=task_with_failure,
        provide_context=True
    )

    performance_monitoring = PythonOperator(
        task_id='performance_monitoring',
        python_callable=task_with_performance_monitoring,
        provide_context=True
    )

    basic_logging >> custom_metrics >> failing_task >> performance_monitoring 