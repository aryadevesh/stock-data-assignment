from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
import sys
import os

# Add scripts directory to Python path
sys.path.append('/opt/airflow/scripts')
sys.path.append('/opt/airflow/scripts')

from stock_data_fetcher import fetch_stock_data_from_api, insert_stock_data, get_database_connection

# Default arguments for the DAG
default_args = {
    'owner': 'data-engineer',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    'stock_data_pipeline',
    default_args=default_args,
    description='A simple stock data pipeline',
    schedule_interval=timedelta(hours=1),  # Run every hour
    catchup=False,
    tags=['stock', 'data-pipeline'],
)


# ETL task functions
def extract(**context):
    symbols = ['AAPL', 'GOOGL', 'MSFT', 'TSLA']
    extracted_data = {}
    for symbol in symbols:
        data = fetch_stock_data_from_api(symbol)
        if data:
            extracted_data[symbol] = data
    context['ti'].xcom_push(key='extracted_data', value=extracted_data)

def transform(**context):
    extracted_data = context['ti'].xcom_pull(key='extracted_data', task_ids='extract')
    # Example: just pass through, but you could add transformation logic here
    transformed_data = extracted_data
    context['ti'].xcom_push(key='transformed_data', value=transformed_data)

def load(**context):
    transformed_data = context['ti'].xcom_pull(key='transformed_data', task_ids='transform')
    conn = get_database_connection()
    for symbol, stock_data in transformed_data.items():
        insert_stock_data(conn, stock_data)
    conn.close()

extract_task = PythonOperator(
    task_id='extract',
    python_callable=extract,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform',
    python_callable=transform,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load',
    python_callable=load,
    provide_context=True,
    dag=dag,
)

extract_task >> transform_task >> load_task