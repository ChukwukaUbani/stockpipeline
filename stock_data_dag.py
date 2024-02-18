# Import modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.bash_operator import BashOperator
from include.etl import extract_data, transform_data, load_data_to_db

# These args will get passed on to each operator
# You can override them on a per-task basis during operator initialization
default_args = {
    'owner': 'Kelvin',
    'depends_on_past': False,
    'start_date': datetime(year=2023, month=10, day=22),
    'email': ['de10alytics@gmail.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

with DAG (
    'stock_data_etl_job',
    default_args = default_args,
    description='This job pulls stock data from a stock trading website',
    schedule_interval= '0 8 * * *', 
    catchup=False,
) as dag:
    
    get_stock_data = PythonOperator(
        task_id = 'extract_data',
        python_callable = extract_data
    )

    transform_data = PythonOperator(
        task_id = 'transform_data',
        python_callable = transform_data
    )

    load_data_to_db = PythonOperator(
        task_id = 'load_data_to_db',
        python_callable = load_data_to_db
    )

    wait = BashOperator(
    task_id = 'wait_some_minutes',
    bash_command = 'sleep 60'
    )

    start = DummyOperator(
    task_id='start')

    end = DummyOperator(
    task_id='end')

# Defining task dependencies
start >> get_stock_data >> transform_data >> wait >> load_data_to_db >> end
