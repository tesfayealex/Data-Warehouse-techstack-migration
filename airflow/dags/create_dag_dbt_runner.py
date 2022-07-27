from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
log: logging.log = logging.getLogger("airflow")
log.setLevel(logging.INFO)

default_args={
    'owner':'tesfaye',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}


with DAG(
    dag_id='debug_and_run_dbt',
    default_args=default_args,
    description='debug and run dbt',
    start_date=datetime(2022,7,6,2),
    schedule_interval='@once'
)as dag:
    task1 = BashOperator(
        task_id='dbt_move_to_folder',
        bash_command='cd ../dbt' 
    )
    task2 = BashOperator(
        task_id='dbt_debug',
        bash_command='dbt debug' 
    )
   
    task3 = BashOperator(
        task_id='dbt_test',
        bash_command='dbt test'
    )

    task4 = BashOperator(
        task_id='dbt_run',
        bash_command='dbt run'
    )

    task1 >> task2 >> task3 >> task4