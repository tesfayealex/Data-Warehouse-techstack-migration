from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2 as db_connect
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
log: logging.log = logging.getLogger("airflow")
log.setLevel(logging.INFO)

default_args={
    'owner':'tesfaye',
    'retries':5,
    'retry_delay':timedelta(minutes=2)
}

def get_table_lists(database):
    host_name="localhost"
    db_user="admin"
    db_password="admin"
    db_name="trial"

    connection = db_connect.connect(host=host_name,user=db_user,password=db_password,database=db_name)
    cursor = connection.cursor()
    query = "SELECT table_schema FROM information_schema.tables WHERE table_schema != 'pg_catalog' and table_schema != 'information_schema' ORDER BY table_schema"
    cursor.execute(query)
    schemas = cursor.fetchall()
    print(schemas)
    new_schemas = list(set(schemas))
    print(new_schemas)

with DAG(
    dag_id='load_data',
    default_args=default_args,
    description='extract and load raw data from the given dataset',
    start_date=datetime(2022,7,6,2),
    schedule_interval='@once'
)as dag:
    task1 = PostgresOperator(
        task_id='create_warehouse_database',
        postgres_conn_id='postgres_connection',
        sql='/sql/create_raw_data.sql',
    )
    task2 = PostgresOperator(
        task_id='create_dataset_table',
        postgres_conn_id='postgres_connection',
        sql='/sql/create_raw_data.sql',
    )
    task3 = PostgresOperator(
        task_id='load_dataset',
        postgres_conn_id='postgres_connection',
        sql='/sql/load_raw_data.sql',
    )

    task1 >> task2 >> task3