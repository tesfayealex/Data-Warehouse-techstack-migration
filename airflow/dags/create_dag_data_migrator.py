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


import subprocess
host_name="localhost"
db_user="admin"
db_password="admin"
db_name="trial"

connection = db_connect.connect(host=host_name,user=db_user,password=db_password,database=db_name)
    
cursor = connection.cursor()
query = "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema != 'pg_catalog' and table_schema != 'information_schema' ORDER BY table_schema"
cursor.execute(query)
schemas = cursor.fetchall()
print(schemas)
new_schemas = list(set(schemas))
print(new_schemas)

def convert_type(type):
       if  type == "boolean":
              return "bit(1)"
       elif type == "integer":
              return  "int"
       elif type == "double precision":
              return "double"
       elif type == "character varying":
              return "text"
       else:
              return type
       #  func=switcher.get(type,lambda : type)
       #  return func

def create_table_query(table,columns):
       create_query = f"CREATE TABLE IF NOT EXISTS {table[1]}("
       for index , c in enumerate(columns):
        data_type = convert_type(c[2])
        print(data_type)
        nullable = "NULL" if c[1] == "YES" else "NOT NULL"
        if index != 0:
              create_query += ','
        create_query +=   "`" +  str(c[0]) + "`" + " "  + str(data_type) + " " + str(nullable)
       create_query += ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;"
       return create_query

def create_db_query(name):
       db_query = f"CREATE DATABSE IF NOT EXISTS {name}"