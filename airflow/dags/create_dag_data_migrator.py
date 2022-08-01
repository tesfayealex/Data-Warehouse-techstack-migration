from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2 as db_connect
from airflow.providers.postgres.operators.postgres import PostgresOperator
from sqlalchemy import create_engine, types, text
import pandas as pd

host_name="localhost"
db_user="admin"
db_password="admin"
db_name="trial"

default_args={
    'owner':'tesfaye',
    'retries':5,
    'retry_delay':timedelta(minutes=1)
}

def get_schema_and_table_name(database_name):
       connection = db_connect.connect(host=host_name,user=db_user,password=db_password,database=database_name)
    
       cursor = connection.cursor()
       query = f'SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema != "pg_catalog" and table_schema != "information_schema" ORDER BY table_schema'

       cursor.execute(query)
       schemas = cursor.fetchall()
       return schemas

def convert_type(type):
       if  type == "boolean":
              return "bit(1)"
       elif type == "integer":
              return  "int"
       elif type == "double precision":
              return "double"
       elif type == "character varying":
              return "LONGTEXT"
       elif type == "text":
              return "LONGTEXT"
       else:
              return type

def create_table_query(table,columns):
       create_query = f"USE {table[0]}; CREATE TABLE IF NOT EXISTS {table[1]}("
       for index , c in enumerate(columns):
        data_type = convert_type(c[2])
        nullable = "NULL" if c[1] == "YES" else "NOT NULL"
        if index != 0:
              create_query += ','
        create_query +=   "`" +  str(c[0]) + "`" + " "  + str(data_type) + " " + str(nullable)
       create_query += ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE utf8mb4_unicode_ci;"
       return create_query

def create_db_query(name):
       db_query = f"CREATE DATABASE IF NOT EXISTS {name}"
       return db_query

def start_workflow(database_name):
       schema = get_schema_and_table_name(database_name)
       create_schemas_and_load_data(database_name , schema)

def create_schemas_and_load_data(database_name , schemas):
       connection = db_connect.connect(host=host_name,user=db_user,password=db_password,database=database_name)
       postgres_engine = create_engine(f'postgresql+psycopg2://{db_user}:{db_password}@{host_name}/{database_name}')
       cursor = connection.cursor()
       for s in schemas:
                     query = f"SELECT column_name , is_nullable , data_type  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA= '{str(s[0])}' and TABLE_NAME = '{str(s[1])}'"
                     cursor.execute(query)
                     columns = cursor.fetchall()
                     mysql_connection = f'mysql://admin:admin@127.0.0.1:3306/{database_name}'
                     engine = create_engine(mysql_connection)
                     conn = engine.connect()
                     db_query  = create_db_query(s[0])
                     db = conn.execute(db_query)
                     singe_db_connection = f'mysql://admin:admin@127.0.0.1:3306/{s[0]}'
                     single_db_engine = create_engine(singe_db_connection)
                     single_db_conn = engine.connect()
                     create_query = create_table_query(s,columns)
                     conn.execute(create_query)
                     the_data = pd.read_sql(f'SELECT * FROM {s[0]}.{s[1]}',postgres_engine)
                     x = the_data.to_sql(s[1], con=single_db_engine, if_exists='replace', index=False)
                     get_data_query = f'select * from {s[0]}.{s[1]};'

def migrate_privilages(database_name):
       connection = db_connect.connect(host=host_name,user=db_user,password=db_password,database=database_name)
       cursor = connection.cursor()
       query = f"SELECT * FROM information_schema.table_privileges"
       cursor.execute(query)
       columns = cursor.fetchall()
       for c in columns:
                     check_user_existance = f'SELECT user,host FROM mysql.user where user = "{c[1]}"'
                     query = f'GRANT {c[5]} on {c[3]}.{c[4]} to {c[1]}@`localhost`;' 
                     mysql_connection = f'mysql://{db_user}:{db_password}@{host_name}:3306/{c[3]}'
                     engine = create_engine(mysql_connection)
                     conn = engine.connect()
                     excuted = conn.execute(check_user_existance)
                     if len(excuted.fetchall()) == 0:
                            create_user_query = f'CREATE USER {c[1]}@`{host_name}` IDENTIFIED WITH mysql_native_password BY "password";'
                            excuted = conn.execute(create_user_query)
                     if c[5] != "TRUNCATE":
                            excuted = conn.execute(query)




with DAG(
    dag_id='migrate_data',
    default_args=default_args,
    description='migrate data from postgres to mysql',
    start_date=datetime(2022,7,6,2),
    schedule_interval='@once'
)as dag:
    task1 = PythonOperator(
       task_id='create_schema_and_migrate_data',
       python_callable=start_workflow,
       op_kwargs={'database_name': 'Warehouse' },
    )
    task2 = PythonOperator(
       task_id='create_dataset_table',
       python_callable=migrate_privilages,
       op_kwargs={'database_name': 'Warehouse' },
    )
    task3 = PythonOperator(
       task_id='create_schema_and_migrate_data_2',
       python_callable=start_workflow,
       op_kwargs={'database_name': 'trial' },
    )
    task4 = PythonOperator(
       task_id='create_dataset_table_2',
       python_callable=migrate_privilages,
       op_kwargs={'database_name': 'trial' },
    )

    task1 >> task2 >> task3 >> task4