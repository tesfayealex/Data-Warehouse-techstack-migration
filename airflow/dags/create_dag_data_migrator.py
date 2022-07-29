from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import psycopg2 as db_connect
from sqlalchemy import create_engine, types, text
import pandas as pd
from airflow.providers.postgres.operators.postgres import PostgresOperator
import logging
log: logging.log = logging.getLogger("airflow")
log.setLevel(logging.INFO)

# default_args={
#     'owner':'tesfaye',
#     'retries':5,
#     'retry_delay':timedelta(minutes=2)
# }

# def get_table_lists(database):
#     host_name="localhost"
#     db_user="admin"
#     db_password="admin"
#     db_name="trial"

#     connection = db_connect.connect(host=host_name,user=db_user,password=db_password,database=db_name)
#     cursor = connection.cursor()
#     query = "SELECT table_schema FROM information_schema.tables WHERE table_schema != 'pg_catalog' and table_schema != 'information_schema' ORDER BY table_schema"
#     cursor.execute(query)
#     schemas = cursor.fetchall()
#     print(schemas)
#     new_schemas = list(set(schemas))
#     print(new_schemas)

# with DAG(
#     dag_id='load_data',
#     default_args=default_args,
#     description='extract and load raw data from the given dataset',
#     start_date=datetime(2022,7,6,2),
#     schedule_interval='@once'
# )as dag:
#     task1 = PostgresOperator(
#         task_id='create_warehouse_database',
#         postgres_conn_id='postgres_connection',
#         sql='/sql/create_raw_data.sql',
#     )
#     task2 = PostgresOperator(
#         task_id='create_dataset_table',
#         postgres_conn_id='postgres_connection',
#         sql='/sql/create_raw_data.sql',
#     )
#     task3 = PostgresOperator(
#         task_id='load_dataset',
#         postgres_conn_id='postgres_connection',
#         sql='/sql/load_raw_data.sql',
#     )

#     task1 >> task2 >> task3


import subprocess
from sqlalchemy.orm import sessionmaker
host_name="localhost"
db_user="admin"
db_password="admin"
db_name="trial"

connection = db_connect.connect(host=host_name,user=db_user,password=db_password,database=db_name)
    
cursor = connection.cursor()
query = "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema != 'pg_catalog' and table_schema != 'information_schema' ORDER BY table_schema"
postgres_engine = create_engine('postgresql+psycopg2://admin:admin@localhost/trial')
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
              return "LONGTEXT"
       elif type == "text":
              return "LONGTEXT"
       else:
              return type
       #  func=switcher.get(type,lambda : type)
       #  return func

def create_table_query(table,columns):
       create_query = f"USE {table[0]}; CREATE TABLE IF NOT EXISTS {table[1]}("
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
       db_query = f"CREATE DATABASE IF NOT EXISTS {name}"
       return db_query


for s in schemas:
       if s[0] == "public":
              query = f"SELECT column_name , is_nullable , data_type  FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_SCHEMA= '{str(s[0])}' and TABLE_NAME = '{str(s[1])}'"
              print(query)
              cursor.execute(query)
              columns = cursor.fetchall()
              print (columns)
              mysql_connection = f'mysql://admin:admin@127.0.0.1:3306/trial'
              engine = create_engine(mysql_connection)
              conn = engine.connect()
              db_query  = create_db_query(s[0])
              print(db_query)
              db = conn.execute(db_query)
              singe_db_connection = f'mysql://admin:admin@127.0.0.1:3306/{s[0]}'
              single_db_engine = create_engine(singe_db_connection)
              single_db_conn = engine.connect()
              # print(db.fetchall())
              create_query = create_table_query(s,columns)
              print(create_query)
              conn.execute(create_query)
              # print(cursor.fetchall())
              the_data = pd.read_sql(f'SELECT * FROM {s[0]}.{s[1]}',postgres_engine)
              # query = text(f'show tables')
              print(the_data)
              x = the_data.to_sql(s[1], con=single_db_engine, if_exists='append', index=False)
              print(x)
              get_data_query = f'select * from {s[0]}.{s[1]};'
              # print(get_data_query)
              # result = conn.execute(get_data_query)
              # print(result.fetchall())
              # conn = pymysql.connect("localhost", "admin", "admin")
              # print(conn)
              # break;

def migrate_privilages():
      
       query = f"SELECT * FROM information_schema.table_privileges"
       print(query)
       cursor.execute(query)
       columns = cursor.fetchall()
       print(columns)
       for c in columns:
              if c[1] == 'try_user':
                     check_user_existance = f'SELECT user,host FROM mysql.user where user = "{c[1]}"'
                     query = f'GRANT {c[5]} on {c[3]}.{c[4]} to {c[1]}@`localhost`;' 
                     mysql_connection = f'mysql://admin:admin@127.0.0.1:3306/{c[3]}'
                     engine = create_engine(mysql_connection)
                     conn = engine.connect()
                     excuted = conn.execute(check_user_existance)
                     if len(excuted.fetchall()) == 0:
                            create_user_query = f'CREATE USER {c[1]}@`localhost` IDENTIFIED WITH mysql_native_password BY "password";'
                            excuted = conn.execute(create_user_query)
                     print(query)
                     if c[5] != "TRUNCATE":
                            excuted = conn.execute(query)




migrate_privilages()

     
# query = "pg_dump mydb > db.sql"


# cursor.execute(query)
# # schemas = cursor.fetchall()
# print(cursor.fetchall())

# try:
#             process = subprocess.Popen(
#                 ['pg_dump',
#                  '--dbname=postgresql://{}:{}@{}:{}/{}'.format("admin", 'admin', "localhost", "5432", "trial"),
#                  '-Fc',
#                  '-f', "db.sql",
#                  '-v'],
#                 stdout=subprocess.PIPE
#             )
#             output = process.communicate()[0]
#             if int(process.returncode) != 0:
#                 print('Command failed. Return code : {}'.format(process.returncode))
#                 exit(1)
#             print(output)
# except Exception as e:
#             print(e)
#             exit(1)

# import pymysql

# conn = pymysql.connect("localhost", "admin", "admin")
# print(conn)
# proc = subprocess.Popen(["mysql", "--user=%s" % "admin", "--password=%s" % "admin", "trial"],
#                         stdin=subprocess.PIPE,
#                         stdout=subprocess.PIPE)
# f = open("./db.sql", "r")
# out, err = proc.communicate(f.read())
# print(out)
# print(err)

# with open('db.sql', 'r') as f: 
#        command = ["mysql", "--user=%s" % "admin", "--password=%s" % "admin", "trial"]
#        proc = subprocess.Popen(command, stdin = f)
#        stdout, stderr = proc.communicate()
#        print(stdout)