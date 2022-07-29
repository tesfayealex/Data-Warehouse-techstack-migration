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

def get_schema_and_table_name(self):
       connection = db_connect.connect(host=host_name,user=db_user,password=db_password,database=db_name)
    
       cursor = connection.cursor()
       query = "SELECT table_schema,table_name FROM information_schema.tables WHERE table_schema != 'pg_catalog' and table_schema != 'information_schema' ORDER BY table_schema"

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

