from datetime import timedelta,datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
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

# import os
# from datetime import datetime

# import pandas as pd
# from airflow import DAG
# from airflow.operators.bash_operator import BashOperator
# from airflow.operators.dummy import DummyOperator
# from airflow.operators.python_operator import PythonOperator
# from airflow.providers.postgres.hooks.postgres import PostgresHook
# from airflow.providers.postgres.operators.postgres import PostgresOperator

# deployment = os.environ.get("DEPLOYMENT", "prod")


# def read_data():
#     df = pd.read_csv(
#         "/home/hp/airflow/dags/data/dataset.csv",
#         skiprows=1,
#         header=None,
#         delimiter="\n",
#     )

#     return df.shape


# def insert_data():
#     pg_hook = PostgresHook(postgres_conn_id=f"traffic_data_{deployment}")
#     conn = pg_hook.get_sqlalchemy_engine()
#     df = pd.read_csv(
#         "/home/hp/airflow/dags/data/dataset.csv",
#         sep="[,;:]",
#         index_col=False,
#     )

#     df.to_sql(
#         "traffic_flow",
#         con=conn,
#         if_exists="replace",
#         index=False,
#     )


# default_args = {
#     "owner": "tesfaye",
#     "start_date": datetime(2022, 7, 19, 8, 25, 00),
#     "concurrency": 1,
#     "retries": 0,
# }


# dag = DAG(
#     "traffic_flow_dag",
#     default_args=default_args,
#     schedule_interval="@daily",
#     catchup=False,
# )
# envs = ["dev", "stg", "prod"]

# with dag:
#     start = DummyOperator(task_id="start")

#     # dbt_test_op = BashOperator(
#     #     task_id="dbt_test",
#     #     bash_command="dbt run --profiles-dir /opt/airflow/dbt --project-dir /opt/airflow/dbt",
#     # )

#     read_data_op = PythonOperator(
#         task_id="read_data", python_callable=read_data
#     )

#     create_table_op = PostgresOperator(
#         task_id=f"create_table_{deployment}",
#         postgres_conn_id=f"traffic_data_{deployment}",
#         sql="""
#             create table if not exists traffic_flow (
#                 track_id integer,
#                 type integer,
#                 traveled_d integer,
#                 avg_speed integer,
#                 lat integer,
#                 lon integer,
#                 speed integer,
#                 lon_acc integer,
#                 lat_acc integer,
#                 time integer,
#                 primary key (track_id)
#             )
#         """,
#     )

#     load_data_op = PythonOperator(
#         task_id=f"load_traffic_data_{deployment}", python_callable=insert_data
#     )

#     start >> read_data_op >> create_table_op >> load_data_op

# import pandas as pd
# df = pd.read_csv(
#         "/home/hp/airflow/dags/data/dataset.csv",
#         skiprows=1,
#         header=None,
#         delimiter=";",
#     )
# print(df.info())

# from sqlalchemy import create_engine
# import pandas as pd
# import os
# from datetime import timedelta,datetime
# import airflow
# from airflow import DAG
# from airflow.operators.python import PythonOperator


# default_args={
#     'owner':'martinluther',
#     'retries':5,
#     'retry_delay':timedelta(minutes=2)
# }


# def migrate_data(path,db_table):
#     engine = create_engine("postgresql://fdaxvbwukyukwg:027f641d5f0f22fbaaa30072c4ec597e296abd558c957ae5698cf603f27cbc3e@ec2-54-152-28-9.compute-1.amazonaws.com:5432/da5npbld631uqb",
#              echo=True, future=True)
#     print(os.system('pwd'))
#     df = pd.read_csv(path,sep="[,;:]",index_col=False)
#     print("<<<<<<<<<<start migrating data>>>>>>>>>>>>>>")
#     df.to_sql(db_table, con=engine, if_exists='replace',index_label='id')
#     print("<<<<<<<<<<<<<<<<<<<completed>>>>>>>>>>>>>>>>")



# with DAG(
#     dag_id='dag_data',
#     default_args=default_args,
#     description='this dag handles data manipulations',
#     start_date=airflow.utils.dates.days_ago(1),
#     schedule_interval='@hourly'
# )as dag:
#     task1 = PythonOperator(
#         task_id='migrate',
#         python_callable=migrate_data,
#         op_kwargs={
#             "path": "./dags/dataset.csv",
#             "db_table":"endpoints_trafficinfo"
#         }
#     )
#     task1
# from sqlalchemy import create_engine
# import psycopg2
# import pandas as pd
# conn_string = "host='localhost' dbname='trial' user='admin' password='admin'"
# conn = psycopg2.connect(conn_string)

# engine = create_engine('postgresql+psycopg2://admin:admin@localhost/trial')
# #              echo=True, future=True)
#     # print(os.system('pwd'))
# df = pd.read_csv("/home/hp/airflow/dags/data/t_data.csv",sep="[;]",index_col=False)
# print("<<<<<<<<<<start migrating data>>>>>>>>>>>>>>")
# print(df)
# df.to_sql("t_data", con=engine, if_exists='replace',index_label='id')
# print("<<<<<<<<<<<<<<<<<<<completed>>>>>>>>>>>>>>>>")