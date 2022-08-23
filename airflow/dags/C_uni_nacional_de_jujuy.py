
from datetime import datetime
from msilib import schema
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

#import logging module
import logging



#Function to open the sql file and save it in a variable
def sql_reader(file):
    sql_file = open(file)
    return sql_file.read()


#Function to fetch data from Postgress 
def get_postgress_data():
    file = "../include/C_uni_nacional_de_jujuy.sql"
    sql_query = sql_reader(file)
    logging.debug(f'The query file is {sql_query}')
    # SET CONN ID WITH THE AIRFLOW CONN ID 
    pg_hook = PostgresHook(
        postgres_conn_id='S3_postgres_OT281',
        schema='training'
    )
    pg_conn = pg_hook.get_conn()
    logging.debug(f'The hook params are: {postgres_conn_id} and schema {schema}')
    cursor = pg_conn.cursor()
    logging.debug("cursor was created successfully")
    cursor.execute(sql_query)
    return cursor.fetchall()

default_args={
    'owner':'airflow',
    'retries':5,
    'retry_delay':5
}


with DAG(
    dag_id='C_uni_nacional_de_jujuy_',
    description='DAG to load data from Universiodad Nacional de Jujuy from postgress',
    schedule_interval='@daily',
    start_date=datetime(year=2022, month=8, day=22),
    default_args=default_args,
    catchup=False
) as dag:
    task_C_uni_nacional_de_jujuy_load_query = PythonOperator(
	task_id='C_uni_nacional_de_jujuy_load_query',
	python_callable=get_postgress_data,
	do_xcom_push=True
)