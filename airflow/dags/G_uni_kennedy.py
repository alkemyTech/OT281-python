import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import timedelta, datetime
import logging as log

""" ETL dag 
--Extract data from Universidad J. F. Kennedy Postgres database
--Transform the data with pandas
--Load the data to a AWS s3 database"""

log.basicConfig(level=log.ERROR,
                format='%(asctime)s - %(processName)s - %(message)s',
                datefmt='%Y-%m-%d')

#Postgre query
def query():
    pg_hook = PostgresHook(postgres_conn_id='db_universidades_postgres', schema="training")

    #set file name
    file_name = "G_uni_kennedy.csv"
    
    #set path
    file_name_main = os.path.join(os.path.dirname(__file__), '../files/'+file_name)

    #open sql file
    j = open('OT281-python/airflow/include/G_universidad_kennedy.sql')
    j = j.read()
    
    #transform query
    sql_query = "COPY ( \n{0}\n ) TO STDOUT WITH CSV HEADER".format(j.replace(";", "")) 
    
    #get and save the file
    pg_hook.copy_expert(sql_query, file_name_main)

#Pandas data transformation
def pandas_process_func():
    pass
#Load the data to AWS
def upload_to_s3():
    pass

#retry 5 times with a delay of 5 seconds
default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    'G_uni_kennedy',
    default_args = default_args,
    description = 'DAG for Universidad J. F. Kennedy',
    schedule_interval = timedelta(hours = 1),
    start_date = datetime(2022, 8, 22)
) as dag:
    query_sql = PythonOperator(task_id = 'query_sql',
                               python_callable = query) 
    pandas_process = PythonOperator(task_id = 'pandas_process',
                                    python_callable = pandas_process_func)   
    load_S3 = PythonOperator(task_id = 'load_S3',
                                python_callable = upload_to_s3) 


    query_sql >> pandas_process >> load_S3
