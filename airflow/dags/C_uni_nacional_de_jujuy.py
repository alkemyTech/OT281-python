import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator

#import logging module
import logging
#Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)

#Create formatter
formatter= logging.Formatter( '%(asctime)s - %(name) - %(message)s' , datefmt='%Y-%m-%d' )
# Add formatter to ch
ch.setFormatter(formatter)

#Function to open the sql file and save it in a variable
def sql_reader(file):
    sql_file = open(file)
    return sql_file.read()


#Function to fetch data from Postgress 
def get_postgress_data():
    #file contains the path to the sql query for this university , could be changed to a dinamic option
    file = "/home/ariel/alkemy/entorno_python3_9/OT281-python/airflow/include/C_uni_nacional_de_jujuy.sql"
    sql_query = sql_reader(file)
    logging.debug(f'The query is {sql_query}')
    # CREATE PostgressHook instance
    # SET CONN ID WITH THE AIRFLOW CONN ID and DDBB NAME 
    pg_hook = PostgresHook(
        postgres_conn_id='db_universidades_postgress',
        schema='training'
    )
    #CONNECT TO THE DDBB
    pg_conn = pg_hook.get_conn()
    # SET FILE NAME FOR CSV WITH RAW DATA
    # PATH AND NAME SHOULD BE SET BY ARGUMENT
    filename=r"/home/ariel/alkemy/entorno_python3_9/OT281-python/airflow/files/C_uni_nacional_de_jujuy.csv"
    # FORMAT THE QUERY TO MAKE IT WORK WITH copy_expert
    new_sql_query= "COPY ( "+sql_query+" ) TO STDOUT WITH CSV HEADER"    
    # REMOVE " ; " CHAR TO GET IT WORK
    new_sql_query=new_sql_query.replace(";","")
    
    # EXECUTE copy_expert TO DOWNLOAD ALL THE RAW DATA TO CSV 
    pg_hook.copy_expert(new_sql_query,filename)

    return True


default_args={
    'owner':'airflow',
    'retries':5,
    'retry_delay':5
}


with DAG(
    dag_id='C_uni_nacional_de_jujuy',
    description='DAG to load data from Universiodad Nacional de Jujuy from postgress',
    schedule_interval='@hourly',
    start_date=datetime(year=2022, month=8, day=22),
    default_args=default_args,
    catchup=False
) as dag:
    logger.debug("C_uni_nacional_de_jujuy Starts")
    
    task_C_uni_nacional_de_jujuy_load_query = PythonOperator(
    	task_id='C_uni_nacional_de_jujuy_load_query',
    	python_callable=get_postgress_data,

)

    #task_C_uni_nacional_de_jujuy_load_query >> task_download_data