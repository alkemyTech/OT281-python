


# ==== START LIBRARIES IMPORT ====

# To manage folders and paths
import os
# For Pandas Dataframe
import pandas as pd
# For Time related functions
from datetime import datetime
# For Airflow 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
#import logging module
import logging


# ==== END LIBRARIES IMPORT ====





# ==== START FUNCTIONS DECLARE AND SETTINGS ====


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
    # file: its the sql name with its location in string format
    sql_file = open(file)
    return sql_file.read()


#Function to fetch data from Postgres 
def get_postgress_data():
    #file contains the path to the sql query for this university 
    file_name_sql="C_uni_nacional_de_jujuy.sql"
    # Add path to the file with the file name in str format
    file = os.path.join(os.path.dirname(__file__), '../include/'+file_name_sql)
    # Call sql_reader function to get the query in str format
    sql_query = sql_reader(file)
    
    logging.debug(f'The query is {sql_query}')
    
    # CREATE PostgressHook instance
    # SET CONN ID WITH THE AIRFLOW CONN ID and DDBB NAME 
    pg_hook = PostgresHook(
        postgres_conn_id='db_universidades_postgress',
        schema='training'
    )
    
    #CONNECT OBJECT TO THE DDBB
    pg_conn = pg_hook.get_conn()
    
    # SET FILE NAME FOR CSV WITH RAW DATA
    file_name_csv="C_uni_nacional_de_jujuy.csv"
    # Add path to the file with the file name in str format
    filename = os.path.join(os.path.dirname(__file__), '../files/'+file_name_csv)
    
    
    # FORMAT THE QUERY TO MAKE IT WORK WITH copy_expert
    new_sql_query= "COPY ( "+sql_query+" ) TO STDOUT WITH CSV HEADER"    
    # REMOVE " ; " CHAR TO GET IT WORK
    new_sql_query=new_sql_query.replace(";","")
    
    # EXECUTE copy_expert TO DOWNLOAD THE QUERY RAW DATA TO CSV 
    pg_hook.copy_expert(new_sql_query,filename)

    return True

# ==== END FUNCTIONS DECLARE AND SETTINGS ====


# ==== START AIRFLOW SETTINGS ====


# Set args for DAGS
default_args={
    #'owner':'airflow',
    'retries':5,
    'retry_delay':5
}

# Set DAG
with DAG(
    dag_id='C_uni_nacional_de_jujuy',
    description='DAG for ETL process with Universiodad Nacional de Jujuy',
    schedule_interval='@hourly',
    start_date=datetime(year=2022, month=8, day=22),
    default_args=default_args,
    catchup=False
) as dag:
    logger.debug("C_uni_nacional_de_jujuy Starts")
    
# Define task to query and donwload data to csv
    task_C_uni_nacional_de_jujuy_load_query = PythonOperator(
    	task_id='C_uni_nacional_de_jujuy_load_query',
    	python_callable=get_postgress_data,

)


# SET AIRFLOW FLOW PROCESS 
    #task_C_uni_nacional_de_jujuy_load_query >> task_download_data


# ==== END AIRFLOW SETTINGS ====
