from datetime import datetime

from airflow import DAG
from airflow.operators.python import PythonOperator
#from airflow.hooks.postgres_hook import PostgresHook
#from airflow.providers.postgres.operators.postgres import PostgresOperator

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
#def sql_reader(file):
#    sql_file = open(file)
#    return sql_file.read()


#Function to fetch data from Postgress - uncomment to connect 
def get_postgress_data():
    pass
    #This file should be parametriced in the next version
#    file = "../include/C_uni_de_palermo.sql"
#    sql_query = sql_reader(file)
    
    #Uncomment to debug
    #logging.debug(f'The query file is {sql_query}')
    
    # SET CONN ID WITH THE AIRFLOW CONN ID 
#    pg_hook = PostgresHook(
#        postgres_conn_id='S3_postgres_OT281',
#        schema='training'
#    )
#    pg_conn = pg_hook.get_conn()
    
    #Uncomment to debug
    #logging.debug(f'The hook params are: {postgres_conn_id} and schema {schema}')
    
#    cursor = pg_conn.cursor()
    
    #Uncomment to debug
    #logging.debug("cursor was created successfully")
    
#    cursor.execute(sql_query)
#    return cursor.fetchall()

default_args={
    'owner':'airflow',
#    'retries':5,
#    'retry_delay':5
}

with DAG(
    dag_id='C_uni_de_palermo_',
    description='DAG to load data from Universiodad Nacional de Jujuy from postgress',
    schedule_interval='@daily',
    start_date=datetime(year=2022, month=8, day=22),
    default_args=default_args,
    catchup=False
) as dag:
    logger.debug("C_uni_de_palermo Starts")
    task_C_uni_nacional_de_jujuy_load_query = PythonOperator(
	task_id='C_uni_de_palermo_load_query',
	python_callable=get_postgress_data,
	do_xcom_push=True
)