import pandas as pd
from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.models import Variable

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
    #logging.debug(f'The query is {sql_query}')
    # SET CONN ID WITH THE AIRFLOW CONN ID 
    pg_hook = PostgresHook(
        postgres_conn_id='db_universidades_postgress',
        schema='training'
    )
    pg_conn = pg_hook.get_conn()
    #logging.debug(f'The hook params are: {postgres_conn_id} and schema {schema}')
    cursor = pg_conn.cursor()
    logging.debug("cursor was created successfully")
    cursor.execute(sql_query)
    print(cursor)
    return cursor.fetchall()

def process_sql_data(ti):
    query_retrieved = ti.xcom_pull(task_ids=['get_postgress_data'])
    if not query_retrieved:
        raise Exception("NO DATA LOADED")

    query_retrieved_pd = pd.DataFrame(
        data=query_retrieved[0],
        columns=['university','career','inscription_date' ,'last_name','gender','birth_date' ,'age','postal_code','location','email']
    )
    query_retrieved_pd.to_csv(Variable.get('location_C_uni_nacional_de_jujuy_csv'),index=False)

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
	    do_xcom_push=True
)
    task_download_data = PythonOperator(
        task_id='download_data',
        python_callable=process_sql_data
    )

    task_C_uni_nacional_de_jujuy_load_query >> task_download_data