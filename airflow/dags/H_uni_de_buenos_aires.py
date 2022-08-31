"""


1 -Configure DAG. To make an ETL for 2 different universities
2- Configure retry for DAG tasks
3- Configure the registry
"""
#Time functions
from datetime import timedelta, datetime
#Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#manager Folders
import os
#Pandas from datafrrame
import pandas as pd
#Logging
import logging


# Config logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
#Format
formatter = logging.Formatter('%(asctime)s-%(levelname)s-%(message)s', datefmt='%Y/%m/%d')
# add format a sh
sh.setFormatter(formatter)

#this function open csvfile and transform
def open_csv():
    file_name = "H_uni_de_buenos_aires.csv"
    filename = os.path.join(os.path.dirname(__file__),'../files/'+file_name)
    H_uni_de_buenos_aires = pd.read_csv(filename)
    return True

#this function open the sqlFile
def sql_reader(file):
    sql_file = open(file)
    return sql_file.read()

#Def extract
def extract_data ():
  #File .sql query
   file_sql = "H_uni_de_buenos_aires.sql"
  #dir .sql query
   file = os.path.join(os.path.dirname(__file__),'../include/'+file_sql)
  #read .sql query
   sql_query = sql_reader(file)

   logging.debug(f"The query use {sql_query}")

  #PostgresHook instance
   pg_hook = PostgresHook(
            postgres_conn_id = 'db_universidades_postgres',
            )
   #conect to the db
   pg_conn = pg_hook.get_conn()
   #csv with raw data
   file_name="H_uni_de_buenos_aires.csv"
    
   filename = os.path.join(os.path.dirname(__file__), '../files/'+file_name)

   new_sql_query= "COPY ( "+sql_query+" ) TO STDOUT WITH CSV HEADER"  
    # remove " ; " 
   new_sql_query=new_sql_query.replace(";","")
    # execute copy_expert
   pg_hook.copy_expert(new_sql_query,filename)

   return True

# ddag Config
default_args={
    #'owner': 'airflow-OT281-28',
    'retries': 5, # This comes from the SQL definition
    'retry_delay': timedelta(seconds=5) #fer 5 seg
}



#Init DAG
with DAG(
          dag_id='H_universidad_de_buenos_aires', 
          default_args=default_args,
          description='Consulta a la Universidad de buenos aires',
          schedule_interval='@hourly', ## DAG should be run every 1 hour, every day
          start_date=datetime(year=2022,month=8,day=19), ## YOU HAVE TO DEFINE THE DATE
          catchup=False
         ) as dag:

        #0 - init Log
        logger.debug("H_universidad_de_buenos_aires start")

        #1 - Data from postgres database
        uba_select_query=PythonOperator(
          task_id = 'extract_data_H_universidad_de_buenos_aires',
            python_callable = extract_data
        )
        
        #2 - Transforms Data
        uba_pandas_transform=PythonOperator(
          task_id = 'transform_data_H_universidad_de_buenos_aires',
            python_callable = open_csv
        )
 
        #3 - Load Data
        #uba_load_data=PythonOperator(
        #)

        uba_select_query >> uba_pandas_transform 
        #>> uba_load_data