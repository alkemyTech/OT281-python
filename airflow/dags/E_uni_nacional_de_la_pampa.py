"""DAG Universidad Nacional de la Pampa considering:
1- Future Operators
2- SQL query
3- Future Data preparation with pandas
4- Upload data to AWS S3 
5- Execution time: Every hour, every day
"""

from datetime import timedelta, datetime
import logging
import os

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.hooks.S3_hook import S3Hook

#Logs config
logging.basicConfig(datefmt= '%Y-%m-%d',
                    format='%(asctime)s - %(name)s - %(message)s',
                    level=logging.DEBUG) 

default_args = {
    'email':[''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}

#Get data and read it with SQL query
def sql_query():

    #Get the Sql query from "include" folder
    sql_query = os.path.join(os.path.dirname(os.path.normpath(__file__)).rstrip('/dags'), 'include/'+"E_uni_nacional_de_la_pampa.sql")
    sql_file = open(sql_query)
    sql_file = sql_file.read()
    sql_file = "COPY (" + sql_file.replace(";", "") + ") TO STDOUT WITH CSV HEADER"


    #Hook Creation and Connection with Postgress
    hook = PostgresHook(
        postgres_conn_id='db_universidades_postgres',
        schema='training'
    )
    logging.info("SQL succesfully extract")    


    #Export raw data in CSV format to the "files" directory
    filename_csv="E_uni_nacional_de_la_pampa.csv"
    filename = os.path.join(os.path.dirname(os.path.normpath(__file__)).rstrip('/dags'), 'files/'+filename_csv)

    # Return the data from the hook using the sql querys and export it in csv
    return hook.copy_expert(sql_file, filename)

#Transform data with pandas
def transform_data():
    pass

#Upload data to AWS S3
def upload_to_s3(filename: str, key: str, bucket_name: str):
    pass


with DAG(
    dag_id='E_uni_nacional_de_la_pampa',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2022,8,28),
) as dag:

    sql_query = PythonOperator(
        task_id='query_sql',
        python_callable=sql_query,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    sql_query >> transform_data >> upload_to_s3


'''
Resources:
a. DAG to Upload to S3 [https://betterdatascience.com/apache-airflow-amazon-s3/]
b. Logs configuration[https://docs.python.org/3/howto/logging.html]
c. Connection to Postgress[https://registry.astronomer.io/providers/postgres/modules/postgreshook]
'''