from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import timedelta, datetime

""" ETL dag 
--Extract data from Facultad Latinoamericana De Ciencias Sociales Postgres database
--Transform the data with pandas
--Load the data to a AWS s3 database"""

#Postgre query
def query():
    pass
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
    'dag_ciencias_sociales1_g',
    default_args = default_args,
    description = 'DAG for Facultad Latinoamericana De Ciencias Sociales',
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

