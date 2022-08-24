'''
    ETL DAG for 'Universidad Nacional de Tres de Febrero' data

    This is a DAG design, it means is not functional.
    In this script we define the DAG structure and its tasks (represented as python functions).
    Ideally, this DAG will use 3 main operators in the future, one for each task:
        - PostgresOperator (Extract): Used to extract raw data from Postgres Database, by executing
            the SQL scripts in the 'include' folder.
        - PythonOperator (Transform): Used to transform and normalize the raw data from last step with Pandas.
        - LocalFilesystemToS3Operator (Load): Load the transformed data into AWS S3.
    The DAG will be executed hourly everyday.
'''

#Imports
#Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
#Time functions
from datetime import datetime, timedelta

#Task functions
def extract_data():
    pass
def transform_data():
    pass
def load_data():
    pass

#Setup DAG default arguments
default_args = {}

#Define and configure DAG
with DAG ('DAG_D_uni_nacional_de_tres_de_febrero',
    description = 'ETL DAG for Universidad Nacional de Tres de Febrero data.',
    start_date=datetime(2022, 8, 23),
    schedule_interval='@hourly',
    default_args=default_args,
) as dag:
    #Task that extract raw data from postgres database
    task_extract_data = PythonOperator(
        task_id = "extract_data",
        python_callable = extract_data
    )
    #Task that transform and normalize the extracted data with pandas
    task_transform_data = PythonOperator(
        task_id = "transform_data",
        python_callable = transform_data
    )
    #Task that load the transformed data into a AWS S3 database
    task_load_data = PythonOperator(
        task_id = "load_data",
        python_callable = load_data
    )

    #Define the task sequence
    task_extract_data >> task_transform_data >> task_load_data
