'''
    ETL DAG for 'Universidad Tecnológica Nacional' data
    Only the extraction phase is functional in this DAG right now.
    The transformation task is defined but not functional yet.
    In this script we define the DAG structure and its tasks (represented as python functions).
    Ideally, this DAG will use 3 main operators in the future, one for each task:
        - PythonOperator with PostgresHook (Extract): Used to extract raw data from Postgres Database, by executing
            the SQL scripts in the 'include' folder.
        - PythonOperator (Transform): Used to transform and normalize the raw data from last step with Pandas.
        - LocalFilesystemToS3Operator (Load): Load the transformed data into AWS S3.
    The DAG will be executed hourly everyday.
    In case of an error, the DAG will try to execute again up to 5 times, with a delay of 5 seconds between each one.
'''

#Imports
#Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#Time functions
from datetime import datetime, timedelta
#Utilities
import os
#Logging
import logging

#Connection id for Postgres Database
POSTGRES_CONN_ID = "db_universidades_postgres"

#Get the root folder (project folder)
ROOT_FOLDER = os.path.dirname(os.path.normpath(__file__)).rstrip('/dags')
#File name for this university
FILE_NAME = "D_uni_tecnologica_nacional"
#Input path for .sql file from extraction
PATH_EXTRACT_INPUT = ROOT_FOLDER + "/include/" + FILE_NAME + ".sql"
#Output path for .csv file from extraction
PATH_EXTRACT_OUTPUT = ROOT_FOLDER + "/files/" + FILE_NAME + ".csv"

#Logger setup
logging.basicConfig(
    format='%(asctime)s - %(name)s - %(message)s',
    datefmt='%Y-%m-%d',
    level=logging.DEBUG
)
logger = logging.getLogger(__name__)


#Task functions
def extract_data(sql, file_path):
    """
    This method execute the given sql query in the postgres database,
    stores the result in a .csv file, and it saves it in the given path.

    Args:
        sql (str): SQL script to execute.
        file_path (str): path where it will save the generated .csv file.

    Returns:
        str: The .csv file path.
    """
    #Get Postgres Hook
    pg_hook = PostgresHook.get_hook(POSTGRES_CONN_ID)
    #Create the files folder if not exists
    os.makedirs(os.path.dirname(file_path), exist_ok=True)
    #Create an empty .csv file in the files folder (needed for copy_expert to work)
    with open(file_path, "w") as empty_csv:
        pass
    #Read the SQL file and save the script in a string
    SQL_script = ""
    with open(sql, 'r') as file:
        SQL_script = file.read()
    #Build the SQL copy query
    copy_query = "COPY (\n{0}\n) TO STDOUT WITH CSV HEADER".format(SQL_script.replace(';', ''))
    #Execute the SQL query and save the result into the .csv file
    pg_hook.copy_expert(copy_query, filename=file_path)
    #Return the generated .csv file path for the next task
    return file_path
def transform_data():
    pass
def load_data():
    pass

#Setup DAG default arguments
default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds = 5)
}

#Define and configure DAG
with DAG ('D_uni_tecnologica_nacional',
    description = 'ETL DAG for Universidad Tecnologica Nacional data.',
    start_date = datetime(2022, 8, 23),
    schedule_interval = '@hourly',
    default_args = default_args,
    catchup=False
) as dag:
    '''
    This task extract the raw data from postgres database
    and it stores it in a .csv file (in the 'files' folder) for future proccessing
    '''
    task_extract_data = PythonOperator(
        task_id = "extract_data",
        python_callable = extract_data,
        do_xcom_push=True,
        op_kwargs={
            "sql": PATH_EXTRACT_INPUT,
            "file_path": PATH_EXTRACT_OUTPUT
        }
    )
    '''
    This task transform and normalize the data from previous task
    and it stores it in a .csv file (in the 'datasets' folder)
    '''
    task_transform_data = PythonOperator(
        task_id = "transform_data",
        python_callable = transform_data
    )

    #Define the task sequence
    task_extract_data >> task_transform_data