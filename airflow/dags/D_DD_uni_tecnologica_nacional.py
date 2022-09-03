'''
    ETL DAG for 'Universidad Tecnológica Nacional' data
    In this script we define the DAG structure and its tasks (represented as python functions).
    This DAG use 3 main operators, one for each task:
        - PythonOperator with PostgresHook (Extract): Used to extract raw data from Postgres Database, by executing
            the SQL scripts in the 'include' folder.
        - PythonOperator (Transform): It is used to transform and normalize the raw data from the 
            extraction task step with Pandas and store it in the 'datasets' folder.
        - PythonOperator with S3Hook (Load): Load the dataset from transformation task into a S3 bucket.
    The DAG will be executed hourly everyday.
    In case of an error, the DAG will try to execute again up to 5 times, with a delay of 5 seconds between each one.
'''

#Imports
#Airflow
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
#Data manipulation
import pandas as pd
#Time functions
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
#Utilities
import os
#Logging
import logging

#Connection id for Postgres Database
POSTGRES_CONN_ID = "db_universidades_postgres"
#Connection id for S3
S3_CONN_ID = "universidades_S3"
#Bucket name
S3_BUCKET_NAME = "cohorte-agosto-38d749a7"

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

def transform_data(ti):
    """
    This method proccess, transform and normalize the data generated by the extraction task.

    Args:
        ti (taskInstance): Is the previous task instance.
            It is used to pull the .csv file path from the extraction task.

    Raises:
        ValueError: Raised if can't pull Xcom value from the task instance.

    Returns:
        str: Return the generated Dataset directory to the next task.
    """
    #Get the Xcom values from previous task
    Xcom_values = ti.xcom_pull(task_ids=['extract_data'])
    #Raise an error if not found
    if not Xcom_values:
        raise ValueError("File path from extract_data task not found in XComs.")
    #Get the file_path (should be the only Xcom value in the list)
    file_path = Xcom_values[0]
    
    #Get the universities dataframe
    df = pd.read_csv(file_path)

    #Get the postal codes dataframe
    df_cp = pd.read_csv(ROOT_FOLDER + '/assets/codigos_postales.csv')

    #Normalize postal codes dataframe
    df_cp.columns = ["postal_code", "location"]
    df_cp["location"] = df_cp["location"].apply(str.lower).apply(str.strip).apply(str.strip, args=(['-']))
    #Drop duplicated locations on postal code dataframe, keeping the first occurrence
    df_cp.drop_duplicates(subset=['location'], keep="first", inplace=True)

    #Detect the column to merge on
    merge_col = "location" if df["postal_code"].isnull().all() else "postal_code"
    #Detect the null column
    null_col = "postal_code" if merge_col == "location" else "location"
    #Merge on merge_col
    df = df.merge(df_cp, how="left", on=merge_col, copy="false")
    #Clean old null column
    df.drop(null_col + "_x", axis=1, inplace=True)
    #Rename new merged column
    df.rename({null_col + "_y" : null_col}, axis=1, inplace=True)

    #Normalize text columns (remove extra spaces, hyphens and convert to lowercase)
    text_columns = ['university', 'career', 'first_name', 'last_name', 'location', 'email']
    df[text_columns] = df[text_columns].astype(str)
    for text_col in text_columns:
        df[text_col].apply(str.lower).apply(str.strip).apply(str.strip, args=(['-']))

    #Define postal_code column type as string
    df["postal_code"] = df["postal_code"].astype(str)

    #Rename gender values
    df["gender"] = df["gender"].map({'m' : 'male', 'f' : 'female'})
    #Define gender column type as category
    df["gender"] = df["gender"].astype('category')

    #Get current year
    current_year = datetime.now().year

    #Define inscription_date as a string with the format '%Y-%m-%d'
    df["inscription_date"] = pd.to_datetime(df["inscription_date"])
    df["inscription_date"] = df["inscription_date"].apply(lambda x : x.strftime('%Y-%m-%d'))

    #Convert birth_date to a date
    df["birth_date"] = pd.to_datetime(df["birth_date"])
    #Correct year century (1900 or 2000)
    df["birth_date"] = df["birth_date"].apply(lambda x : x - relativedelta(years=100) if x.year >= current_year else x)
    #Calculate age column (// : floor division)
    df["age"] = ((datetime.now() - df["birth_date"]) // timedelta(days=365.2425))
    #Now convert birth_date to a string with the format '%Y-%m-%d'
    df["birth_date"] = df["birth_date"].apply(lambda x : x.strftime('%Y-%m-%d'))

    #Define the dataset directory
    dataset_dir = ROOT_FOLDER + "/datasets/" + FILE_NAME + ".csv"
    #Create folder "datasets" if not exists
    os.makedirs(os.path.dirname(dataset_dir), exist_ok=True)
    #Store the transformed data in a .csv file in the "datasets" folder
    df.to_csv(dataset_dir)
    
    #Return the dataset directory to the next task
    return dataset_dir

def load_data(ti):
    """
    This methods uploads the generated dataset by transformation task into a S3 bucket.

    Args:
        ti (taskInstance): Is the previous task instance.
            It is used to pull the dataset path from the transformation task.

    Raises:
        ValueError: Raised if can't pull Xcom value from the task instance.
    """
    #Get the Xcom values from previous task
    Xcom_values = ti.xcom_pull(task_ids=['transform_data'])
    #Raise an error if not found
    if not Xcom_values:
        raise ValueError("File path from transform_data task not found in XComs.")
    #Get the file_path (should be the only Xcom value in the list)
    file_path = Xcom_values[0]
    # Instantiate the S3 Hook
    s3_hook = S3Hook(S3_CONN_ID)
    # Load dataset to S3
    s3_hook.load_file(
        filename=file_path,
        key=FILE_NAME + ".csv",
        bucket_name=S3_BUCKET_NAME,
        replace=True
    )

#Setup DAG default arguments
default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds = 5)
}

#Define and configure DAG
with DAG ("D_DD_uni_tecnologica_nacional",
    description = "ETL DAG for Universidad Tecnológica Nacional data.",
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
        python_callable = transform_data,
        do_xcom_push=True
    )
    '''
    This task load the transformed data into a S3 bucket
    '''
    task_load_data = PythonOperator(
        task_id = "load_data",
        python_callable = load_data
    )

    #Define the task sequence
    task_extract_data >> task_transform_data >> task_load_data