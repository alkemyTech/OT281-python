"""

1 -Configure DAG. To make an ETL for 2 different universities
2- Configure retry for DAG tasks
3- Configure the registry
"""
# Time functions
from datetime import timedelta, datetime
# Airflow
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
# manager Folders
import os
# Pandas from datafrrame
import pandas as pd
# Logging
import logging
from dateutil.relativedelta import relativedelta


# Config logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

sh = logging.StreamHandler()
sh.setLevel(logging.DEBUG)
# Format
formatter = logging.Formatter(
    '%(asctime)s-%(levelname)s-%(message)s', datefmt='%Y/%m/%d')
# add format a sh
sh.setFormatter(formatter)

# Path
current_dir = os.path.dirname(os.path.normpath(__file__))
ROOT_FOLDER = os.path.abspath(os.path.join(current_dir, os.pardir))
FILE_NAME = 'H_uni_del_cine'


# this function open csvfile and transform
def open_csv():
    file_name = "H_uni_del_cine.csv"
    filename = os.path.join(ROOT_FOLDER, '../files/'+file_name)
    H_uni_del_cine = pd.read_csv(filename)
    return True
# this function open the sqlFile


def sql_reader(file):
    sql_file = open(file)
    return sql_file.read()


# Def extract
def extract_data():
    # File .sql query
    file_sql = "H_uni_del_cine.sql"
    # read .sql query
    sql_query = sql_reader(f'{ROOT_FOLDER}/include/{file_sql}')

    logging.debug(f"The query use {sql_query}")

    # PostgresHook instance
    pg_hook = PostgresHook(
        postgres_conn_id='db_universidades_postgres',
    )
    # conect to the db
    pg_conn = pg_hook.get_conn()
    # csv with raw data
    file_name = "H_uni_del_cine.csv"

    filename = f'{ROOT_FOLDER}/files/{file_name}' #

    new_sql_query = "COPY ( "+sql_query+" ) TO STDOUT WITH CSV HEADER"
    # remove " ; "
    new_sql_query = new_sql_query.replace(";", "")
    # execute copy_expert
    pg_hook.copy_expert(new_sql_query, filename)

    return True
############################### DATA TRANSFORM #####################################################

def transform_data_pd(csv, csv_transform):
    """
    Function for transform data from Universidad del Cine previously extracted from Postgres database.
        - Read previously extracted csv
        - Import postal code asset csv
        - Transform data to make a .txt output prepared to be loaded to S3
    Args:
        csv (str): input filename, extracted data
        txt (str): output filename, transformed data

    """
    
    ##Open CSV 
    df = pd.read_csv(f'{ROOT_FOLDER}/files/{csv}', encoding='utf-8')

    #get postal code df
    df_code_post = pd.read_csv(f'{ROOT_FOLDER}/assets/codigos_postales.csv', encoding='utf-8')
          
     #df postal code
    df_code_post.columns = ["postal_code", "location"]
    df_code_post["location"] = df_code_post["location"].apply(str.lower).apply(str.strip).apply(str.strip, args=(['-']))
    df_code_post.drop_duplicates(subset=['location'], keep="first", inplace=True)
    
     #########################################

    df["location"] = df["location"].apply(lambda x: x.replace('-',' ')).apply(lambda x: x.lower()).apply(lambda x: x.strip())

     ###############################################

     #Detect the column to merge on
    merge_col = "location" if df["postal_code"].isnull().all() else "postal_code"
     #Detect the null column
    null_col = "postal_code" if merge_col == "location" else "location"
     #Merge on merge_col
    df = df.merge(df_code_post, how="left", on=merge_col, copy="false")
     #Clean old null column
    df.drop(null_col + "_x", axis=1, inplace=True)
     #Rename new merged column
    df.rename({null_col + "_y" : null_col}, axis=1, inplace=True)

     #Normalize text columns (remove extra spaces, hyphens and convert to lowercase)
    df['university'] = df['university'].apply(lambda x: x.replace('-',' ')).apply(lambda x: x.lower()).apply(lambda x: x.strip())
     ###########################
    df['career'] = df['career'].apply(lambda x: x.replace('-',' ')).apply(lambda x: x.lower()).apply(lambda x: x.strip())
     ############
    df["inscription_date"] = pd.to_datetime(df["inscription_date"])
    df['inscription_date'] = pd.to_datetime(df['inscription_date']).apply(lambda x : x.strftime('%Y-%m-%d'))
     ################################
    df['last_name'] = df['last_name'].apply(lambda x: x.replace('-',' ')).apply(lambda x: x.lower())
     ################################
    current_year = datetime.now().year
    df["birth_date"] = pd.to_datetime(df["birth_date"])
    df["birth_date"] = df["birth_date"].apply(lambda x : x - relativedelta(years=100) if x.year >= current_year else x)
     #################################
    df["age"] = ((datetime.now() - df["birth_date"]) // timedelta(days=365.2425))

    df["birth_date"] = df["birth_date"].apply(lambda x : x.strftime('%Y-%m-%d'))

     #Define postal_code column type as string
    df["postal_code"] = df["postal_code"].astype(str)

     #Rename gender values and define gender

    df['gender'] = df['gender'].apply(lambda x: x.upper()).apply(lambda x: x.replace('F','female')).apply(lambda x: x.replace('M','male'))

     ##########################
     
    result = df[['university', 'career', 'inscription_date', 'first_name', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
     #Define the dataset directory
    dataset_dir = ROOT_FOLDER + "/datasets/" + csv_transform
     #Create folder "datasets" if not exists
    os.makedirs(os.path.dirname(dataset_dir), exist_ok=True)
     #Store the transformed data in a .csv file in the "datasets" folder
    result.to_csv(dataset_dir)

     #Return the dataset directory to the next task
    return dataset_dir


################## DAG CONFIG #############################
default_args = {
    # "owner": "airflow-OT281-28",
    "retries": 5,  # This comes from the SQL definition
    "retry_delay": timedelta(seconds=5)  # fer 5 seg
}

################## DAG INIT #############################
with DAG(
    dag_id='H_universidad_del_cine',
    default_args=default_args,
    description='Consulta a la Universidad del Cine',
    schedule_interval='@hourly',  # DAG should be run every 1 hour, every day
    # YOU HAVE TO DEFINE THE DATE
    start_date=datetime(year=2022, month=8, day=19),
    catchup=False
) as dag:

    # 0 - init Log
    logger.debug("H_universidad_del_cine start")

    # 1 - Data from postgres database
    udc_extract_data = PythonOperator(
        task_id='extract_data_H_universidad_del_cine',
        python_callable=extract_data
    )

    # 2 - Transforms Data
    udc_pandas_transform = PythonOperator(
       task_id='transform_data_H_universidad_del_cine',
       python_callable=transform_data_pd,
       op_kwargs={
                        'csv': 'H_uni_del_cine.csv',
                        'csv_transform': 'H_uni_del_cine.csv'
                        }
    )

    # 3 - Load Data
    # udc_load_data=PythonOperator(
    # )

    # 4 - The execution order of the DAG
    udc_extract_data >> udc_pandas_transform
    # >> udc_load_data
