import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
from datetime import timedelta, datetime
import logging as log
import numpy as np
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


""" ETL dag 
--Extract data from Facultad Latinoamericana De Ciencias Sociales Postgres database
--Transform the data with pandas
--Load the data to a AWS s3 database"""

log.basicConfig(level=log.ERROR,
                format='%(asctime)s - %(processName)s - %(message)s',
                datefmt='%Y-%m-%d')

#Postgre query
def query():

    DIR = os.path.dirname(os.path.normpath(__file__)).rstrip('/dags')
    FILE = f'{DIR}/include/G_facultad_latinoamericana_de_ciencias_sociales.sql'
    try:
        pg_hook = PostgresHook(postgres_conn_id='db_universidades_postgres', schema="training")
    except Exception as e:
        log.error(e)
        raise e


    #set file name
    file_name = "G_uni_ciencias_sociales.csv"
    
    #set path
    file_name_main = os.path.join(os.path.dirname(__file__), '../files/'+file_name)

    #open sql file

    j = open(FILE)

    j = j.read()
    
    #transform query
    sql_query = "COPY ( \n{0}\n ) TO STDOUT WITH CSV HEADER".format(j.replace(";", "")) 
    
    #get and save the file
    pg_hook.copy_expert(sql_query, file_name_main)

#Pandas data transformation
def pandas_process_func():
    DIR = os.path.dirname(os.path.normpath(__file__)).rstrip('/dags')

    try:
        df = pd.read_csv(f"{DIR}/files/G_uni_ciencias_sociales.csv", encoding='UTF-8')
        cp = pd.read_csv(f"{DIR}/assets/codigos_postales.csv", encoding='UTF-8' )
    except Exception as e:
        log.error(e)
        raise e

    cp.rename(columns= {'codigo_postal': 'postal_code', 'localidad': 'location'}, inplace = True)

    #to string and lower
    df["university"] = df["university"].str.lower()
    df["career"] = df["career"].str.lower()
    df["last_name"] = df["last_name"].str.lower()
    df["gender"] = df["gender"].str.lower()
    df["location"] = df["location"].str.lower()
    df["email"] = df["email"].str.lower()

    df["inscription_date"] = df["inscription_date"].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))

    cp["location"] = cp["location"].str.lower()
    cp["postal_code"] = cp["postal_code"]

    #strip spaces
    df["university"] = df["university"].str.strip()
    df["career"] = df["career"].str.strip()
    df["last_name"] = df["last_name"].str.strip()
    df["location"] = df["location"].str.strip()
    df["email"] = df["email"].str.strip()
    cp["location"] = cp["location"].str.strip()

    columns = ['university', 'career','last_name', 'location', 'postal_code']
    for i in columns:
        df[f"{i}"] = df[f"{i}"].replace(['-',],' ', regex=True)

    #merge
    mrg = pd.merge(
        df['location'],
        cp.drop_duplicates(subset=['location']),
        how="left",
        left_on='location',
        right_on='location'
    )
    df['postal_code'] = mrg['postal_code']#.replace(np.NaN,'')
    df['location'] = mrg['location']#.replace(np.NaN,'')
    df['postal_code'] = df['postal_code'].astype(str)

    #setting gender 
    df['gender'] = df['gender'].replace('m', 'male')
    df['gender'] = df['gender'].replace('f', 'female')

    #age
    df["birthdate"] =  pd.to_datetime(df["birthdate"], dayfirst=True)
    now = pd.Timestamp('now')
    df['age'] = (now - df['birthdate']).astype('<m8[Y]')
    df["age"] = df["age"].astype(int)

    #creating the file
    try:
        df.to_csv(f"{DIR}/datasets/G_uni_ciencias_sociales.csv")
    except Exception as e:
        log.error(e)
        raise e
        
#Load the data to AWS
def upload_to_s3():
    """This function upload the csv file to AWS S3 DB with a S3Hook"""

    log.info("Uploading cvs files")
    DIR = os.path.dirname(os.path.normpath(__file__)).rstrip('/dags')
    #S3 Hook
    try:
        s3_hook = S3Hook("universidades_S3")
    except Exception as e:
        log.error(e)
        raise e
    #Load to S3
    try:
        s3_hook.load_file(
            filename=f"{DIR}/datasets/G_uni_ciencias_sociales.csv",
            key="G_uni_ciencias_sociales.csv",
            bucket_name="cohorte-agosto-38d749a7",
            replace=True
        )
    except Exception as e:
        log.error(e)
        raise e

#retry 5 times with a delay of 5 seconds
default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    'G_uni_ciencias_sociales',
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

