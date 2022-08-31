import os
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
import pandas as pd
import numpy as np
from datetime import timedelta, datetime
from dateutil.relativedelta import relativedelta
import logging as log


""" ETL dag 
--Extract data from Universidad J. F. Kennedy Postgres database
--Transform the data with pandas
--Load the data to a AWS s3 database"""

log.basicConfig(level=log.ERROR,
                format='%(asctime)s - %(processName)s - %(message)s',
                datefmt='%Y-%m-%d')


#Postgre query
def query():
    DIR = os.path.dirname(os.path.normpath(__file__)).rstrip('/dags')
    FILE = f'{DIR}/include/G_universidad_kennedy.sql'

    try:
        pg_hook = PostgresHook(postgres_conn_id='db_universidades_postgres', schema="training")
    except Exception as e:
        log.error(e)
        raise e  

    #set file name
    file_name = "G_uni_kennedy.csv"
    
    #set path
    file_name_main = os.path.join(os.path.dirname(__file__), '../files/'+file_name)

    #open sql file
    try:
        j = open(FILE)
        j = j.read()
    except Exception as e:
        log.error(e)
        raise e
    
    #transform query
    sql_query = "COPY ( \n{0}\n ) TO STDOUT WITH CSV HEADER".format(j.replace(";", "")) 
    
    #get and save the file
    pg_hook.copy_expert(sql_query, file_name_main)

#Pandas data transformation
def pandas_process_func():
    DIR = os.path.dirname(os.path.normpath(__file__)).rstrip('/dags')

    try:
        #df = pd.read_csv("OT281-python/airflow/files/G_uni_kennedy.csv", encoding='UTF-8')
        df = pd.read_csv(f"{DIR}/files/G_uni_kennedy.csv", encoding='UTF-8')
        #cp = pd.read_csv("OT281-python/airflow/assets/codigos_postales.csv", encoding='UTF-8' )
        cp = pd.read_csv(f'{DIR}/assets/codigos_postales.csv', encoding='UTF-8')
    except Exception as e:
        log.error(e)
        raise e

    cp.rename(columns= {'codigo_postal': 'postal_code', 'localidad': 'location'}, inplace = True)

    #to string and lower
    df["university"] = df["university"].str.lower()
    df["career"] = df["career"].str.lower()
    df["last_name"] = df["last_name"].str.lower()
    df["gender"] = df["gender"].str.lower()
    df["postal_code"] = df["postal_code"].astype(str)
    df["email"] = df["email"].str.lower()
    df["inscription_date"] = df["inscription_date"].apply(lambda x: datetime.strptime(x,'%Y-%m-%d'))


    cp["location"] = cp["location"].str.lower()
    cp["postal_code"] = cp["postal_code"].astype(str)

    #strip spaces
    df["university"] = df["university"].str.strip()
    df["career"] = df["career"].str.strip()
    df["last_name"] = df["last_name"].str.strip()
    df["postal_code"] = df["postal_code"].str.strip()
    df["email"] = df["email"].str.strip()
    cp["location"] = cp["location"].str.strip()

    columns = ['university', 'career','last_name', 'location', 'postal_code']
    for i in columns:
        df[f"{i}"] = df[f"{i}"].replace(['-',],' ', regex=True)

    #merge
    mrg = pd.merge(
        df['postal_code'],
        cp.drop_duplicates(subset=['postal_code']),
        how="left",
        left_on='postal_code',
        right_on='postal_code'
    )
    df['location'] = mrg['location']#.replace(np.NaN,'')
    df['postal_code'] = mrg['postal_code']#.replace(np.NaN,'')
    df['postal_code'] = df['postal_code'].astype(str)

    #setting gender 
    df['gender'] = df['gender'].replace('m', 'male')
    df['gender'] = df['gender'].replace('f', 'female')


    #age
    df.drop(df.loc[df['birthdate']=='29-Feb-10'].index, inplace=True)

    df["birthdate"] = pd.to_datetime(df["birthdate"], yearfirst=True)
    max_year = datetime.now().year - 12

    df["birthdate"] = df["birthdate"].apply(lambda x : x - relativedelta(years=100) if x.year > max_year else x)

    df['age'] = df["birthdate"].apply(lambda x: (relativedelta(datetime.now(), x).years))

    #creating the file
    try:
        #df.to_csv("OT281-python/airflow/datasets/G_uni_kennedy.csv")
        df.to_csv(f"{DIR}/datasets/G_uni_kennedy.csv")
    except Exception as e:
        log.error(e)
        raise e
        
#Load the data to AWS
def upload_to_s3():
    pass

#retry 5 times with a delay of 5 seconds
default_args = {
    'retries': 5,
    'retry_delay': timedelta(seconds=5),
}

with DAG(
    'G_uni_kennedy',
    default_args = default_args,
    description = 'DAG for Universidad J. F. Kennedy',
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
