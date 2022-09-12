'''
COMO: Analista de datos
QUIERO: Utilizar un operador creado por la comunidad
PARA: poder subir el txt creado por el operador de Python al S3
'''

#Airflow Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.amazon.aws.hooks.s3 import S3Hook


# Other imports
from datetime import datetime, timedelta, date
import logging
import os
from pathlib import Path
import pandas as pd


#Connection id for Postgres Database
POSTGRES_CONN_ID = "db_universidades_postgres"

#Connection id for S3
s3_id = 'universidades_S3'

#Define bucket name
s3_bucket = 'cohorte-agosto-38d749a7'

# Filename
file_name = 'F_uni_de_moron'

#Define airflow root folder

air_root_folder = os.path.dirname(os.path.normpath(__file__)).rstrip('/dags')

#Log Config
logging.basicConfig(level=logging.INFO, datefmt= '%Y-%m-%d',
                    format='%(asctime)s - %(name)s - %(message)s') 
             

logger = logging.getLogger('F_uni_de_moron')



#Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}

# SQL extraction
def sql_query():
    '''
    This function gets data from Postgres DB and stores it as a .csv in /file folder according to the sql query
    '''
    pg_hook= PostgresHook.get_hook(POSTGRES_CONN_ID)
   
    #Log
    logging.info('Exporting query to file')

    #Sql path
    sql_path = os.path.join(air_root_folder, 'include/'+ file_name+ '.sql')
    

    #csv path
    csv_path = os.path.join(air_root_folder, 'files/' + file_name + '.csv')


    #Open and read sql file
    with open(sql_path, "r") as file:
        sql = file.read()
        sql = "COPY (" + sql.replace(";", "") + ") TO STDOUT WITH CSV HEADER" 
    
    #Run query and storage it in csv file
    pg_hook.copy_expert(sql, csv_path)

#Pandas processing and Transformation
def pandas_process():
    '''
    This function uses pandas to normalize data in .csv and stores it in /dataset folder as a .csv
    '''
    #Load university dataframe
    df_uni = pd.read_csv(os.path.join(air_root_folder, 'files/' + file_name + '.csv'))
    
    #Log
    logger.info('Pandas transformation in process')

    #Applied no lowercase, no extra spaces, no hyphens in specific cols
    special_cols = ['university', 'career', 'last_name', 'email', 'location']
    for name_col in special_cols:
        df_uni[name_col] = df_uni[name_col].astype(str).apply(lambda x: x.lower()).apply(lambda x: x.strip()).apply(lambda x:x.replace('-', ' '))

    #Change gender col to category type
    df_uni["gender"] = df_uni["gender"].map({'M' : 'male', 'F' : 'female'})
    df_uni["gender"] = df_uni["gender"].astype('category')

    #Inscription date
    df_uni["inscription_date"] = pd.to_datetime(df_uni["inscription_date"],infer_datetime_format=True)
    df_uni['inscription_date'] = df_uni['inscription_date'].astype(str)

    #Age
    df_uni['birth_date'] = pd.to_datetime(df_uni['birth_date'],infer_datetime_format=True)
    def calc_age(birth_col):
        '''
        Given a birth date columns it returns the years according to current date
        '''
        today = pd.to_datetime(date.today())  # convert today to a pandas datetime
        return (today - birth_col) / pd.Timedelta(days=365.25)  # divide by days to get years

    df_uni['age'] = calc_age(df_uni['birth_date'])
    df_uni['age']= df_uni['age'].astype(int)

    #Load postal code dataframe
    df_pc = pd.read_csv(air_root_folder + '/assets/codigos_postales.csv')
    
    #working with postal_code
    df_pc.columns = ['postal_code', 'location']
    df_pc['location'] = df_pc['location'].apply(lambda x: x.lower()).apply(lambda x: x.strip()).apply(lambda x:x.replace('-', ' '))
    df_pc.drop_duplicates(subset=['location'], keep="first", inplace=True)
    #merge df_uni with df_pc on 'postal_code'
    df = df_uni.merge(df_pc, how="left", on='postal_code', copy="false")
    df.drop('location_x', axis = 1, inplace = True)
    df.rename(columns = {'location_y':'location'}, inplace = True)

    #We keep just with cols of interest
    df = df[['university', 'career', 'inscription_date', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
    
    #Dataset path
    df_path = air_root_folder + '/datasets/' + file_name + '.csv'
    #Dataset folder creation
    os.makedirs(os.path.dirname(df_path), exist_ok=True)
    
    #Log
    logger.info('Saving file')


    #Save df final version in /dataset as a csv file
    df.to_csv(df_path)


#Loading to S3
def load_to_S3():
    '''
    This function loads the file in a S3 bucket
    '''
    logger.info('Loading started')

    # Instantiate the S3 Hook
    s3_hook = S3Hook(s3_id)

    s3_hook.load_file(
        filename= air_root_folder + '/datasets/' + file_name + '.csv',
        key= file_name + ".csv",
        bucket_name= s3_bucket,
        replace=True
    )
    #Log
    logger.info('The file was succesfully load into s3')
    


#Instantiate DAG
with DAG('F_uni_de_moron',
         description = ' dag for Universidad de Moron',
         start_date=datetime(2022, 8, 23),
         schedule_interval='@hourly',
         default_args = default_args,
         catchup=False
        ) as dag:

        
        #First task: Extraction. (PostgresOperator could be used)
        opr_sql_query = PythonOperator(    
            task_id='sql_query',
            python_callable = sql_query
        )
        
        #Second Task: Pandas Processing and Transformation.
        opr_pandas_process = PythonOperator(
            task_id='pandas_process',
            python_callable = pandas_process
        )

        # Third Task: loading. (LocalFilesystemToS3Operator could be used)
        opr_load_to_S3 = PythonOperator(
            task_id='load_to_S3',
            python_callable = load_to_S3
        )



        opr_sql_query >> opr_pandas_process >> opr_load_to_S3