'''
COMO: Analista de datos
QUIERO: Implementar SQL Operator
PARA: tomar los datos de las bases de datos en el DAG

'''

#Airflow Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook

# Other imports
from datetime import datetime, timedelta
import logging
import os
from pathlib import Path



#Connection id for Postgres Database
POSTGRES_CONN_ID = "db_universidades_postgres"

# Filename
file_name = 'F_uni_nacional_de_rio_cuarto'


# Logging Set Up
logging.basicConfig(level=logging.INFO, datefmt= '%Y-%m-%d',
                    format='%(asctime)s - %(name)s - %(message)s')


logger = logging.getLogger("F_uni_nacional_de_rio_cuarto")


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
    sql_path = os.path.join(os.path.dirname(os.path.normpath(__file__)).rstrip('/dags'), 'include/'+ file_name+ '.sql')
    

    #csv path
    csv_path = os.path.join(os.path.dirname(os.path.normpath(__file__)).rstrip('/dags'), 'files/' + file_name + '.csv')

    #Open and read sql file
    with open(sql_path, "r") as file:
        sql = file.read()
        sql = "COPY (" + sql.replace(";", "") + ") TO STDOUT WITH CSV HEADER" 
    
    #Run query and storage it in csv file
    pg_hook.copy_expert(sql, csv_path)

#Pandas processing and Transformation
def pandas_process():
    pass

#Loading to S3
def load_to_S3():
    pass


#Instantiate DAG
with DAG('F_uni_nacional_de_rio_cuarto',
         description = ' dag for Universidad de nacional de Rio Cuarto',
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