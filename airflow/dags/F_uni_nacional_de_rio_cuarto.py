'''
COMO: Analista de datos
QUIERO: Configurar los retries con la conexión al a base de datos
PARA: poder intentar nuevamente si la base de datos me produce un error

'''

#Imports
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta


#Default settings applied to all tasks
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(minutes=5)
}

# SQL extraction
def sql_query():
    pass

#Pandas processing and Transformation
def pandas_process():
    pass

#Loading to S3
def load_to_S3():
    pass


#Instantiate DAG
with DAG('F_uni_nacional_de_rio_cuarto',
         description = ' dag for Universidad de nacional de Rio Cuarto' 
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