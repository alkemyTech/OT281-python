"""DAG Universidad Nacional de la Pampa considering:
1- Future Operators
2- SQL query
3- Future Data preparation with pandas
4- Upload data to AWS S3 
5- Execution time: Every hour, every day
"""

from datetime import timedelta, datetime
import logging
import os
import pandas as pd

from airflow import DAG

from airflow.operators.python import PythonOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
#from airflow.hooks.S3_hook import S3Hook


#Logs config
logging.basicConfig(datefmt= '%Y-%m-%d',
                    format='%(asctime)s - %(name)s - %(message)s',
                    level=logging.DEBUG) 


default_args = {
    'email':[''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}

#Get data and read it with SQL query
def sql_query():

    #Get the Sql query from "include" folder
    sql_query = os.path.join(os.path.dirname(os.path.normpath(__file__)).rstrip('/dags'), 'include/'+"E_uni_nacional_de_la_pampa.sql")
    sql_file = open(sql_query)
    sql_file = sql_file.read()
    sql_file = "COPY (" + sql_file.replace(";", "") + ") TO STDOUT WITH CSV HEADER"

    #Hook Creation and Connection with Postgress
    hook = PostgresHook(
        postgres_conn_id='db_universidades_postgres',
        schema='training'
    )
    logging.info("SQL succesfully extract")    

    #Export raw data in CSV format to the "files" directory
    filename_csv="E_uni_nacional_de_la_pampa.csv"
    filename = os.path.join(os.path.dirname(os.path.normpath(__file__)).rstrip('/dags'), 'files/'+filename_csv)

    # Return the data from the hook using the sql querys and export it in csv
    return hook.copy_expert(sql_file, filename)

#Transform data with pandas
def transform_data():
        #Get the raw data
    file_csv="E_uni_nacional_de_la_pampa.csv"
    filename = os.path.join(os.path.dirname(os.path.normpath(__file__)).rstrip('/dags'), 'files/'+file_csv)
    df_original = pd.read_csv(filename, encoding='UTF-8')

        #Convert strings to lower case and normalize
    df_original['university'] = df_original['university'].apply(str).apply(lambda x: x.lower().strip())
    df_original['career'] = df_original['career'].apply(str).apply(lambda x: x.lower().strip())
    df_original['first_name'] = df_original['first_name'].apply(str).apply(lambda x: x.lower().replace('mrs. ','').replace('mr. ','').strip())
    df_original['postal_code'] = df_original['postal_code'].apply(str).apply(lambda x: x.lower().strip())
    df_original['email'] = df_original['email'].apply(str).apply(lambda x: x.lower().strip())

        #Gender to Male and Female
    df_original['gender'] = df_original['gender'].apply(str).apply(lambda x: x.lower().strip().replace('m','male')).apply(lambda x: x.replace('f','female'))
        
        #First_name and Last_name
    df_original['last_name'] = df_original['first_name'].apply(lambda x: x.split()[1])
    df_original['first_name'] = df_original['first_name'].apply(lambda x: x.split()[0])


        #Inscription_date to %Y-%m-%d format as string format
    df_original["inscription_date"] = pd.to_datetime(df_original["inscription_date"], format='%d/%m/%Y')
    df_original["inscription_date"] = pd.to_datetime(df_original["inscription_date"].astype(str), format='%Y-%m-%d')
    df_original["inscription_date"] = df_original["inscription_date"].astype(str)


        #Calculate int Age from birth_date
    df_original["birth_date"] = pd.to_datetime(df_original["birth_date"], format='%d/%m/%Y')
    df_original["age"] = df_original['birth_date'].apply(lambda x: (datetime.today() - x).days//365)
        
        #Merge df_original with df_locations by postal_code
    df_locations=pd.read_csv(os.path.join(os.path.dirname(os.path.normpath(__file__)).rstrip('/dags'), 'assets/'+"codigos_postales.csv"), encoding='UTF-8')
    df_locations["location"] = df_locations["localidad"].astype(str)
    df_locations["postal_code"] = df_locations["codigo_postal"].astype(str)
    df_locations = df_locations.drop(columns=["localidad", "codigo_postal"], axis=1)

    df_merged = pd.merge(left= df_original, right=df_locations, on='postal_code')

    df_original['location'] = df_merged['location_y'].apply(str).apply(lambda x: x.lower().strip())
    df_original = df_original.drop("birth_date", axis=1)

        #Export df_original to CSV
    DIR = os.path.join(os.path.dirname(os.path.normpath(__file__)).rstrip('/dags'),'datasets/')
    df_original.to_csv(DIR+'E_uni_nacional_de_la_pampa.csv', index=False, encoding='UTF-8')

#Upload data to AWS S3
def upload_to_s3():
    pass


with DAG(
    dag_id='E_uni_nacional_de_la_pampa',
    default_args=default_args,
    schedule_interval='@hourly',
    start_date=datetime(2022,8,28),
) as dag:

    sql_query = PythonOperator(
        task_id='query_sql',
        python_callable=sql_query,
    )

    transform_data = PythonOperator(
        task_id='transform_data',
        python_callable=transform_data
    )

    upload_to_s3 = PythonOperator(
        task_id='upload_to_s3',
        python_callable=upload_to_s3
    )

    sql_query >> transform_data >> upload_to_s3


'''
Resources:
a. DAG to Upload to S3 [https://betterdatascience.com/apache-airflow-amazon-s3/]
b. Logs configuration[https://docs.python.org/3/howto/logging.html]
c. Connection to Postgress[https://registry.astronomer.io/providers/postgres/modules/postgreshook]
'''