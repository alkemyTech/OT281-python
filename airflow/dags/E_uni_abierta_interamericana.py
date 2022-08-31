"""DAG Universidad Abierta Interamericana considering:
1- Future Operators
2- SQL query
3- Future Data preparation with pandas
4- Upload data to AWS S3 
5- Execution time: Every hour, every day
"""

from datetime import timedelta, datetime

from airflow import DAG

from airflow.operators.python import PythonOperator
#from airflow.hooks.S3_hook import S3Hook

default_args = {
    'email':[''],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 5,
    'retry_delay': timedelta(seconds=5)
}

#Get data and read it with SQL query
def sql_query():
    pass

#Transform data with pandas
def transform_data():
    pass

#Upload data to AWS S3
def upload_to_s3(filename: str, key: str, bucket_name: str):
    pass


with DAG(
    dag_id='E_uni_abierta_interamericana',
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
'''

