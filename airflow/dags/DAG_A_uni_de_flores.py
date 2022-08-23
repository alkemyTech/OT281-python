"""In the present file, DAG is configured for Universidad de Flores in order to set:
1- Future Operators.
2- Process data using Pandas.
3- Upload the data to S3 (Amazon Simple Storage Service).
Execution:
DAG is going to be executed every our, every day.
Sources:
- https://www.astronomer.io/guides/airflow-sql-tutorial/
- Airflow Documentation.
- DAGs: The Definitive Guide (from Astronomer) - Please, check Wiki section from this project.
- https://airflow.apache.org/docs/apache-airflow-providers-amazon/stable/_api/airflow/providers/amazon/aws/transfers/local_to_s3/index.html
- https://docs.python.org/3/library/logging.html
"""

# Import libraries
from datetime import datetime, timedelta
import pandas as pd # to transform the future data from the DB
import logging

# Import DAG class and Airflow Operators
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

# Configure logging and logger object
logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d', level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Configure default settings to be applied to all tasks
default_args = {
        'retries': 5,
        'retry_delay': timedelta(seconds=5)
} 

# Instantiate DAG
with DAG(
        dag_id='DAG_A_uni_de_flores',
        description='DAG created to make the ETL process for Universidad de Flores',
        default_args=default_args,
        start_date=datetime(2022,8,22),
        schedule_interval='@hourly',
        catchup=False
) as dag:
        
        # Zero task: set logging before tasks
        dag_init_log = PythonOperator() # set empty to future stage of the project
        
        # First task: retrieve data from Postgres Database
        query_sql = PythonOperator() # set empty to future stage of the project
        
        # Second task: modify data using pandas library
        pandas_transform = PythonOperator() # set empty to future stage of the project
        
        # Third task: load to S3 in Amazon
        local_to_s3 = LocalFilesystemToS3Operator() # set empty to future stage of the project