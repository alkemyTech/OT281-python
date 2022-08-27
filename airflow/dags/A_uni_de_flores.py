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
- https://betterdatascience.com/apache-airflow-postgres-database/
"""

# Import libraries
from datetime import datetime, timedelta
import pandas as pd
import logging
from pathlib import Path
import os

# Import DAG class and Airflow Operators
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.amazon.aws.transfers.local_to_s3 import LocalFilesystemToS3Operator

# Configure logging and logger object
logging.basicConfig(format='%(asctime)s - %(name)s - %(message)s', datefmt='%Y-%m-%d', level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Path variables
BASE_DIR = Path(__file__).parent.parent
INCLUDE_PATH_WITH_FILE = open(os.path.join(BASE_DIR,'include', 'A_uni_de_flores.sql'), 'r').read().replace(';','')
CSV_FROM_QUERY = os.path.join(BASE_DIR, 'files', 'A_uni_de_flores.csv')


# Functions to apply within the DAG
def get_sql_data(query_sql):
        """
        The following function connects to the postgres Universidades database
        and build a .csv from de SQL query (query_sql)
        """
        pg_hook = PostgresHook(postgres_conn_id = "db_universidades_postgres", schema='training')
        logger.info('Get SQL data initialized.')
        pg_hook.copy_expert(query_sql, filename=CSV_FROM_QUERY)

def transform_data():
        pass

# Configure default settings to be applied to all tasks
default_args = {
        'retries': 5,
        'retry_delay': timedelta(seconds=5)
} 

# Instantiate DAG
with DAG(
        dag_id='A_uni_de_flores',
        description='DAG created to make the ETL process for Universidad de Flores',
        default_args=default_args,
        start_date=datetime(2022,8,26),
        schedule_interval='@hourly',
        catchup=False,
) as dag:
        
        # First task: retrieve data from Postgres Database
        extract_sql = PythonOperator(
                task_id='extract_sql',
                python_callable=get_sql_data,
                op_kwargs={'query_sql': 'COPY ( '+INCLUDE_PATH_WITH_FILE+' ) TO STDOUT WITH CSV HEADER'})
        
        # Second task: modify data using pandas library
        pandas_transform = PythonOperator(
                task_id='pandas_transform',
                python_callable=transform_data,
        )
        
        """  # Third task: load to S3 in Amazon
        local_to_s3 = LocalFilesystemToS3Operator(
                task_id='local_to_s3',
        ) """

        extract_sql >> pandas_transform
