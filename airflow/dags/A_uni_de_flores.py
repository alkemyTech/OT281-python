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
from datetime import timedelta
import datetime
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
CSV_ROOT = 'A_uni_de_flores'
BASE_DIR = Path(__file__).parent.parent
INCLUDE_PATH_WITH_FILE = open(os.path.join(BASE_DIR,'include', f'{CSV_ROOT}.sql'), 'r').read().replace(';','')
CSV_FROM_QUERY = os.path.join(BASE_DIR, 'files', f'{CSV_ROOT}.csv')
POSTAL_CODE_TABLE = os.path.join(BASE_DIR, 'assets', 'codigos_postales.csv')
DATASETS_TARGET = os.path.join(BASE_DIR, 'datasets', f'{CSV_ROOT}.csv')


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

        """
        The next function transform the .csv file produced by the SQL extraction
        and uses the postal_code file to produce a .csv file for further load.
        This final file, is going to be saved inside datasets folder.
        """
        df = pd.read_csv(CSV_FROM_QUERY)
        # Give format to original dataframe
        df['university'] = df['university'].apply(lambda x: x.lower()).apply(lambda x: x.strip())
        df['career'] = df['career'].apply(lambda x: x.lower()).apply(lambda x: x.strip())
        df['last_name'] = df['last_name'].apply(lambda x: x.lower()).apply(lambda x: x.replace(' ',''))
        df['gender'] = df['gender'].apply(lambda x: x.replace('F','female')).apply(lambda x: x.replace('M','male'))
        df['age'] = datetime.date.today() - pd.to_datetime(df['birth_date']).apply(lambda x: x.date())
        df['age'] = df['age'].apply(lambda x: x.days)/365
        df['age'] = df['age'].astype(int)
        df['postal_code'] = df['postal_code'].astype(str)
        df['email'] = df['email'].apply(lambda x: x.lower()).apply(lambda x: x.strip())
        # Drop column location
        df = df.drop(columns='location')
        # Build dataframe for location and postal_code
        location = pd.read_csv(POSTAL_CODE_TABLE)
        location['postal_code'] = location['codigo_postal'].astype(str)
        location['location'] = location['localidad'].astype(str)
        location = location.drop(columns=['codigo_postal', 'localidad'],axis=1)
        # Merge both dataframes
        df = pd.merge(left=df, right=location, on='postal_code')
        # Give format to new location column
        df['location'] = df['location'].apply(lambda x: x.lower()).apply(lambda x: x.strip())
        # Create result dataframe
        result = df[['university', 'career', 'inscription_date', 'first_name', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
        return result.to_csv(DATASETS_TARGET)


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
        start_date=datetime.datetime(2022,8,25),

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

