# Import libraries
from datetime import timedelta
import dateutil
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

# Age transform data function
def age_transform(bd):
        """
        This function allows transforming current birth_date column format into
        a format that generates not negative ages.
        ##### NOT USED ON UNI DE FLORES #####
        """
        bd = datetime.datetime.strptime(bd, '%d-%b-%y')
        if bd.year >= datetime.date.today().year:
                bd = bd - dateutil.relativedelta.relativedelta(years=100)
        return bd

def transform_data():

        """
        The next function transform the .csv file produced by the SQL extraction
        and uses the postal_code file to produce a .csv file for further load.
        This final file, is going to be saved inside datasets folder.
        """
        df = pd.read_csv(CSV_FROM_QUERY)
        # Give format to original dataframe
        if CSV_ROOT == 'A_uni_de_flores':
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
                
        else:
                df['university'] = df['university'].apply(lambda x: x.replace('_',' ')).apply(lambda x: x.lower()).apply(lambda x: x.strip())
                df['career'] = df['career'].apply(lambda x: x.replace('_',' ')).apply(lambda x: x.lower()).apply(lambda x: x.strip())
                df['inscription_date'] = pd.to_datetime(df['inscription_date']).astype(str)
                df['last_name'] = df['last_name'].apply(lambda x: x.replace('_','')).apply(lambda x: x.lower())
                df['age'] = datetime.date.today() - pd.to_datetime(df['birth_date'].apply(age_transform)).apply(lambda x: x.date())
                df['age'] = df['age'].apply(lambda x: x.days)/365
                df['age'] = df['age'].astype(int)
                df['gender'] = df['gender'].apply(lambda x: x.replace('F','female')).apply(lambda x: x.replace('M','male'))
                df['location'] = df['location'].apply(lambda x: x.replace('_',' ')).apply(lambda x: x.lower()).apply(lambda x: x.strip())
                df['email'] = df['email'].apply(lambda x: x.lower()).apply(lambda x: x.strip())
                # Apply format to location/postal_code table
                location = pd.read_csv(POSTAL_CODE_TABLE)
                location['postal_code'] = location['codigo_postal'].astype(str)
                location['location'] = location['localidad'].astype(str)
                location = location.drop(columns=['codigo_postal', 'localidad'],axis=1)
                location['location'] = location['location'].apply(lambda x: x.lower()).apply(lambda x: x.strip())
                # Merge both dataframes
                df = pd.merge(left=df, right=location, on='location')
                # Return correct dataframe using standardized columns
                df['postal_code'] = df['postal_code_y']
        result = df[['university', 'career', 'inscription_date', 'first_name', 'last_name', 'gender', 'age', 'postal_code', 'location', 'email']]
        return result.to_csv(DATASETS_TARGET)

# Configure default settings to be applied to all tasks
default_args = {
        'retries': 5,
        'retry_delay': timedelta(seconds=5)
} 


with DAG(dag_id="A_DD_uni_de_flores",
        default_args=default_args,
        start_date=datetime.datetime(2022, 8, 28),
        schedule_interval="@hourly",
        catchup=False) as dag:

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
        
        # Third task: load to S3 in Amazon
        local_to_s3 = LocalFilesystemToS3Operator(
                task_id='local_to_s3',
                filename=DATASETS_TARGET,
                dest_key=f'{CSV_ROOT}.csv',
                dest_bucket='cohorte-agosto-38d749a7',
                replace=True,
        )

        extract_sql >> pandas_transform >> local_to_s3