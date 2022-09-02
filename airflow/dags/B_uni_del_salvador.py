"""
DAG workflow:

- Extract data from base
- Transform data using pandas
- Load data into s3 bucket
"""

import pandas as pd
import logging
from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.hooks.S3_hook import S3Hook
from pathlib import Path
import os

# Set up logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# Format
format = logging.Formatter(
    fmt = "%(asctime)s - %(name)s - %(message)s",
    datefmt = "%Y-%m-%d" 
)

# Handler
stream_handler = logging.StreamHandler()
stream_handler.setFormatter(format)

# Add handler to logger
logger.addHandler(stream_handler)


# SQL query
def sql_queries():

    # Filename  
    filename = Path(
        Path(__file__).absolute().parent.parent, 
        "files/B_uni_del_salvador.csv"
    )

    sql_path = Path(
        Path(__file__).parent.parent, 
        "include/B_uni_del_salvador.sql"
    )
  
    # Read sql statement
    with open(sql_path, "r") as file:
       sql = file.read()
       sql = "COPY (" + sql.replace(";", "") + ") TO STDOUT WITH CSV HEADER"
     
    # Define Hook
    hook = PostgresHook(
        postgres_conn_id = "db_universidades_postgres",
        schema = "training"
    )
    
    return hook.copy_expert(sql, filename)


# Pandas data wrangling
def data_transformation():
    # Parent folder
    parent_path = Path(__file__).parent.parent
    
    # CSV files
    path_university = Path(
        parent_path,
        'files/B_uni_del_salvador.csv'
    )

    path_location = Path(
        parent_path,
        'assets/codigos_postales.csv'
    )

    path_to_save = Path(
        parent_path,
        'datasets/B_uni_del_salvador.csv'
    )

    # Postal code
    locations = pd.read_csv(path_location)    # Read csv
    
    locations = locations.drop_duplicates(    # Drop duplicates
        subset='localidad', 
        keep='first'
    )

    locations['localidad'] = locations['localidad'].apply(str.lower)

    # University
    df = pd.read_csv(path_university, encoding = 'UTF8')
    
    # Columns with string data to transform
    str_columns = [
        'university', 'career',
        'last_name', 'location',
        'email'
    ]

    df[str_columns] = df[str_columns].apply( 
        lambda x: x.str.lower()
        .str.replace('_', ' ')
        .str.replace('-', ' ')
        .str.strip()
    )    # Delete dashes, underscores and upper -> lower

    # First name
    df['first_name'] = df['last_name'].apply(
        lambda x: x.split()[0]
    ).astype('string')

    # Last name
    df['last_name'] = df['last_name'].apply(
        lambda x: x.split()[1]
    ).astype('string')

    # Gender
    df['gender'] = df['gender'].str.replace('M', 'male').replace(
        'F', 'female'
    ).astype('string')

    # Inscription date
    df['inscription_date'] = df['inscription_date'].apply(    # Fix format
        lambda x: 
                x.split('-')[0] + '-' 
                + x.split('-')[1] + '-' 
                + '19' + x.split('-')[2]
            if 
                int(x.split('-')[2]) > 22    # Limit to classify in 19's or 20's
            else 
                x.split('-')[0] + '-' 
                + x.split('-')[1] + '-' 
                + '20' + x.split('-')[2]
                
    )

    df['inscription_date'] = pd.to_datetime(    # String -> datetime 
        df['inscription_date'], 
        format = '%d-%b-%Y'    
    ).dt.date.astype('string')    # Datetime -> String with the asked format

    # Age
    """
    To generate the age, first 
    we have to transform birth_date column
    """
    df['birth_date'] = df['birth_date'].apply(    # Fix format
        lambda x: 
                x.split('-')[0] + '-' 
                + x.split('-')[1] + '-' 
                + '19' + x.split('-')[2]
            if 
                int(x.split('-')[2]) > 5    # Limit to classify in 19's or 20's
            else 
                x.split('-')[0] + '-' 
                + x.split('-')[1] + '-' 
                + '20' + x.split('-')[2]
                
    )

    df['birth_date'] = pd.to_datetime(    # String -> Datetime
        df['birth_date'],
        format = '%d-%b-%Y'
    ).dt.date

    df['age'] = (datetime.now().date() - df['birth_date']).dt.days//365    # Age

    # Obtain location from locations df
    merged_df = df.merge(
        locations, how = 'left', 
        left_on='location', 
        right_on='localidad'
    )

    # Assing localidad column to location
    merged_df['postal_code'] = merged_df['codigo_postal'].astype('string')

    # Set location as string
    merged_df['location'] = merged_df['location'].astype('string')

    # Drop codigo_postal and localidad columns
    merged_df = merged_df.drop(
        ['codigo_postal', 'localidad']
        , axis = 1
    )
    
    # Make sure that the folder where data will be saved exists
    if not os.path.exists(os.path.join(parent_path, 'datasets')):
        os.mkdir(os.path.join(parent_path, 'datasets'))
    
    # Save data
    cols_of_insterest = [
        'university', 'career',
        'inscription_date', 'first_name',
        'last_name', 'gender',
        'age', 'postal_code',
        'location', 'email'
    ]

    merged_df[cols_of_insterest].to_csv(
        path_to_save, encoding='UTF8'
    )
    

# Load data into s3
def load_data_s3():

    # S3 Hook
    hook = S3Hook('universidades_s3')
    
    # File path
    file_path = os.path.join(
        Path(__file__).parent.parent,
        'datasets/B_uni_del_salvador.csv'
    )

    # File
    file_name = 'B_uni_del_salvador.csv'

    # Bucket
    bucket_name = 'cohorte-agosto-38d749a7'

    hook.load_file(
        filename = file_path,
        key = file_name,
        bucket_name = bucket_name

    )    


with DAG(
    "B_uni_del_salvador",
    description = "DAG for Universidad del Salvador",
    default_args = {},
    start_date = datetime(2022, 8, 23),
    schedule_interval = timedelta(hours = 1),
    catchup = False
) as dag:
    
    task_sql_queries = PythonOperator(
        task_id="sql_queries", 
        python_callable = sql_queries,
        retries = 5,
        retry_delay = timedelta(seconds = 5)
    )

    task_pandas_data_wrangling = PythonOperator(
        task_id="pandas_data_wrangling",
        python_callable = data_transformation

    )

    task_load_data = PythonOperator(
        task_id="load_data_s3",
        python_callable = load_data_s3
    )

    task_sql_queries >> task_pandas_data_wrangling >> task_load_data