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
        "files/B_uni_nacional_del_comahue.csv"
    )

    sql_path = Path(
        Path(__file__).parent.parent, 
        "include/B_uni_nacional_del_comahue.sql"
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
        'files/B_uni_nacional_del_comahue.csv'
    )

    path_location = Path(
        parent_path,
        'assets/codigos_postales.csv'
    )

    path_to_save = Path(
        parent_path,
        'datasets/B_uni_nacional_del_comahue.csv'
    )

    # Postal code
    locations = pd.read_csv(path_location)    # Read csv
    locations['localidad'] = locations['localidad'].apply(str.lower)
    
    # University
    df = pd.read_csv(path_university, encoding = 'UTF8')

    # This step depends in the columns that will be used datasets.
    if df['postal_code'].isnull().values.any():
        locations = locations.drop_duplicates(    # Drop duplicates
            subset='localidad', 
            keep='first'
        )
        
    # Columns with string data to transform
    str_columns = [
        'university', 'career',
        'last_name', 'location',
        'email', 'gender'
    ]
    
    """
    If location contains missing values
    we won't parse it to string, because
    we won't be able to identify Nan's
    """
    # Delete location from str_columns if is null
    if df['location'].isnull().values.any():
        str_columns.remove('location')
    
    # Objecto -> String
    df[str_columns] = df[str_columns].astype('string')

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
    df['gender'] = df['gender'].str.replace('m', 'male').replace(
        'f', 'female'
    ).astype('string')

    # Inscription date    
    if len(df['inscription_date'].iloc[0]) == 9:
        # Fix format
        df['inscription_date'] = df['inscription_date'].apply(
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
        # String -> datetime
        df['inscription_date'] = pd.to_datetime(     
            df['inscription_date'], 
            format = '%d-%b-%Y'    
        ).dt.date.astype('string')    # Datetime -> String with the asked format
    else:
        # Object -> String
        df['inscription_date'] = df['inscription_date'].astype('string')

    # Age
    """
    To generate the age, first 
    we have to transform birth_date column
    """
    if len(df['birth_date'].iloc[0]) == 9:
        # Fix format
        df['birth_date'] = df['birth_date'].apply(    
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
        # Object -> Datetime
        df['birth_date'] = pd.to_datetime(
            df['birth_date'],
            format = '%d-%b-%Y'
        ).dt.date
    else:
        # Object -> Datetime
        df['birth_date'] = pd.to_datetime(
            df['birth_date'], 
            format = '%Y-%m-%d'
        ).dt.date

    # Calculate age
    df['age'] = (datetime.now().date() - df['birth_date']).dt.days//365

    # Merge
    if df['postal_code'].isnull().values.any():
        # Obtain postal code from locations df
        merged_df = df.merge(
            locations, how = 'left', 
            left_on='location', 
            right_on='localidad'
        )
    else:
        # Obtain location from locations df
        merged_df = df.merge(
            locations, how = 'left', 
            left_on='postal_code', 
            right_on='codigo_postal'
        )
    
    if merged_df['postal_code'].isnull().values.any():
        # Assing codigo_postal column to postal_code if postal_code is null
        merged_df['postal_code'] = merged_df['codigo_postal'].astype('string')
    else:
        # Assing localidad column to location if location is null
        merged_df['location'] = merged_df['localidad']
    
    # Set location as string type
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
        path_to_save, encoding='UTF8',
        index = False
    )
    

# Load data into s3
def load_data_s3():

    # S3 Hook
    hook = S3Hook('universidades_s3')
    
    # File path
    file_path = os.path.join(
        Path(__file__).parent.parent,
        'datasets/B_uni_nacional_del_comahue.csv'
    )

    # File
    files_name = 'B_uni_nacional_del_comahue.csv'

    # Bucket
    bucket_name = 'cohorte-agosto-38d749a7'

    hook.load_file(
        filename = file_path,
        key = files_name,
        bucket_name = bucket_name,
        replace = True
    )    


with DAG(
    "B_DD_uni_nacional_del_comahue",
    description = "DAG for Uni. Nacional del Comahue",
    default_args = {},
    start_date = datetime(2022, 8, 23),
    schedule_interval = timedelta(hours=1),
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
