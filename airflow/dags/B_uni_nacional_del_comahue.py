"""
DAG workflow:

- Extract data from base
- Transform data using pandas
- Load data into s3 bucket
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from sqlalchemy import create_engine


# SQL query
def sql_queries():
    
    # Parameters
    USER = ""
    PASSWORD = ""
    HOST = ""
    DB_NAME = ""
    url = f"postgresql+psycopg2://{USER}:{PASSWORD}@{HOST}/{DB_NAME}"

    # Create engine
    engine = create_engine(url)


# Pandas data wrangling
def pandas_data_wrangling():
    pass


# Load data into s3
def load_data_s3():
    pass


with DAG(
    "B_uni_nacional_del_comahue",
    description = "DAG for Uni Nacional del comahue",
    default_args = {},
    start_date = datetime(2022, 8, 23),
    schedule_interval = timedelta(hours = 1),
    catchup = False
) as dag:
    
    task_sql_queries = PythonOperator(
        task_id="sql_queries", 
        python_callable = sql_queries,
        retries = 5,
        retry_delay = timedelta(senconds = 5)
    )

    task_pandas_data_wrangling = PythonOperator(
        task_id="pandas_data_wrangling",
        python_callable = pandas_data_wrangling
    )

    task_load_data = PythonOperator(
        task_id="load_data_s3",
        python_callable = load_data_s3
    )

    task_sql_queries >> task_pandas_data_wrangling >> task_load_data
    