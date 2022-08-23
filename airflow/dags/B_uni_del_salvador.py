"""
DAG workflow:

- Extract data from base
- Transform data using pandas
- Load data into s3 bucket
"""

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python import PythonOperator
import logging

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
    pass


# Pandas data wrangling
def pandas_data_wrangling():
    pass


# Load data into s3
def load_data_s3():
    pass


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
        python_callable = sql_queries
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
    