




# ==== START LIBRARIES IMPORT ====

# To manage folders and paths
import os
# For Pandas Dataframe
import pandas as pd
# For Time related functions
from datetime import datetime
# For Airflow 
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.providers.postgres.operators.postgres import PostgresOperator
#import logging module
import logging


# ==== END LIBRARIES IMPORT ====





# ==== START FUNCTIONS DECLARE AND SETTINGS ====


#Create logger
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)
# Create console handler and set level to debug
ch = logging.StreamHandler()
ch.setLevel(logging.DEBUG)
#Create formatter
formatter= logging.Formatter( '%(asctime)s - %(name) - %(message)s' , datefmt='%Y-%m-%d' )
# Add formatter to ch
ch.setFormatter(formatter)



# ======= START FUNCTION TO TRANSFORM DATAFRAME ====

import re

from datetime import date

# university: str minúsculas, sin espacios extras, ni guiones
# career: str minúsculas, sin espacios extras, ni guiones
# first_name: str minúscula y sin espacios, ni guiones
# last_name: str minúscula y sin espacios, ni guiones
# location: str minúscula sin espacios extras, ni guiones
# email: str minúsculas, sin espacios extras, ni guiones
# inscription_date: str %Y-%m-%d format
# gender: str choice(male, female)
# age: int
# postal_code : str

# Clean extra spaces , remove "_" and lower caps
# # Input: pd str type  Output: pd str type  
def remove_chars(column):
    try:
        return column.str.replace('_'," ").replace('  '," ").str[1:].str.lower()
    except:
        # if the value its not an alphabetic str rise exception
        if pd.isna(column).any():
        #  if there is any NA value
            print(f" The field {column.name} its NA ")
        else:
        #  If there is no NA value and its not an alphabetic str it its an unhandled error    
            raise print(f" There was an error in the code for {column.name} ")

# Transform date format This function converts a scalar, array-like, Series or DataFrame/dict-like to a pandas datetime object.             
def date_format(original_date):
    try:
        # Due each sql query returns different format of time its implemented a try condition
        return pd.to_datetime(original_date,format="%d/%b/%y" )
    except ValueError:
        return pd.to_datetime(original_date,format="%Y/%m/%d" )
# Re format data time pandas
# Input datatime output pd str type
def inscription_date_format(inscription_formated):
    return date_format(inscription_formated).dt.strftime('%Y-%m-%d').astype("string")

# Replace chars f and m for female and male
def gender_choice(gender):
        return gender.replace("f","female").replace("m","male")


# fix century issue with the date
def fix_birth_date(pd_birth_dates):
    # Get today date and format it
    today= date.today().strftime("%Y-%m-%d")
    today=pd.to_datetime(today,format="%Y-%m-%d" )
    # Format birth dates field
    date_pd = date_format(pd_birth_dates)
    date_pd=date_format(date_pd)
    # for the wrong century birth year (ej 2045) use dateoffset to substract 100 years
    date_pd[date_pd > today] = date_pd[date_pd > today] - pd.DateOffset(years=100)

    return date_pd
# get age from the diff between the years, months and days    
def calculate_age(born):
# - For next version "today" should be defined outside of the function and shared between them     
    today = date.today()
    return today.year - born.year - ((today.month, today.day) < (born.month, born.day))



# Function for dictionary , takes csv and transform in a dict in lower case
def cp_pd_dic(codigos_postales_csv):
    # Add path to the file with the file name in str format
    filename = os.path.join(os.path.dirname(__file__), '../assets/'+codigos_postales_csv)
    df_to_dic = pd.read_csv(filename)

    return dict(zip(df_to_dic['localidad'].str.lower(), df_to_dic['codigo_postal']))

# ====== FUNCTION FOR POSTAL CODE ====
# Takes two pandas series (location and postal_code) 
# if postal code is NA gets its value using the location and the codigos_postales.csv file
def postal_code_function(cp_pd,location_pd):
# If the field postal_code its all NA then use codigos_postales.csv to get the data
    if pd.isna(cp_pd).all():
        #CODE TO SEARCH IN CSV
        
        # get dict obj from codigos_postales file
        cp_dic_pd = cp_pd_dic("codigos_postales.csv")
        # Return Dataframe using map function between location series and the dic for that values
        return pd.DataFrame(location_pd.map(cp_dic_pd).astype("int"))
        # If there is no NA values in postal_code series then return its values as int
    elif not pd.isna(cp_pd).any():
        return cp_pd.astype("int")
    else:
        # In case that there are some NA values (but not all) in the pd series then the data should be preprocesed and cleaned
        raise print("CRITICAL ERROR DATA IS NOT CLEAN - NA VALUES")

# ===== FUNCTION FOR LOCATION  All the written docs and comments  apply  ====
def location_pd_dic(codigos_postales_csv):
    # Add path to the file with the file name in str format
    filename = os.path.join(os.path.dirname(__file__), '../assets/'+codigos_postales_csv)
    df_to_dic = pd.read_csv(filename)

    return dict(zip(df_to_dic['codigo_postal'], df_to_dic['localidad'].str.lower()))


def location_function(cp_pd,location_pd):
    if pd.isna(location_pd).all():
        #CODE TO SEARCH IN CSV
        
        location_dic_pd = location_pd_dic("codigos_postales.csv")
        return pd.DataFrame(cp_pd.map(location_dic_pd).astype("str"))

    elif not pd.isna(location_pd).any():
        
        return location_pd.astype("str")
    else:
        raise print("CRITICAL ERROR DATA IS NOT CLEAN - NA VALUES")
# Create a new pandas dataframe with all the regenerated pandas series     
def create_export_dataframe(original_dataframe):

    university_ok=remove_chars(original_dataframe['university']).to_frame(name='university')
    career_ok=remove_chars(original_dataframe['careers']).to_frame(name='careers')
    last_name_ok=remove_chars(original_dataframe['last_name']).to_frame(name='last_name')
    email_ok=remove_chars(original_dataframe['email']).to_frame(name='email')
    inscription_date_ok=inscription_date_format(original_dataframe['inscription_date']).to_frame(name='inscription_date')
    gender_ok=gender_choice(original_dataframe['gender']).to_frame(name='gender')
    age_ok=(fix_birth_date(original_dataframe['birth_date']).apply(calculate_age)).astype('int').to_frame(name='birth_date')
    postal_code_ok=postal_code_function(original_dataframe['postal_code'],original_dataframe['location'])
    location_ok=location_function(original_dataframe['postal_code'],original_dataframe['location'])

    return  pd.concat([university_ok,career_ok,last_name_ok,location_ok,email_ok,inscription_date_ok,gender_ok,age_ok,postal_code_ok,location_ok], axis=1)

#====== END FUNCTIONS TO TRANSFORM DATAFRAME =====






# Function to open csv file and transform to Dataframe
def open_csv_to_pd():
    
    # SET FILE NAME TO LO LOAD CSV WITH RAW DATA
    file_name_csv="C_uni_de_palermo.csv"
    # Add path to the file with the file name in str format
    filename = os.path.join(os.path.dirname(__file__), '../files/'+file_name_csv)

    # SET FILE NAME TO LO SAVE CSV WITH CLEAN DATA
    file_name_csv_clean="C_uni_de_palermo.csv"
    # Add path to the file with the file name in str format
    filename_clean = os.path.join(os.path.dirname(__file__), '../datasets/'+file_name_csv_clean)


    # Open csv and transform to Dataframe
    C_uni_de_palermo_pd = pd.read_csv(filename)
    # Edit and transform data in DF and output a new DF
    new_df = create_export_dataframe(C_uni_de_palermo_pd)

    # Export to CSV
    new_df.to_csv(filename_clean)



    return True


#Function to open the sql file and save it in a variable
def sql_reader(file):
    # file: its the sql name with its location in string format
    sql_file = open(file)
    return sql_file.read()


#Function to fetch data from Postgres 
def get_postgress_data():
    #file contains the path to the sql query for this university 
    file_name_sql="C_uni_de_palermo.sql"
    # Add path to the file with the file name in str format
    file = os.path.join(os.path.dirname(__file__), '../include/'+file_name_sql)
    # Call sql_reader function to get the query in str format
    sql_query = sql_reader(file)
    
    logging.debug(f'The query is {sql_query}')
    
    # CREATE PostgressHook instance
    # SET CONN ID WITH THE AIRFLOW CONN ID and DDBB NAME 
    pg_hook = PostgresHook(
        postgres_conn_id='db_universidades_postgres',
        schema='training'
    )
    
    #CONNECT OBJECT TO THE DDBB
    pg_conn = pg_hook.get_conn()
    
    # SET FILE NAME FOR CSV WITH RAW DATA
    file_name_csv="C_uni_de_palermo.csv"
    # Add path to the file with the file name in str format
    filename = os.path.join(os.path.dirname(__file__), '../files/'+file_name_csv)
    
    
    # FORMAT THE QUERY TO MAKE IT WORK WITH copy_expert
    new_sql_query= "COPY ( "+sql_query+" ) TO STDOUT WITH CSV HEADER"    
    # REMOVE " ; " CHAR TO GET IT WORK
    new_sql_query=new_sql_query.replace(";","")
    
    # EXECUTE copy_expert TO DOWNLOAD THE QUERY RAW DATA TO CSV 
    pg_hook.copy_expert(new_sql_query,filename)

    return True

# ==== END FUNCTIONS DECLARE AND SETTINGS ====


# ==== START AIRFLOW SETTINGS ====


# Set args for DAGS
default_args={
    #'owner':'airflow',
    'retries':5,
    'retry_delay':5
}

# Set DAG
with DAG(
    dag_id='C_uni_de_palermo',
    description='DAG for ETL process with Universidad de Palermo',
    schedule_interval='@hourly',
    start_date=datetime(year=2022, month=8, day=22),
    default_args=default_args,
    catchup=False
) as dag:
    logger.debug("C_uni_de_palermo Starts")
    
# Define task to query and donwload data to csv
    task_C_uni_de_palermo_load_query = PythonOperator(
    	task_id='C_uni_de_palermo_load_query',
    	python_callable=get_postgress_data,
    )
# Define task to open csv and transform into DataFrame
    task_C_uni_de_palermo_csv_to_pd = PythonOperator(
        task_id='C_uni_de_palermo_csv_to_pd',
        python_callable=open_csv_to_pd,
    )




# SET AIRFLOW FLOW PROCESS 
    task_C_uni_de_palermo_load_query >> task_C_uni_de_palermo_csv_to_pd

# ==== END AIRFLOW SETTINGS ====
