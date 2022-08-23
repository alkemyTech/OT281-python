"""
1 -Configurar un DAG, sin consultas, ni procesamiento. Para Hacer un ETL para 2 universidades distintas
2- Configurar el retry para las tareas del DAG
3- Configurar los log
"""

from datetime import timedelta, datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import logging


# configuracion logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s-%(levelname)s-%(message)s', datefmt='%Y/%m/%d')
logger = logging.getLogger(__name__)



# dag configuracion
default_args={
    'owner': 'airflow-OT281-28',
    'retries': 5, # Esto viene de la definicion de SQL
    'retry_delay': timedelta(seconds=5) #fer determino 5 seg
}

#Inicializamos DAG
with DAG('Consulta universidad UDC', 
          default_args=default_args,
          description='Consulta a la Universidad del Cine',
          scudule_inverval=timedelta(hours=1), ## DAG se debe ejecutar cada 1 hora, todos los días
          star_date=datetime(2022,8,19), ## HAY QUE DEFINIR LA FECHA
         ) as dag:

        #0 - init Log
        log_init = PythonOperator()

        #1 - Data from postgres database
        udc_select_query=PythonOperator(
        )
           
        #2 - Transforms Data
        udc_pandas_transform=PythonOperator(
        )

        #3 - Load Data
        udc_load_data=PythonOperator(
        )

        #4 - The execution order of the DAG
        udc_select_query >> udc_pandas_transform >> udc_load_data