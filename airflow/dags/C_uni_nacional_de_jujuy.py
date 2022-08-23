# Import timedelta to establish the schedule
from datetime import timedelta
#Import airflow modules 
from airflow import DAG
from airflow.utils.dates import days_ago
# This module is used for dummy Operatos
from airflow.operators.dummy import DummyOperator

#Define DAG 
with DAG(
    dag_id='C_uni_nacional_de_jujuy',
    description='Dag with no operator related to Universidad Nacional de Jujuy',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['C_uni_nacional_de_jujuy'],

)  as dag_C_uni_de_palermo:
# Set dummy operator    
    empty_dag_C_uni_de_palermo= DummyOperator(task_id='C_uni_nacional_de_jujuy')

