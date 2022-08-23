# Import timedelta to establish the schedule
from datetime import timedelta
#Import airflow modules 
from airflow import DAG
from airflow.utils.dates import days_ago
# This module is used for Empty Operatos
from airflow.operators.dummy import DummyOperator

#Define DAG 
with DAG(
    dag_id='C_uni_de_palermo',
    description='Dag with no operator related to Universidad de Palermo',
    schedule_interval=timedelta(days=1),
    start_date=days_ago(2),
    tags=['C_uni_de_palermo'],

)  as dag_C_uni_de_palermo:
# Set empty operator    
    empty_dag_C_uni_de_palermo= DummyOperator(task_id='C_uni_de_palermo')

