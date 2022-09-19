'''
COMO: Analista de datos
QUIERO: Utilizar MapReduce
PARA: Analizar los datos de StackOverflow

Del ranking de los primeros 100-200 por score, tomar el tiempo de respuesta promedio e informar un único valor.
'''

#Imports
from functools import reduce
import xml.etree.ElementTree as ET
import os
import logging
import logging.config
from datetime import datetime
from datetime import timedelta
import pandas as pd

#Define crucial variables
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
xml_path = os.path.join(project_folder, 'datasets', 'posts.xml')
file_name = 'F_tiempo_rta_promedio'
csv_output_path = os.path.join(project_folder, 'output', file_name + '.csv')


#logging config
logging.config.fileConfig(
    f'{project_folder}/logs/F_logger.cfg'
)

logger = logging.getLogger("root")

#Functions definitions
def chunckify(iterable,len_of_chunk):
    for i in range(0,len(iterable), len_of_chunk):
        yield iterable[i:i + len_of_chunk]


def get_score(data):
    try:
        post_type_id = data.attrib['PostTypeId']
        if post_type_id == '1':
            id = data.attrib['Id']
            score = int(data.attrib['Score'])
            creation_date = data.attrib['CreationDate']
            creation_date = datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f')
            return id, score, creation_date
    except Exception as e:
        logging.error(str(e))

def get_parent_date(data):
    try:
        post_type_id = data.attrib['PostTypeId']
        if post_type_id == '2':
            parent_id = data.attrib['ParentId']
            creation_date = data.attrib['CreationDate']
            creation_date = datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f')
            return parent_id, creation_date
    except Exception as e:
        logging.error(str(e))

def mapper(data):
    try:
        score = list(map(get_score, data))
        score = list(filter(None, score))
        return score
    except Exception as e:
        logging.error(str(e))

def mapper_2(data):
    try:
        parent_date = list(map(get_parent_date, data))
        parent_date = list(filter(None, parent_date))
        return parent_date

    except Exception as e:
        logging.error(str(e))

lista_2 = []
clave = '0'
def mapper_3(data):
    try:
        global clave
        for i in data:
            if isinstance(i, str) and clave != i:
                clave = i
                lista_2.append(data)   
    except Exception as e:
        logging.error(str(e))

def reducer(data_1, data_2):
    data_1 = data_1 + data_2
    return data_1


diferencia = timedelta(seconds=0)
def mapper_4(data):
    try:
        for i in order_score:
            for a in data:
                if i[0] == a:
                    global diferencia
                    diferencia += data[1] - i[2]
    except Exception as e:
        logging.error(str(e))
            

def temp_rta_promedio():

    try:
        #Opening xml file
        #preguntas
        logger.info('Working with questions...')
        logger.info('Reading and opening xml file')
        tree = ET.parse(xml_path)
        root = tree.getroot()

        #Divide data in chuncks
        logger.info('Dividing data in 50 chuncks')
        data_chuncks = chunckify(root, 50)
        logger.info('Data division success')

        #Mapping
        mapped = list(map(mapper, data_chuncks))
        mapped = reduce(reducer, mapped)
        global order_score
        order_score = sorted(mapped, key=lambda x: x[1],reverse=True)[100:200]

        logger.info('working with questions finished.')
        
        #rtas
        logger.info('Working with answers...')
        logger.info('Reading and opening xml file')
        tree = ET.parse(xml_path)
        root = tree.getroot()

        #Divide data in chuncks
        logger.info('Dividing data in 50 chuncks')
        data_chuncks = chunckify(root, 50)
        logger.info('Data division success')

        #Mapping
        mapped_2 = list(map(mapper_2, data_chuncks))
        mapped_2 = sorted(reduce(reducer, mapped_2),key=lambda x :x[0])
        list(map(mapper_3, mapped_2))

        
        # genera diferencias, devuelve lista: demora
        logger.info('Getting difference (average response time)')
        list(map(mapper_4, lista_2))
        
        return diferencia/100

    
    except Exception as e:
        logging.error(str(e))

if __name__=="__main__":
    '''
    Run temp_rta_promedio
    '''
    logger.info('Executing tem_rta_promedio function')
    tiempo_rta_promedio = temp_rta_promedio()
    logger.info('Succesfully executed program')

    # #Transform into a DF
    logger.info('Transforming into a DF')
    df = pd.DataFrame({'tiempo_rta_promedio': tiempo_rta_promedio}, index = [0])
    logger.info('DF successfully created')

    #Save it as a .csv in /output folder
    df.to_csv(csv_output_path)
    logger.info('CSV file was saved in /output folder correctly')