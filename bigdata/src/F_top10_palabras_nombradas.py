'''
COMO: Analista de datos
QUIERO: Utilizar MapReduce
PARA: Analizar los datos de StackOverflow

Top 10 palabras mas nombradas en los post.
'''

#Imports
from functools import reduce
import xml.etree.ElementTree as ET
import os
import re
from collections import Counter
import pandas as pd
import logging 
import logging.config

#Define crucial variables
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
xml_path = os.path.join(project_folder, 'datasets', 'posts.xml')
file_name = 'F_top10_palabras_nombradas'
csv_output_path = os.path.join(project_folder, 'output', file_name + '.csv')

#logging config
logging.config.fileConfig(
    f'{project_folder}/logs/F_logger.cfg'
)

logger = logging.getLogger("root")

#FUNCTIONS DEFINITIONS

def chunckify(iterable,len_of_chunk):
    for i in range(0,len(iterable), len_of_chunk):
        yield iterable[i:i + len_of_chunk]

def extract_words(data):
    try:
        body = data.attrib['Body']
        body = re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))', body)
        counter_words = Counter(body)
        return counter_words
    except Exception as e:
        logging.error(str(e))

def reducer(data_1, data_2):
    data_1 = data_1 + data_2
    return data_1

def mapper(data):
    try:
        palabras_mapeadas= list(map(extract_words, data))
        palabras_mapeadas = list(filter(None, palabras_mapeadas))
        reducido = reduce(reducer, palabras_mapeadas)
        return reducido
    except Exception as e:
        logging.error(str(e))


if __name__ == "__main__":
    logger.info('Reading and opening xml file')
    tree = ET.parse(xml_path)
    root = tree.getroot()
    logger.info('xml file read and opended successfully')

    #Divide data in chuncks
    logger.info('Dividing data in 50 chuncks')
    data_chunks = chunckify(root, 50)
    logger.info('Data division success')


    mapped = list(map(mapper, data_chunks))
    
    reduced = reduce(reducer, mapped)

    #Get top 10 words
    top10_palabras = reduced.most_common(10)
    logger.info('Top 10 least common dates gotten')

    #Transform into a DF
    logger.info('Creating and saving DATAFRAME with TOP 10 least common dates')

    df = pd.DataFrame(data = top10_palabras, columns = ['Palabras', 'Cantidad']  )
    logger.info('DF successfully created')

    
    #Save it as a .csv in /output folder
    df.to_csv(csv_output_path)
    logger.info('CSV file was saved in /output folder correctly')
