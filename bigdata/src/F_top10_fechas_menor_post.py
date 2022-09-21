'''
COMO: Analista de datos
QUIERO: Utilizar MapReduce
PARA: Analizar los datos de StackOverflow

Top 10 fechas con menor cantidad de post creados
'''

#Imports
from functools import reduce
import xml.etree.ElementTree as ET
import os
from collections import Counter
from operator import add
import pandas as pd
import datetime
import logging
import logging.config

#Define crucial variables
project_folder = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
xml_path = os.path.join(project_folder, 'datasets', 'posts.xml')
file_name = 'F_top10_fechas_menor_post'
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

def get_dates(data):
    try:
        dates = data.attrib['CreationDate']
        dates_formatted = datetime.datetime.strptime(dates, '%Y-%m-%dT%H:%M:%S.%f')
        dates_formatted = dates_formatted.strftime('%Y-%m-%d')
        
        return dates_formatted
    except Exception as e:
        logging.error(str(e))


def date_adder(data1, data2):
    data1.update(data2)
    return data1

def mapper(data):
    try:
        mapped_dates = list(map(get_dates, data))
        counter_fecha = Counter(mapped_dates)

        return counter_fecha
    except Exception as e:
        logging.error(str(e))
    

if __name__ == "__main__":
    
    #Opening xml file
    logger.info('Reading and opening xml file')
    tree = ET.parse(xml_path)
    root = tree.getroot()
    
    
    #Divide data in chuncks
    logger.info('Dividing data in 50 chuncks')
    data_chunks = chunckify(root, 50)
    logger.info('Data division success')

    #Mapping
    mapped = list(map(mapper, data_chunks))

    reduced = reduce(date_adder, mapped)
    
    #Get top 10 least common dates.
    top_10 = reduced.most_common()[-10:-1]
    logger.info('Top 10 least common dates gotten')
    
    # #Transform into a DF
    logger.info('Creating and saving DATAFRAME with TOP 10 least common dates')

    df = pd.DataFrame(data = top_10, columns = ['Fechas', 'Cantidad']  )
    logger.info('DF successfully created')

    #Save it as a .csv in /output folder
    df.to_csv(csv_output_path)
    logger.info('CSV file was saved in /output folder correctly')
