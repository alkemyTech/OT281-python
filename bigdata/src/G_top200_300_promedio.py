from functools import reduce
from typing import Counter
import xml.etree.ElementTree as ET
import re
from multiprocessing import Pool
import os
import sys
from pathlib import Path
import logging as log
import logging.config
import pandas as pd
import time
from datetime import datetime
from datetime import timedelta

"""
-From the ranking of the top 200-300 by score, it takes the average response time and reports a 
 single value.

 1) chunk the data(n times 50)
 2) get id, score, creation_date and filter them
 3) get 100 top score posts between 200 and 300
 4) get responses, main_id, creation_date(parentid, creation date)
 5) group by parent id
 6) get the difference between dates and sum them
 7) average = difference/100
"""

#dir
DIR = os.path.dirname(os.path.normpath(__file__)).rstrip('/src')

#logging config
log.config.fileConfig(
    f'{DIR}/logs/G_logger.cfg'
)

logger = log.getLogger("G_logger")


#file path
try:
    FILE = f'{DIR}/datasets/posts.xml'
except FileNotFoundError as e:
    logger.error()

#get the data from "posts.xml"
try:
    tree = ET.parse(FILE)
    root = tree.getroot()
except FileNotFoundError as e:
    logger.error("Data not found")

#this function splits the dataset by n parts of 50 rows
def chunckify(iterable,len_of_chunk):
    for i in range(0,len(iterable), len_of_chunk):
        yield iterable[i:i + len_of_chunk]

#get score
def get_score(data):
    post_type_id = data.attrib['PostTypeId']
    if post_type_id == '1':
        id = data.attrib['Id']
        score = int(data.attrib['Score'])
        creation_date = data.attrib['CreationDate']
        creation_date = datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f')
        return id, score, creation_date

#get main date
def get_main_date(data):
    post_type_id = data.attrib['PostTypeId']
    if post_type_id == '2':
        main_id = data.attrib['ParentId']
        creation_date = data.attrib['CreationDate']
        creation_date = datetime.strptime(creation_date, '%Y-%m-%dT%H:%M:%S.%f')
        return main_id, creation_date

#execute get_score function, filter the None data and return post_score(id, score, creation_date)
def mapper(data):
    post_score = list(map(get_score, data))
    post_score = list(filter(None, post_score))
    return post_score

#execute get_main_date function, filter the None data and return main_date(main_id, creation_date)
def main_date_mapper(data):
    main_date = list(map(get_main_date, data))
    main_date = list(filter(None, main_date))
    return main_date

lista_2 = []
clave = '0'

def mapper3(data):
    global clave
    for i in data:
        if isinstance(i, str) and clave != i:
            clave = i
            lista_2.append(data)

#convine data1 and data2
def reducer(data1, data2):
    data1 = data1 + data2
    return data1

difference = timedelta(seconds=0)

#get the difference between dates and sum them
def mapper_4(data):
    for i in order_score:
        for a in data:
            if i[0] == a:
                global difference
                difference += data[1] - i[2]

#main function
def average_time():

    # Questions
    log.info("getting the data")
    data = root
    data_chuncks = chunckify(data, 50)
    #get id, score, creation_date and filter them
    mapped = list(map(mapper, data_chuncks))
    mapped = reduce(reducer, mapped)
    
    #get 100 top score between 200 and 300
    global order_score
    order_score = sorted(mapped, key=lambda x: x[1],reverse=True)[200:300]
        

    # Answers
    data = root
    data_chuncks = chunckify(data, 50)
    mapped_2 = list(map(main_date_mapper, data_chuncks))
    #group by parent id
    mapped_2 = sorted(reduce(reducer, mapped_2),key=lambda x :x[0])
        
    list(map(mapper3, mapped_2))
        
    # get the difference between dates
    log.info("geting difference")
    list(map(mapper_4, lista_2))
    average = difference/100
        
    #returning average
    log.info("returning average")
    return average

time_start = time.time()
log.info("initializing main function")
tiempo_promedio = average_time()
time_end = time.time()
print(tiempo_promedio)

