#viewCount
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
import operator

"""
-Top 10 most viewed posts

1)  chunk the data(n times 50)
2)  get views, title and id from each post
3)  filter None data
4)  reduce the 3d array
5)  sort by views and get top 10
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


#get the specificts data
def get_posts_views(data):
    '''
    Extract user_id and favorite count post
    '''
    id = data.get('Id')
    title = data.get('Title')
    
    views = int(data.get('ViewCount'))
    if views != None:
        if views != '0' and views != '>':
        
            return [views, id,title]

#function to reduce the 3d array
def flat(data1, data2):
    return data1 + data2


def mapper(data):
    map_words = list(map(get_posts_views, data))
    map_words = list(filter(None, map_words))
    
    return map_words

log.info("getting the data")
data_chunks = chunckify(root, 50)
mapped = list(map(mapper, data_chunks))
mapped = list(reduce(flat,mapped))

#get top 10 posts with most views
log.info("getting the top 10")
mapped = sorted(mapped,key = lambda x:x[0], reverse=True)[:10]

#transfor data to dataFrame
log.info("tranforming data to dataFrame")
df = pd.DataFrame(mapped, columns=["views", "id", "title"])

#create csv file
log.info("creating csv file")
#set file name
file_name = "G_top10_posts_mas_vistos.csv"
#set path
file_name_main = f'{DIR}/output/' + file_name
#df to csv file
df.to_csv(file_name_main)







