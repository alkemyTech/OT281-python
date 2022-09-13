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

"""
-Top 10 most mentioned words in the post by tag
1) chunk the data(n times 50)
2) from body get words, from tags get tags
3) sum all counters per word
4) with counter method "most_common" return top 10 words per tag
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

#get words and tags from xml file
def get_words_tags(data):
    try:
        tags = data.attrib["Tags"]
    except:
        return None
    tags = re.findall("<(.+?)>", tags)
    body = data.attrib["Body"]
    body = re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))', body)
    counter_words = Counter(body)
    return tags, counter_words

def separate_tags_words(data):
    return dict([[tag, data[1].copy()] for tag in data[0]])

#sum all counters
def reduce_counters(data1, data2):
    for key, value in data2.items():
        if key in data1.keys():
            data1[key].update(data2[key])
        else:
            data1.update({key:value})
    return data1

#get top 10 most common using counter method "most_common"
def calculate_top10(data):
    return data[0], data[1].most_common(10)



#this function map the data and executes 3 functions
def mapper(data):
    map_words = list(map(get_words_tags, data))
    map_words = list(filter(None, map_words))
    words_per_tag = list(map(separate_tags_words, map_words))
    #convinar tags
    try:
        redu = reduce(reduce_counters, words_per_tag)
    except:
        return
        
    return redu

log.info("getting the data")
data_chunks = chunckify(root, 50)
log.info("transforming the data")
mapped = list(map(mapper, data_chunks))
mapped = list(filter(None, mapped))
reduced = reduce(reduce_counters, mapped)
top_10 = dict(map(calculate_top10, reduced.items()))

#transfor data to dataFrame
log.info("tranforming data to dataFrame")
df = pd.DataFrame(top_10.values())
df["tags"] = top_10.keys()


#create csv file
log.info("creating csv file")
#set file name
file_name = "G_top10_tags_words.csv"
#set path
file_name_main = f'{DIR}/output/' + file_name
#df to csv file
df.to_csv(file_name_main)










