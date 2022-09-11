""" This file answers the word count in a post vs it score.
"""
# Import libraries
from datetime import datetime
from pathlib import Path
from statistics import mean
from typing import Counter
from operator import itemgetter
from itertools import groupby
import os
import re
import xml.etree.ElementTree as ET
from functools import reduce

# Insert chunkify function (in order to check debbuging)
def chunckify(iterable,len_of_chunk):
    for i in range(0,len(iterable), len_of_chunk):
        yield iterable[i:i + len_of_chunk]

# Path variables definition
BASE_DIR = Path(os.path.abspath(__file__)).parent.parent
XML_FOLDER = os.path.join(BASE_DIR, 'datasets')
XML_FILE = os.path.join(XML_FOLDER,'posts.xml')

# Root row keys
"""
['Id', 'PostTypeId', 'AcceptedAnswerId', 'CreationDate', 'Score', 'ViewCount', 'Body', 'OwnerUserId',
'LastEditorUserId', 'LastEditorDisplayName', 'LastEditDate', 'LastActivityDate', 'Title', 'Tags',
'AnswerCount', 'CommentCount', 'FavoriteCount']
"""

# Helper functions to map and reduce

def get_score_and_words(data):
    try:
        score = data.attrib['Score']
    except:
        return
    body = data.attrib['Body']
    body = re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))', body)
    words = len(Counter(body))
    return {score: [words]}

def score_words_average(data1, data2):
    for key, value in data2.items():
        if key in data1.keys():
            data1[key].append(value) # it adds actually a list not an int
        else:
            data1.update({key: value})
    return data1

def mapper(data):
    scored = list(map(get_score_and_words, data))
    scored = list(filter(None, scored))
    # scored = list(map(lambda tuple: list(tuple), scored))
    score_avg = reduce(score_words_average, scored)
    return score_avg

if __name__ == '__main__':
    
    # Get root from xml file and chunkify it
    tree = ET.parse(XML_FILE)
    root = tree.getroot()
    data_chunks = chunckify(root, 50)
    mapped = list(map(mapper, data_chunks))
    mapped = list(filter(None, mapped))
    print(mapped)