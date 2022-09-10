""" This file shows the top 10 tags with maximum accepted answers.
"""
# Import libraries
from datetime import datetime
from pathlib import Path
from typing import Counter
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

def get_tags_and_answers(data):
    try:
        tags = data.attrib['Tags']
    except:
        return
    tags = re.findall('<(.+?)>', tags)
    try:
        accepted_answers = int(data.attrib['AnswerCount'])
    except:
        accepted_answers = 0
    return tags, accepted_answers

def separate_tags_and_answers(data):
    return dict([[tag, data[1]] for tag in data[0]])

def reduce_tags_and_answers(data1, data2):
    for key in data2.keys():
        if key in data1.keys():
            data1[key].update({key: data1[key] + data2[key]})
        else:
            data1.update(data2.items())
    return data1

def mapper(data):
    mapped_tags_and_answers = list(map(get_tags_and_answers, data))
    mapped_tags_and_answers = list(filter(None, mapped_tags_and_answers))
    answers_counted_per_tag = list(map(separate_tags_and_answers, mapped_tags_and_answers))
    try:
        reduced = reduce(reduce_tags_and_answers, answers_counted_per_tag)
    except:
        return
    return reduced

def calculate_top10(data):
    return data[0], data[1]

if __name__ == '__main__':
    
    # Get root from xml file and chunkify it
    tree = ET.parse(XML_FILE)
    root = tree.getroot()
    data_chunks = chunckify(root, 50)
    mapped = list(map(mapper, data_chunks))
    mapped = list(filter(None, mapped))
    print(mapped)