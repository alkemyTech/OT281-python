""" This file shows the top 10 tags with maximum accepted answers."""

# Import libraries
from operator import itemgetter
from pathlib import Path
import os
import pandas as pd
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
CSV_FILE = os.path.join(BASE_DIR, 'output', 'A_top10_tags_accepted_answers.csv')

# Root row keys
"""
['Id', 'PostTypeId', 'AcceptedAnswerId', 'CreationDate', 'Score', 'ViewCount', 'Body', 'OwnerUserId',
'LastEditorUserId', 'LastEditorDisplayName', 'LastEditDate', 'LastActivityDate', 'Title', 'Tags',
'AnswerCount', 'CommentCount', 'FavoriteCount']
"""

# Helper functions to map and reduce
def get_tags_and_answers(data):
    """
    This funtion takes the data from the root xml post file
    and returns the answercount per tag.
    """
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
    """ This function returns a dictionary with tags as key and answer count as value."""
    return dict([[tag, data[1]] for tag in data[0]])

def reduce_tags_and_answers(data1, data2):
    """
    This function compares two dictionaries. If the keys are equal, it sums the values.
    Else, it updates the key-value pair.
    """
    for key, value in data2.items():
        if key in data1.keys():
            data1[key] += value
        else:
            data1.update({key: value})
    return data1

def mapper(data):
    """ This function maps the tag and answer count and reduces it into one dictionary."""
    mapped_tags_and_answers = list(map(get_tags_and_answers, data))
    mapped_tags_and_answers = list(filter(None, mapped_tags_and_answers))
    answers_counted_per_tag = list(map(separate_tags_and_answers, mapped_tags_and_answers))
    try:
        reduced = reduce(reduce_tags_and_answers, answers_counted_per_tag)
    except:
        return
    return reduced

if __name__ == '__main__':
    
    # Get root from xml file and chunkify it
    tree = ET.parse(XML_FILE)
    root = tree.getroot()
    data_chunks = chunckify(root, 50)

    # Apply MapReduce functionality
    mapped = list(map(mapper, data_chunks))
    mapped = list(filter(None, mapped))
    reduced = reduce(reduce_tags_and_answers, mapped)
    
    # Sort the values descending order and make the top 10
    sorted_values = sorted(reduced.items(), key=itemgetter(1), reverse=True)
    top10 = sorted_values[:10]
    
    # Create .csv file and push it into output folder
    df = pd.DataFrame(top10, columns=['Tag', 'AnswerCount'])
    df.to_csv(CSV_FILE)