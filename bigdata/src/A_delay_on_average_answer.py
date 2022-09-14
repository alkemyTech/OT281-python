""" This file answers the delay on average answer for each post."""
# Import libraries
from pathlib import Path
from statistics import mean
import dateutil.parser
import os
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
def get_delay_time(data):
    """ This function takes the data from the root and returns the delay (in days) for each post."""
    try:
        accepted_answer = data.attrib['AcceptedAnswerId']
    except:
        return
    creation_date = data.attrib['CreationDate']
    creation_date = dateutil.parser.parse(creation_date)
    last_activity_date = data.attrib['LastActivityDate']
    last_activity_date = dateutil.parser.parse(last_activity_date)
    delay = last_activity_date - creation_date
    return delay.days

def mapper(data):
    """ This function maps the get_delay_time data and returns the average delay time."""
    delayed = list(map(get_delay_time, data))
    delayed = list(filter(None, delayed))
    try:
        average = mean(delayed)
    except:
        return
    return average

def delayed_average(data):
    """ This function calculates the average on the final dataset."""
    data = sum(data)/len(data)
    return data


if __name__ == '__main__':
    
    # Get root from xml file and chunkify it
    tree = ET.parse(XML_FILE)
    root = tree.getroot()
    data_chunks = chunckify(root, 50)

    # Apply MapReduce functionality
    mapped = list(map(mapper, data_chunks))
    mapped = list(filter(None, mapped))
    averaged = delayed_average(mapped)

    # Print the average delay on answer
    print(f'The average delay on answer each post is: {reduced:.3} days')
