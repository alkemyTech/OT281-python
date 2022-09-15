'''
    Script that applies MapReduce logic in order to process data from a .xml file and store results in a .csv file.
    INPUT: "posts.xml" containing data from Stack Overflow posts.
    OUTPUT: "D_top10_preguntas_mayor_actividad.csv" containing a small dataset with the top 10 questions
    ordered by descending answer count. "Activity time" has been calculated as:
        ActivityTime = LastActivityDate - CreationDate
    It has been used LastActivityDate instead of ClosedDate since a post can still be active even if is already closed.
'''

#Imports
from functools import reduce
import logging
from logging import config
import os
import sys
import xml.etree.ElementTree as ET
import datetime as dt
import pandas as pd

#Get the root folder (project folder)
ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.normpath(__file__)))
#Add root folder to sys.path (needed for relative imports)
sys.path.insert(0, ROOT_FOLDER)
#Import chunkckify from libs folder
from libs.chunckify import chunckify

#Get the xml path
XML_PATH = os.path.join(ROOT_FOLDER, 'datasets', 'posts.xml')
#Get filename (without extension)
FILENAME = os.path.split(__file__)[-1].rstrip('.py')
#Define output csv path
OUTPUT_CSV_PATH = os.path.join(ROOT_FOLDER, 'output', FILENAME + '.csv')

#Define datetime format
DATETIME_FORMAT = '%Y-%m-%dT%H:%M:%S.%f'

#Logger setup from .cfg file
logging.config.fileConfig(
    f'{ROOT_FOLDER}/logs/D_logger.cfg'
)
logger = logging.getLogger("root")

#Methods
def check_is_question(data):
    """
    Check if given data is a question, looking into its attribute 'PostTypeId'.
    
    Args:
        data (row): Data in a XML row format.

    Returns:
        bool: True if data is a question. False if not, or if can't find 'PostTypeId' attribute.
    """
    #Return true if row is a question, false otherwise
    try:
        if data.attrib["PostTypeId"] == '1':
            return True
        return False
    #If row does not have PostTypeId attribute, return false
    except:
        return False

def get_creation_date_and_last_activity(data):
    """
    This method extracts CreationDate, LastActivityDate and Id attributes from given data.
    Returns extracted information in a dictionary, with Id as keys.

    Args:
        data (row): Data in a XML row format.

    Returns:
        dict: Dictionary with Id as keys and tuples (containing the dates) as values
    """
    #Return attributes in a dictionary
    try:
        creation_date = data.attrib["CreationDate"]
        last_activity_date = data.attrib["LastActivityDate"]
        id = data.attrib["Id"]
        return {id : (creation_date, last_activity_date)}
    #If can't find any attribute, return None
    except:
        return

def get_activity_time(data):
    """
    Calculates activity time using Creation and LastActivity dates from given data.

    Args:
        data (dict): A dictionary containing post's id, creation and last activity dates.

    Returns:
        dict: A dictionary with posts id as keys, and activity time as values.
    """
    #Extract post id and dates
    post_id = list(data.keys())[0]
    dates = list(data.values())[0]
    #Calculate activity time
    creation_date = dt.datetime.strptime(dates[0], DATETIME_FORMAT)
    last_activity_date = dt.datetime.strptime(dates[1], DATETIME_FORMAT)
    activity_time = last_activity_date - creation_date
    #Return updated dictionary
    return {post_id : activity_time}
    
def reduce_top10_activity_time(data1, data2):
    """
    Reducer method that calculate the top 10 posts by activity_time.

    Args:
        data1 (dict): Dictionary 1 containing cumulative data.
        data2 (dict): Dictionary 2 to extract data from.

    Returns:
        dict: Dictionary containing top 10 posts by activity_time.
    """
    #Add each item from data2 to data 1
    #Since they are unique posts id, can't be duplicated keys
    for key, value in data2.items():
        data1.update({key : value})
    #Order data1 dictionary by Activity time (values) in descending order
    ordered_data1 = dict(sorted(data1.items(), reverse=True, key=lambda item: item[1]))
    #Stay only with top 10 items by activity time
    if len(ordered_data1) > 10:
        keys_to_delete = list(ordered_data1.keys())[10:]
        for key in keys_to_delete:
            del(ordered_data1[key])
    #Return ordered data1 dictionary
    return ordered_data1

def mapper(data):
    """
    Main mapper method that receive a chunk of data and get top 10 questions by activity time for that chunk.

    Args:
        data (list): List containing XML rows objects.

    Returns:
        dict: Dictionary containing 10 posts with their ids and their activity times.
    """
    #Filter for questions
    questions = list(filter(check_is_question, data))
    #Extract creation and last activity dates
    creation_date_and_last_activity = list(map(get_creation_date_and_last_activity, questions))
    #Filter invalid posts
    creation_date_and_last_activity = list(filter(None, creation_date_and_last_activity))
    #Calculate activity time
    activity_time = list(map(get_activity_time, creation_date_and_last_activity))
    try:
        #Try to get top 10 posts by activity time
        top10_activity_time = reduce(reduce_top10_activity_time, activity_time)
    #If this chunk has not any valid post, return None
    except:
        logger.warning("This chunk doesn't have any valid post.")
        return
    #Return resulting dictionary
    return top10_activity_time

def read_xml(path):
    """
    Read XML file at given path and return its root.

    Args:
        path (str): Path of XML file.

    Returns:
        root: Root of readed XML file.
    """
    #Get the tree of XML file at 'path'
    tree = ET.parse(path)
    #Return its root
    return tree.getroot()

def map_reduce(root, csv_path):
    """
    Method that applies MapReduce logic in order to process big volumes of data given by 'root'
    and store the results in a .csv file in 'csv_path'.

    Args:
        root (root): Root of XML file containing the data.
        csv_path (str): Path where store generated .csv file.
    """
    #Segment data into chunks of 50 elements
    data_chunks = chunckify(root, 50)
    #Map the data chunks with main mapper method
    mapped = list(map(mapper, data_chunks))
    logger.info("Mapped chunks successfully.")
    #Filter not valid chunks
    mapped = list(filter(None, mapped))
    logger.info("Filtered empty chunks.")
    #Reduce chunks into a single dictionary
    reduced = reduce(reduce_top10_activity_time, mapped)
    logger.info("Data reduced successfully.")
    #Create datadrame from that dictionary
    df = pd.DataFrame(data=reduced.keys(), columns=["Id"])
    df["ActivityTime"] = reduced.values()
    #Create the folder if not exists
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    #Store it in given path
    df.to_csv(csv_path)
    #Log
    logger.info("Dataset successfully generated in " + csv_path)

#This is called when this .py is run directly
if __name__ == "__main__":
    #Log
    logger.info("Starting program...")
    logger.info("Reading data from: " + XML_PATH)
    #Get the data from xml file
    root = read_xml(XML_PATH)
    #Log
    logger.info("Started mapReduce logic")
    #Process that data with mapReduce logic
    map_reduce(root, OUTPUT_CSV_PATH)