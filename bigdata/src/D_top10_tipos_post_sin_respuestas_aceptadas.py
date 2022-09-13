'''
    Script that applies MapReduce logic in order to process data from a .xml file and store results in a .csv file.
    INPUT: "posts.xml" containing data from Stack Overflow posts.
    OUTPUT: "D_top10_tipos_post_sin_respuestas_aceptadas.csv" containing a small dataset with the top 10 questions
    without accepted answers, ordered by descending answer count.
    This script uses a method from 'chunckify.py' script located in 'libs' folder, so in order to run this,
    you must do it as a module with "python -m" or set PYTHONPATH env variable.
'''

#Imports
from functools import reduce
import os
import xml.etree.ElementTree as ET
import pandas as pd
from ..libs.chunckify import chunckify

#Get the root folder (project folder)
ROOT_FOLDER = os.path.dirname(os.path.dirname(os.path.normpath(__file__)))
#Get the xml path
XML_PATH = os.path.join(ROOT_FOLDER, 'datasets', 'posts.xml')
#Get filename (without extension)
FILENAME = os.path.split(__file__)[-1].rstrip('.py')
#Define output csv path
OUTPUT_CSV_PATH = os.path.join(ROOT_FOLDER, 'output', FILENAME + '.csv')

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

def check_not_accepted_answer(data):
    """
    Check if given data has not an accepted answer.
    
    Args:
        data (row): Data in a XML row format.

    Returns:
        bool: True if data has not an accepted answer. False if it has, or if can't find 'AcceptedAnswerId' attribute.
    """
    #Return true if the post has not an accepted answer, false otherwise
    try:
        if data.attrib["AcceptedAnswerId"] == None:
            return True
        return False
    #If row does not have AcceptedAnswerId attribute, return true
    except:
        return True

def get_answer_count(data):
    """
    This method extracts Id and AnswerCount attributes from given data and returns it in a dictionary.

    Args:
        data (row): Data in a XML row format.

    Returns:
        dict: Dictionary with Id as keys and AnswerCount as values
    """
    #Return attributes in a dictionary
    try:
        post_id = data.attrib["Id"]
        answer_count = int(data.attrib["AnswerCount"])
        return {post_id : answer_count}
    #If can't find any attribute, return None
    except:
        return

def reduce_top10_answer_count(data1, data2):
    """
    Reducer method that calculate the top 10 posts by answer_count.

    Args:
        data1 (dict): Dictionary 1 containing cumulative data.
        data2 (dict): Dictionary 2 to extract data from.

    Returns:
        dict: Dictionary containing top 10 posts by answer_count.
    """
    #Add each item from data2 to data 1
    #Since they are unique posts id, can't be duplicated keys
    for key, value in data2.items():
        data1.update({key : value})
    #Order data1 dictionary by AnswerCount (values) in descending order
    ordered_data1 = dict(sorted(data1.items(), reverse=True, key=lambda item: item[1]))
    #Stay only with top 10 items by AnswerCount
    if len(ordered_data1) > 10:
        keys_to_delete = list(ordered_data1.keys())[10:]
        for key in keys_to_delete:
            del(ordered_data1[key])
    #Return ordered data1 dictionary
    return ordered_data1

def mapper(data):
    """
    Main mapper method that receive a chunk of data and get top 10 questions
    with no accepted answer ordered by answers in descending order for that chunk.

    Args:
        data (list): List containing XML rows objects.

    Returns:
        dict: Dictionary containing 10 posts with their ids and their answer counts.
    """
    #Filter for questions
    questions = list(filter(check_is_question, data))
    #Filter for questions without accepted answer
    questions_no_accepted_answer = list(filter(check_not_accepted_answer, questions))
    #Extract answer counts
    answer_counts = list(map(get_answer_count, questions_no_accepted_answer))
    try:
        #Try to get top 10 posts by answer count
        top10_answer_count = reduce(reduce_top10_answer_count, answer_counts)
    #If this chunk has not any valid post, return None
    except:
        return
    #Return resulting dictionary
    return top10_answer_count

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
    #Filter not valid chunks
    mapped = list(filter(None, mapped))
    #Reduce chunks into a single dictionary
    reduced = reduce(reduce_top10_answer_count, mapped)
    #Create datadrame
    df = pd.DataFrame(data=reduced.keys(), columns=["Id"])
    df["AnswerCount"] = reduced.values()
    #Create the folder if not exists
    os.makedirs(os.path.dirname(csv_path), exist_ok=True)
    #Store it in given path
    df.to_csv(csv_path)

#This is called when this .py is run directly
if __name__ == "__main__":
    #Get the data from xml file
    root = read_xml(XML_PATH)
    #Process that data with mapReduce logic
    map_reduce(root, OUTPUT_CSV_PATH)