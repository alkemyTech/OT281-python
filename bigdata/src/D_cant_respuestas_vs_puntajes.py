'''
    Script that applies MapReduce logic in order to process data from a .xml file and store results in a .csv file.
    INPUT: "posts.xml" containing data from Stack Overflow posts.
    OUTPUT: "D_cant_respuestas_vs_puntajes.csv" containing a small dataset describing the relationship
        between posts answer count and its score. In order to do this, answer count has been segmented into
        intervals defined by INTERVALS_BOUNDARIES constant.
        The intervals include start boundary and exclude end boundary.
        Eg: - "0-1" interval include only posts with 0 answers.
            - "5-10" interval include posts with 5,6,7,8 and 9 answers.
    This script uses a method from 'chunckify.py' script located in 'libs' folder, so in order to run this,
    you must do it as a module with "python -m" or set PYTHONPATH env variable.
'''

#Imports
from functools import reduce
import os
from unicodedata import name
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

#Define interval boundaries
INTERVAL_BOUNDARIES = [0, 1, 2, 5, 10, 25, 50, 100, 200, 500, 1000]
#INTERVAL_BOUNDARIES = [0, 5, 50, 100, 200]

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

def get_answer_count_and_score(data):
    """
    This method extracts AnswerCount and Score attributes from given data, and return both in a dictionary.

    Args:
        data (row): Data in a XML row format.

    Returns:
        dict: Dictionary with AnswerCount as key and Score as value.
    """
    #Return both attributes in a dictionary
    try:
        answer_count = int(data.attrib["AnswerCount"])
        score = int(data.attrib["Score"])
        return {answer_count : score}
    #If can't find any attribute, return None
    except:
        return

def segment_answer_count(data):
    """
    This method segment AnswerCount into intervals.
    The intervals boundaries are given by INTERVAL_BOUNDARIES constant.

    Args:
        data (dict): A dictionary with AnswerCount as keys and Score as values.

    Returns:
        dict: Dictionary with answer count intervals as keys and Score as values.
    """
    #Get the boundaries list
    boundaries_list = INTERVAL_BOUNDARIES
    #Get answer count and score from data
    answer_count = list(data.keys())[0]
    score = list(data.values())[0]
    #Iterate through boundaries
    for idx, boundary in enumerate(boundaries_list):
        #If is not the penultimate boundary
        if idx < len(boundaries_list) - 1:
            #Get next boundary
            next_boundary = boundaries_list[idx+1]
            #If answer count is between this boundary and the next
            if answer_count >= boundary and answer_count < next_boundary:
                #Add item to data with interval as key instead of answer count
                interval = f'{boundary}-{next_boundary}'
                data.update({interval : score})
                #Delete old answer count item
                del(data[answer_count])
                #Return updated data dict
                return data
        #If is the penultimate boundary
        else:
            #And answer_count is higher than it
            if answer_count >= boundary:
                #Add item to data with interval as key instead of answer count
                interval = f'{boundary}+'
                data.update({interval : score})
                #Delete old answer count item
                del(data[answer_count])
                #Return updated data dict
                return data
    #In case of data not fitting in any interval, return None
    return

def get_interval_avg_score(data1, data2):
    """
    Reducer method that calculate the average score per interval.

    Args:
        data1 (dict): Dictionary 1 containing cumulative data.
        data2 (dict): Dictionary 2 to extract data from.

    Returns:
        dict: Dictionary with unique answer count intervals as keys and its corresponding cumulative average scores.
    """
    #Iterate through data2 items
    for key, value in data2.items():
        #If the key exists in data1 dictionary
        if key in data1.keys():
            #Update the value for that key in data1, with the average of both scores
            data1.update({key : (data1[key] + value) / 2})
        #If the key does not exists in data 1 dictionary, just add it
        else:
            data1.update({key : value})
    #Return updated data1 dictionary
    return data1

def mapper(data):
    """
    Main mapper method that receive a chunk of data and calculate average score per answer count interval for that chunk.

    Args:
        data (list): List containing XML rows objects.

    Returns:
        dict: Dictionary containing average score per answer count interval.
    """
    #Filter for questions
    questions = list(filter(check_is_question, data))
    #Extract answer count and score
    answer_count_and_score = list(map(get_answer_count_and_score, questions))
    #Filter posts without answers
    answer_count_and_score = list(filter(None, answer_count_and_score))
    #Segment answer count into intervals
    answer_interval_and_score = list(map(segment_answer_count, answer_count_and_score))
    try:
        #Try to calculate avg score per interval
        reduced = reduce(get_interval_avg_score, answer_interval_and_score)
    #If this chunk has not any valid post, return None
    except:
        return
    #Return resulting dictionary
    return reduced

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
    reduced = reduce(get_interval_avg_score, mapped)
    #Create datadrame from that dictionary
    df = pd.DataFrame(data=reduced.keys(), columns=["AnswerCountInterval"])
    df["AverageScore"] = reduced.values()
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