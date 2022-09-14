
# Import libraries 
import xml.etree.ElementTree as ET
import re
from typing import Counter
import functools
from functools import reduce
import itertools
from collections import ChainMap
import os
import sys


import os
# Import logging module
import logging.config
import logging
# Set logging path and init logging config
filename = os.path.join(os.path.dirname(__file__), '../logs/C_logger.cfg')
logging.config.fileConfig(filename)



# Set path to local libraries
path_librerias=os.path.join(os.path.dirname(__file__), '../libs/')
sys.path.insert(0, path_librerias)

#Import chunckify from local libraries
from chunckify import chunckify
from C_export_to_csv import export_to_csv




# IMPORT posts.xml and make a tree object instance
path_to_postsxml=os.path.join(os.path.dirname(__file__), '../datasets/posts.xml')
logging.debug("The xml file was imported succesfuly from {}".format(path_to_postsxml))
tree = ET.parse(path_to_postsxml)
root = tree.getroot()




# Function to get a list of PostID    
# Function to get a list of AcceptedAnswerId    
def get_PostID_and_answer(data):
# Function to get the list of answers by ParentId that is related to a PostID    
    
    try:
        answers_id = data.attrib['ParentId']
        
    except:
        return
    ## logging.debug("The answers_id value in get_PostID_and_answer is {}".format(answers_id))
    return answers_id


def get_accepted_answer(data):
# Function to get the list of Id with aceptedanswers    
    
    try:
        answers_id = data.attrib['Id']
        accepted_answers_id = data.attrib['AcceptedAnswerId']
    except:
        return
    ##logging.debug("The answers_id value from get_accepted_answer is: {}".format(answers_id))
    return answers_id




def runner_1(data):
    # Function to get a clean list (with no None value) with all the times that a question was answered 
    a=list(map(get_PostID_and_answer,data))
    
    a_limpia= list(filter(None,a))
    
    ##logging.debug("The a_limpia value is {}".format(a_limpia))
    return a_limpia

def runner_2(data):
    # Function to get a clean list (with no None value) with all the questions that has accepted answers
    
    b=list(map(get_accepted_answer,data))
    
    b_limpia= list(filter(None,b))
    ##logging.debug("The a_limpia value is {}".format(b_limpia))
    return b_limpia

#Get chunks of data to process
data_chunks = chunckify(root, 50)
# Get list of post with accepted answers
posts_accepted_answers= list(map(runner_2,data_chunks))
# Merge the lists of posts that has accepted answers
posts_accepted_answers=list(itertools.chain.from_iterable(posts_accepted_answers))

#Get chunks of data to process
data_chunks = chunckify(root, 50)
# Get list with the posts, each post repeated for every answer 
posts_answered= list(map(runner_1,data_chunks))

# Get a Counter object with the times that every post was answered
posts_answered_countofanswers=reduce(lambda x,y:Counter(x)+Counter(y),posts_answered)
##logging.debug("The posts_answered_countofanswers value is {}".format(posts_answered_countofanswers))


def get_top_answered_accepted_post(aceptedanswers,data=posts_answered_countofanswers):
# Function to get a tuple with the (post,amount of answers of that post) that has an accepted answer
# it works checking if the Id (post id) its pressent in the list of ParentId (from the accepted answers lists  ) 
    lista_posts=[]
    
    if data[aceptedanswers]:
        ##logging.debug("The tuple value is {},{}".format(aceptedanswers,data[aceptedanswers]))
        return aceptedanswers,data[aceptedanswers]


# Get a list with tuples with the posts with accepted answers and its amount of answers
lista_posts=list(map(get_top_answered_accepted_post,posts_accepted_answers))
logging.debug("List of posts succesfully obtained ")

# Clean list (remove None values)
lista_posts_limpia= list(filter(None,lista_posts))



def sort_data(data):
#Function to sort data to get the top values    
    return sorted(data,key= lambda x:x[1],reverse=True )


top_post_answered = sort_data(lista_posts_limpia)

top_10_post_answered=top_post_answered[:10]
logging.debug("Code has run succesfully")
logging.debug("Output: {}".format(top_10_post_answered))
print(top_10_post_answered)


fields=['Post_id','Count_of_answers']
fromlist=top_10_post_answered
filename='C_first_request.csv'
export_to_csv(fields,fromlist,filename)

