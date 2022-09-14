
# Import libraries 
import xml.etree.ElementTree as ET
import re
from typing import Counter
import functools
from functools import reduce
import itertools
from itertools import chain
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


path_librerias=os.path.join(os.path.dirname(__file__), '../libs/')
sys.path.insert(0, path_librerias)

from chunckify import chunckify


# IMPORT posts.xml and make a tree object instance
path_to_postsxml=os.path.join(os.path.dirname(__file__), '../datasets/posts.xml')
logging.debug("xml file imported successfully from: {}".format(path_to_postsxml))    
tree = ET.parse(path_to_postsxml)
root = tree.getroot()



def sort_data(data1,data2):
# Function to get list sortered from max to min, it takes 2 list of tuples and sort them using the 
# second value of each tuple
    return sorted(list(chain(data1,data2)),key= lambda x:x[1],reverse=True )

def get_post_favorites(data):
# Function to get a tuple with Post Id and its Favorite Count value    
# It Return a list with tuples or None value   
    try:       
        favorite_count=data.attrib['FavoriteCount']
        post_id=data.attrib['Id']       
        ## logging.debug("get_post_favorites returned tuple: ({},{})".format(post_id,favorite_count))
        return (post_id,int(favorite_count))
    except:
            return
 

def runner(data):
# Function to get a clean list of tuples with the Id Post and its favorite Value
    out=list(map(get_post_favorites,data))
    out_limpia= list(filter(None,out))
    ## logging.debug("runner output: {}".format(out_limpia))
    return out_limpia

def devolver_top_post(out):
# Function to get a list with the top 200 post with more favorite values
# the value 200 was decided because its unprobable that we cant find 10 accepted answers in 200 values
    top200=[]
    tuple_top200=reduce(sort_data,out)[:200]
    for i in tuple_top200:
        top200.append(i[0])
    ## logging.debug("devolver_top_post returne {}".format(top200))
    return top200


def get_accepted_owner(data):
# Function to get a tuple of values with ParentID and OwnerUserID
# It retrieves the information needed to relate a question to the owner of the accepted answer
    try:
        accepted_answers_id = data.attrib['AcceptedAnswerId']
        parent_id = data.attrib['ParentID']
        owner_id = data.attrib['OwnerUserId']
        return parent_id,owner_id
    except:
        return
    
    


# STEPS TO GET tuples WITH postid top favorites
#Get chunks of data to process
data_chunks = chunckify(root, 50)
#Process every data chunck
out= list(map(runner,data_chunks))
top200=devolver_top_post(out)

def get_top_accepted_owner(data,top200=top200):
# Function to return a list of the owners of answer that were accepted in each of the top200 questions
        if data[0] in top200:
            ## logging.debug("get_top_accepted_owner returned {}".format(data[1]))
            return data[1]
    

# NOW WE search for the answers with parentid == top200 < its the answer of the top posts we took the first 200 because it could be no accepted answer in the top 10 posts
# With the list of top we look for the top 10 that has accepted answer

data_chunks = chunckify(root, 50)
out_parents= list(map(get_accepted_owner,data_chunks))
out_parents_limpia= list(filter(None,out))
out_parents_limpia=list(itertools.chain(*out_parents_limpia))
lista_owner=list(map(get_top_accepted_owner,out_parents_limpia))
lista_owner_limpia= list(filter(None,lista_owner))
#lista_owner_limpia=list(itertools.chain(*lista_owner_limpia))
# TOP 10 OWNERID that has answered a top question (top because has a lot of favorites points)
lista_owner_limpia[:9]

print(lista_owner_limpia[:9])