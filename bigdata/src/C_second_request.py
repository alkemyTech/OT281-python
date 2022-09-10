
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

path_librerias=os.path.join(os.path.dirname(__file__), '../libs/')
sys.path.insert(0, path_librerias)

from chunckify import chunckify


# IMPORT posts.xml and make a tree object instance
path_to_postsxml=os.path.join(os.path.dirname(__file__), '../datasets/posts.xml')
tree = ET.parse(path_to_postsxml)
root = tree.getroot()



# Function to return a list of dict of parentid : count of times that it appears
def mapper1(data):
    parentId_mapeadas=list(map(get_ParentID,data))
    parentId_mapeadas= list(filter(None,parentId_mapeadas))
    list_dict_parentId= list(sum_Parent_ID(parentId_mapeadas))
    
    return list_dict_parentId


# Function to get a list of ParentID    
def get_ParentID(data):
    try:
        ParentId_id = data.attrib['ParentId']
        
    except:
            return    
    return ParentId_id


# Function to get the total of times that a ParentID its listed
def sum_Parent_ID(data):
    return Counter(data)


def mapper2(data):
# Get list of dicts with postid and the count of words in it   
    post_mapeadas=    list(map(get_Post,data))
    
    
    return post_mapeadas

def get_Post(data):
    # Get Body and postid list
    body = data.attrib['Body']
    postID = data.attrib['Id']
    # Clean the body and get a list of words
    body = re.findall('(?<!\S)[A-Za-z]+(?!\S)|(?<!\S)[A-Za-z]+(?=:(?!\S))',body)
    # Filter the None list objetcs
    body= list(filter(None,body))
    # count the number of words in each post
    counter_palabras = len(body)
    
    
    
    return {postID:counter_palabras}


# STEPS TO GET DICT WITH PARENT_ID vs COUNT OF TIMES THAT IS LISTED
#Get chunks of data to process
data_chunks = chunckify(root, 50)
mapped1 = list(map(mapper1,data_chunks))
#dict_posts_palabras = list(reduce(lambda a,b: a.update(b),mapped[:][1]))
# Make dict with the ParentID and the times that it appears
dict_ParentId=reduce(lambda x,y:Counter(x)+Counter(y),mapped1)



# STEPS TO GET DICT WITH POST_ID vs COUNT OF WORDS 
#Get chunks of data to process
data_chunks = chunckify(root, 50)
mapped2 = list(map(mapper2,data_chunks))
# TRANSFORM LIST OF DICT INTO A BIG DICT
mapped2=list(itertools.chain.from_iterable(mapped2))
postid_palabras = reduce(lambda d, src: d.update(src) or d, mapped2,{})


# Get list of keys 
post_con_respuesta=list(dict_ParentId.keys())

# Function to generate dict of postID and amount of word 
def genera_palabras_dict(data,postid_palabras=postid_palabras):
    return {data:postid_palabras[data]}

# Generate list of dicts with postID and amount of word    
postid_palabras_limpio=list(map(genera_palabras_dict,post_con_respuesta))
# Convert list of dicts in one big dicts
postid_palabras_limpio = reduce(lambda d, src: d.update(src) or d, postid_palabras_limpio,{})


def relacion(data,postid_palabras_limpio=postid_palabras_limpio,dict_ParentId=dict_ParentId):
# Function to get relationship between amount of answers / amount of words in post 
    try:
        # This try clause is because there are many post with 0 words
        relacion_porc = (dict_ParentId[data]/postid_palabras_limpio[data])*100
        return (data,relacion_porc)
        # Case post words count is 0 will ignore it
    except ZeroDivisionError:
        return 
    
# Get a list of tuples with post ID and relationship    
lista_relacion=list(map(relacion,post_con_respuesta))

print(lista_relacion)