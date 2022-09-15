import pandas as pd
import xml.etree.ElementTree as ET
from collections import Counter
from nltk.corpus import stopwords
from operator import itemgetter
import os
import re


################## Path #############################
CURRENT_DIR = os.path.dirname(os.path.normpath(__file__))
ROOT_FOLDER = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
FILE_NAME = f'{ROOT_FOLDER}/datasets/posts.xml'
OUTPUT_CSV = f'{ROOT_FOLDER}/output/H_top10_palabras_mas_nombradas_por_post_tipo.csv'


################### 2) Top 10 palabras mas nombradas por post por tipo #############################


#Adaptacion funcion reduce para que tome argumentos es relacion n > m
def reduce_c_args(function, iterable, n, rev, flag_orden, initializer=None):
    it = iter(iterable)
    if initializer is None:
        value = next(it)
    else:
        value = initializer
    for element in it:
        value = function(value, element,n,rev,flag_orden)
    return value

#funcion reduce para generar el top
def reducer_menores(p, c, top,rev,fo):
    n=[]
    if isinstance(p[0],list):
        n=p
    else:
        n.append(p)
    n.append(c)
    if len(n)>10:
        N=top
    else:
        N=len(n)
    return sorted(n,key=itemgetter(fo),reverse=rev)[0:N]

#funcion de mapeo, devuelve clave valores es relacion n a n 
def map_xml(nombre,nombre2,flag_post,ans):
    mapped=[]  
    for event, elem in ET.iterparse(FILE_NAME):
        if elem.attrib=={}:
            break
        else:
            if flag_post==0:
                mapped.append([elem.attrib[nombre],elem.attrib[nombre2]])
            elif flag_post==1:
                if elem.attrib['PostTypeId']=='1':
                    mapped.append([elem.attrib[nombre],elem.attrib[nombre2]])
            elif flag_post==2:
                if elem.attrib['PostTypeId']=='2':
                    mapped.append([elem.attrib[nombre],elem.attrib[nombre2]])
            elif flag_post==3:
                if elem.attrib['PostTypeId']=='1' and 'AcceptedAnswerId' in elem.attrib:
                    mapped.append([elem.attrib[nombre],elem.attrib[nombre2],
                        elem.attrib['AcceptedAnswerId'],elem.attrib['CreationDate']])
            elif flag_post==4:
                if elem.attrib['PostTypeId']=='2' and elem.attrib['Id'] in ans:
                    mapped.append([elem.attrib[nombre],elem.attrib[nombre2]])

        elem.clear()
    return mapped


#funcion para quitar caracteres que no sean claros
def limpiar_palabra(palabra):
    return re.sub(r'[^\w\s]','',palabra).lower()

#Funcion para eliminar stopwords y las que no son alfabeticas
def palabra_no_stop(palabra):
    return palabra not in stopwords.words() and palabra.isalpha()

#mapear y filtro de palabras
def palabras_top(data):
    cnt = Counter()
    for text in data:
        tokens_in_text = text.split()
        tokens_in_text = map(limpiar_palabra, tokens_in_text)
        tokens_in_text = filter(palabra_no_stop, tokens_in_text)
        cnt.update(tokens_in_text)
    return cnt.most_common(10)
 
resultado=dict()


def task(csv_path):
        for i in range(1,3):
            #Mapeo por score y por tipo
            mapped=map_xml('Score','Body',i,[])
            #reduzco a 10 mas visto por tipo
            reduced = reduce_c_args(reducer_menores, mapped,10,True,0)
            #convierto en dataframe para trabajar el body de los posts
            df_reduced=pd.DataFrame(reduced, columns=['Score','Body'])
            #Obtengo las 10 palabras top por tipo de post
            resultado['Top 10 palabras en 10 post mas score Tipo Post=' + str(i)]=palabras_top(df_reduced['Body'].astype(str).apply(lambda y :re.sub('<.*?>','',y)))
        #Creo el DF para el csv  
        df = pd.DataFrame(resultado)
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        df.to_csv(csv_path)
        

def main():
    #Llamamos la funcion y le agregamos el pad para crear el CSV
    task(OUTPUT_CSV)
    print("Fin del Analisis")
     
if __name__ == '__main__':
    main()