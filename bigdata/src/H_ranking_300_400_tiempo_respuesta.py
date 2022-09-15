import pandas as pd
import xml.etree.ElementTree as ET
from collections import Counter
from nltk.corpus import stopwords
import time
from operator import itemgetter
from datetime import datetime as dt
import os
import re


################## Tiempo de Ejecucion #############################
start_time = time.time()

################## Path #############################
CURRENT_DIR = os.path.dirname(os.path.normpath(__file__))
ROOT_FOLDER = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
FILE_NAME = f'{ROOT_FOLDER}/datasets/posts.xml'
OUTPUT_CSV = f'{ROOT_FOLDER}/output/H_ranking_300_400_tiempo_respuesta.csv'


########## 3) ranking de los primeros 300-400 por score, tomar el tiempo de respuesta promedio e informar un único valor. ###############

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

#funcion de mapeo, devuelve clave valor/es es relacion n a n 
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
# Funcion de task que usamos para ejecutar las tareas
def task(csv_path):
        #Mapeo por score y tipo 1 y que tengan respuesta 'Answerid'
        mapped=map_xml('Score','Id',3,[])
        #reduzco a 400 mas vistos
        reduced = reduce_c_args(reducer_menores, mapped,400,True,0)
        #Convierto en dataframe para trabajar el creation date y el 'AcceptedAnswerId' de la respuesta
        df_reduced=pd.DataFrame(reduced, columns=['Score','Id','AcceptedAnswerId','CreationDate'])
        df_reduced['CreationDate']=df_reduced['CreationDate'].apply(lambda y : dt.fromisoformat(y))
        #Mapeo para buscar los datos que necesito de la respuesta tipo post 2 y id en df_reduced['AcceptedAnswerId']
        mapped=map_xml('Id','CreationDate',4,df_reduced['AcceptedAnswerId'].astype(str).tolist())
        #Convierto en dataframe para trabajar la creation date
        df_mapped=pd.DataFrame(mapped,columns=['Idr','CreationDater'])
        df_mapped['CreationDater']=df_mapped['CreationDater'].apply(lambda y : dt.fromisoformat(y))
        #Uno los dataframe para calcular el tiempo de la respuesta
        df_dif=pd.merge(df_reduced,df_mapped,how='left',left_on=['AcceptedAnswerId'],right_on=['Idr'])
        #Calculo el promedio en minutos y guardo en resultado
        res_final=int(round(((df_dif['CreationDater']-df_dif['CreationDate']).mean()).total_seconds() / 60 , 0))
        resultado['Minutos promedio de respuesta para los Top 400 post por score']=[res_final]
        df = pd.DataFrame(resultado)
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        df.to_csv(csv_path)


def main():
    #Llamamos la funcion y le agregamos el pad para crear el CSV
    task(OUTPUT_CSV)
    print("Fin del Analisis")
    #Para ver el tiempo que tardo la ejecucion
    print("Tiempo de ejecucion--- %s seconds ---" % (time.time() - start_time))
     
if __name__ == '__main__':
    main()