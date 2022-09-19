import pandas as pd
import xml.etree.ElementTree as ET
import time
from operator import itemgetter
import os

################## Path #############################
CURRENT_DIR = os.path.dirname(os.path.normpath(__file__))
ROOT_FOLDER = os.path.abspath(os.path.join(CURRENT_DIR, os.pardir))
FILE_NAME = f'{ROOT_FOLDER}/datasets/posts.xml'
OUTPUT_CSV = f'{ROOT_FOLDER}/output/H_top10_post_menos_visto.csv'


################### 1) Top 10 post menos vistos #############################

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


resultado=dict()


def task(csv_path):
        #Mapeo por menos vistos
        mapped=map_xml('Id','ViewCount',0,[])
        #reduzco a 10 menos vistos
        reduced = reduce_c_args(reducer_menores, mapped,10,False,1)
        resultado['Top 10 menos Vistos']=[pd.DataFrame(reduced, columns=['Id','Vistas'])]
        #lo pasamos a DF para guardarlo en el CVS
        df = pd.DataFrame(resultado)
        os.makedirs(os.path.dirname(csv_path), exist_ok=True)
        df.to_csv(csv_path)



def main():
    #Llamamos la funcion y le agregamos el pad para crear el CSV
    task(OUTPUT_CSV)
    print("Fin del Analisis")
if __name__ == '__main__':
    main()