#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
PRÁCTICA 4
"""

from pyspark import SparkContext
sc = SparkContext()

import json

"""Empezamos con el ejemplo"""

rdd_base = sc.textFile('sample_10e3.json') #creamos un rdd a partir del fichero de datos.


#para colocarlo de una manera más cómoda de manipular, utilizamos la función mapper de los ejemplos.
def mapper(line):
    data = json.loads(line)
    u_t = data['user_type']
    u_c = data['user_day_code']
    start = data['idunplug_station']
    end = data['idplug_station']
    time = data['travel_time']
    return u_t, u_c, start, end, time
#y creamos el rdd aplicando la función al base
rdd_util = rdd_base.map(mapper)

#filtramos los datos para quedarnos solo con los de user_type = 1 y transformamos cada dato en una tupla con la estación de salida, de entrada, y el tiempo de uso. 
rdd_users = rdd_util.filter(lambda x: x[0]==1).map(lambda x: (tuple(x[2:])))

#Primero contamos el número de bicis que salen de cada estación.
count_out = rdd_users.map(lambda x: x[0]).countByValue()
rdd_out = sc.parallelize(list(count_out.items())) #creamos un rdd con los valores obtenidos en count_out

#Después hacemos lo mismo con las estaciones a las que entran las bicis (por eso nos quedamos con el segundo elemento de cada tupla)
count_in = rdd_users.map(lambda x: x[1]).countByValue()
rdd_in = sc.parallelize(list(count_in.items()))

def agrupar_estaciones(par): #queremos meter cada estación en un grupo (las agruparemos de 10 en 10, siendo el grupo 0 las que van del 0-9, el 1 el que va del 10-29 y asi sucesivamente)
    clave = par[0]
    valor = par[1]
    grupo = clave // 10
    return grupo,valor

#aplicamos esta función a cada uno de los pares, tanto en el caso de rdd_out como en el rdd_in y después sumaremos cada uno de los valores de los grupos, para ver cuántas bicis de cada grupo de estaciones salen y entran.
rdd_out_agrup = rdd_out.map(agrupar_estaciones).reduceByKey(lambda x,y: x + y)

#hacemos lo mismo con rdd_in
rdd_in_agrup = rdd_in.map(agrupar_estaciones).reduceByKey(lambda x,y: x + y)
#como lo que queremos ver es de qué grupo (de qué zona) salen más bicicletas y a qué zona llegan más bicicletas (de momento en un mes),
#lo ordenamos por valores, para ver como están de mayor a menor.
rdd_out_agrup.sortBy(lambda x : x[1], ascending = False)

rdd_in_agrup.sortBy(lambda x : x[1], ascending = False)

"""SOLUCIÓN AL PROBLEMA"""

#creamos, como se anticipó, las cuatro listas con los ficheros de datos correspondientes:

invierno = ["201801_Usage_Bicimad.json", "201802_Usage_Bicimad.json","201803_Usage_Bicimad.json"]
primavera = ["201804_Usage_Bicimad.json", "201805_Usage_Bicimad.json","201806_Usage_Bicimad.json"]
verano = ["201807_Usage_Bicimad.json", "201808_Usage_Bicimad.json","201809_Usage_Bicimad.json"]
otoño = ["201810_Usage_Bicimad.json", "201811_Usage_Bicimad.json","201812_Usage_Bicimad.json"]

#ahora crearemos la función trimestres, que tratará de transformar cada lista en un rdd con los datos de tres meses en cada uno de ellos
def trimestres(sc,lista_filenames): 
    lista_rdds = []
    for mes in lista_filenames:
        lista_rdds.append(sc.textFile(mes))#nos transformará cada archivo en un rdd y lo almacenará en una lista de ellos
    rdd_final = sc.union(lista_rdds)#después los uniremos en un solo rdd con datos sin distinguir a qué mes pertenecen
    rdd_final = rdd_final.map(mapper) #convertimos cada dato en tuplas
    #filtramos para solo estudiar al tipo 1 y agrupamos los datos de salida y entrada de las bicis y el tiempo de viaje en una tupla.
    rdd_final = rdd_final.filter(lambda x: x[0]==1).map(lambda x: (tuple(x[2:])))
    return rdd_final

#se lo aplicamos a cada estación para obtener los cuatro rdds representando los datos del año divididos por estaciones
rdd_inv = trimestres(sc,invierno).filter(lambda x: x[1]<300)
rdd_prim = trimestres(sc,primavera).filter(lambda x: x[1]<300)
rdd_ver = trimestres(sc,verano).filter(lambda x: x[1]<300)
rdd_ot = trimestres(sc,otoño).filter(lambda x: x[1]<300)
#hacemos el filter porque hemos observado un valor de una estación de bicis aislado, que es el 2008 y no nos interesa estudiarlo.

rdd_prueba = rdd_inv.filter(lambda x: x[1]>300)

#ahora ya tenemos cuatro rdds con los datos de tres meses cada uno unidos, usando union como en algunos ejemplos vistos.
#a cada uno de ellos le podremos aplicar nuestro programa de más arriba. En esta ocasión uniremos todos los pasos dentro de una misma función:

def mas_bicis_salida(rdd_util):
    #queremos contar el número de bicis que salen de cada estación:
    count_out = rdd_util.map(lambda x: x[0]).countByValue()
    rdd_out = sc.parallelize(list(count_out.items()))
    #las agruparemos por zonas,  10 estaciones por zona, utilizando la función ya definida previamente 'agrupar_estaciones': 
    rdd_out_agrup = rdd_out.map(agrupar_estaciones).reduceByKey(lambda x,y: x + y)
    #lo último que nos quedará será agrupar de mayor a menor este rdd:
    rdd_out_agrup_ord = rdd_out_agrup.sortBy(lambda x : x[1], ascending = False)
    return rdd_out_agrup_ord
    
def mas_bicis_entrada(rdd_util):
    #queremos contar el número de bicis que entran en cada estación
    count_in = rdd_util.map(lambda x: x[1]).countByValue()
    rdd_in = sc.parallelize(list(count_in.items()))
    #las agruparemos por zonas igual que en el caso anterior:
    rdd_in_agrup = rdd_in.map(agrupar_estaciones).reduceByKey(lambda x,y: x + y)
    #lo último que nos quedará será agrupar de mayor a menor este rdd:
    rdd_in_agrup_ord = rdd_in_agrup.sortBy(lambda x : x[1], ascending = False)
    return rdd_in_agrup_ord

#aplicamos cada función a los cuatro rdds, para ver en cada estación del año de qué zona salen más bicis y a qué zona entran:
salida_inv = mas_bicis_salida(rdd_inv)
salida_prim = mas_bicis_salida(rdd_prim)
salida_ver = mas_bicis_salida(rdd_ver)
salida_ot = mas_bicis_salida(rdd_ot)

entrada_inv = mas_bicis_entrada(rdd_inv)
entrada_prim = mas_bicis_entrada(rdd_prim)
entrada_ver = mas_bicis_entrada(rdd_ver)
entrada_ot = mas_bicis_entrada(rdd_ot)

"""EXTRA"""
#lo último que haremos, aprovechando que tenemos todas las bicis que salen y que entran en total cada trimestre
#será ver cuál de todos los trimestres será en el que más bicis se alquilan.
num_inv = salida_inv.map(lambda x: x[1]).reduce(lambda x,y: x + y)
num_prim = salida_prim.map(lambda x: x[1]).reduce(lambda x,y: x + y)
num_ver = salida_ver.map(lambda x: x[1]).reduce(lambda x,y: x + y)
num_ot = salida_ot.map(lambda x: x[1]).reduce(lambda x,y: x + y)

#también podríamos querer ver si se ha perdido alguna bicicleta viendo el número de ellas que entran a las estaciones
#de modo que si hay menos que las que salieron, es que hay alguna que no ha llegado nunca a ser devuelta.
num_inv2 = entrada_inv.map(lambda x: x[1]).reduce(lambda x,y: x + y)
num_prim2 = entrada_prim.map(lambda x: x[1]).reduce(lambda x,y: x + y)
num_ver2 = entrada_ver.map(lambda x: x[1]).reduce(lambda x,y: x + y)
num_ot2 = entrada_ot.map(lambda x: x[1]).reduce(lambda x,y: x + y)