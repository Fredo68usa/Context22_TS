$ cat Outlier_Normality_Testing_2.py
# birch clustering
from numpy import unique
from numpy import where
import pymongo
import psutil
import urllib.parse
import json
import getpass
import datetime
import time
import os
import calendar
import socket
import glob
import psycopg2
import numpy as np
import pandas as pd
# Normality testing
from scipy.stats import normaltest
from scipy.stats import shapiro
from scipy.stats import jarque_bera


#Access to PostGreSQL
postgres_connect = None
cursor = None
monthStart = 1
CURRENT_TIMESTAMP = None
DayOfWeek = None

Extract = {
'_id':0,
'Timestamp':1,
'Response Time':1,
'Records Affected':1,
'DayOfYear':1,
'WeekNbr':1,
'HashHash User Datastore':1,
'DayOfWeek':1
}


# Opening PostGreSQL
def open_PostGres():

    try:
         postgres_connect = psycopg2.connect(user = "sonargd",
                                  # password = "AIM2020",
                                  # host = "127.0.0.1",
                                  port = "5432",
                                  database = "infra"
                                  )


    except (Exception, psycopg2.Error) as error :
         print("Error while connecting to PostgreSQL", error)
         print ("Hello")

    return(postgres_connect)

# --- Opening Mongo ---------------
def open_Mongo():

   # --- Password ----
   try:
        p = getpass.getpass()
   except Exception as error:
        print('ERROR', error)
   else:
        print('Password entered')

   # ----- Connection
   username = urllib.parse.quote_plus('fpetit')
   password = urllib.parse.quote_plus(p)

   myclient = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1:27117/admin' % (username,password))
   return(myclient)


# --------------------   MAIN  ------------------
if __name__ == '__main__':
    print("Start Daily Forecast Generation")
    # print ('Type of myListIPs', type(myListIPs))
    monthStart = input(" Month to Start from (nbr) : ")
    QueryNbr = input(" Query Nbr please : ")
    DayOfWeek = input("Day Of Week (number between 0 & 6 - 0 = sunday , return if all: ")
    print ( 'DayOfWeek  ' , DayOfWeek )
    dataType = input(" Extraction : 1  - Response Time : 2  --  ")
    print ('DayOfWeek', DayOfWeek )

    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)  #Create a TCP/IP
    # --- Process
    process = psutil.Process(os.getpid())

    # -- Read list of Crit Servers
    myclient=open_Mongo()
    # -- Open PostGres
    postgres_connect=open_PostGres()
    cursor = postgres_connect.cursor()
    print ( postgres_connect.get_dsn_parameters(),"\n")
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")
    queryNbr = "'#" + QueryNbr +"'"
    print ( queryNbr )
    select_Query = "select query from query where queryName = " + queryNbr
    cursor.execute(select_Query)
    query_to_study_t  = cursor.fetchone()
    query_to_study  = query_to_study_t[0]
    print (query_to_study)

    date_start = datetime.datetime(2020, int(monthStart) , 1, 5)
    if DayOfWeek  !=  "" :
       DayOfWeek = int(DayOfWeek)
    mydb = myclient["AIMAnalytics"]
    if int(QueryNbr) < 104:
       mycol = mydb["ENRICHED_FULL_SQL_FINAL"]
    else:
       mycol = mydb["ENRICHED_FULL_SQL_PYTHON"]

    print ("Memory ",'{:d}'.format(process.memory_info().rss))
    ticks_start = time.time()
    print ('Starting time' , ticks_start)
    # Nbr = mycol.find({'HashHash User Datastore': query_to_study , 'Timestamp':{'$gte': date_start},'DayOfWeek': DayOfWeek }).count()
    if DayOfWeek == "" :
       Nbr = mycol.find({'HashHash User Datastore': query_to_study , 'Timestamp':{'$gte': date_start} }).count()
    else:
       Nbr = mycol.find({'HashHash User Datastore': query_to_study , 'Timestamp':{'$gte': date_start},'DayOfWeek': DayOfWeek  }).count()
    print (' Nbr of Documents to Process',Nbr)
    if Nbr == 0 :
       exit(0)
    if DayOfWeek == "" :
       Extraction = mycol.find({'HashHash User Datastore': query_to_study , 'Timestamp':{'$gte': date_start} } , Extract )
    else:
      Extraction = mycol.find({'HashHash User Datastore': query_to_study , 'Timestamp':{'$gte': date_start} , 'DayOfWeek': DayOfWeek } , Extract )
    print (' Mongo find completed')
    # -- Insert
    data_list = list(Extraction)
    data_df = pd.DataFrame(data_list)
    if dataType == '1':
         data = data_df[['Records Affected']]
    else :
         data = data_df[['Response Time']]

    print (data)
    print (type(data))

    data=data.dropna()

    print (type(data))

    # normality test
    print ('===========================')
    print ('Normality Testing : Shapiro')
    print ('---------------------------')
    stats, ps = shapiro(data)
    print('Statistics=%.3f, p=%.3f' % (stats, ps))
    # interpret
    alpha = 0.05
    print('Alpha : ', alpha)
    if ps > alpha:
        print('Sample looks Gaussian (fail to reject H0)')
    else:
        print('Sample does NOT look Gaussian (reject H0)')

    print('Done')
    print ('=========================')

    # normality test
    print ('===========================')
    print ("Normality Testing : D'Agostino")
    print ('---------------------------')

    stata, pa = normaltest(data)
    print('Statistics=%.3f, p=%.3f' % (stata, pa))
    # interpret
    alpha = 1e-3
    print ('Alpha : ', alpha)
    if pa > alpha:
        print('Sample looks Gaussian (fail to reject H0)')
    else:
        print('Sample does NOT look Gaussian (reject H0)')
    print('Done')
    print ('=========================')

    print ('===========================')
    print ("Normality Testing : Jarque-Bera")
    print ('---------------------------')

    # jarque_bera_test = jarque_bera(data)
    statj, pj = jarque_bera(data)

    print('Statistics=%.3f, p=%.3f' % (statj,pj))
    # interpret
    alpha = .1
    print ('Alpha : ', alpha)
    if pj > alpha:
        print('Sample looks Gaussian (fail to reject H0)')
    else:
        print('Sample does NOT look Gaussian (reject H0)')
    print('Done')
    print ('=========================')

    ticks_after_mongo = time.time()
    print ('After Mongo time' , ticks_start)
    print ("Memory ",'{:d}'.format(process.memory_info().rss))
    # print(ExtrDf['Response Time'].describe())
    # print(ExtrDf['Response Time'])
    ticks_after_DataFrame = time.time()
    print ('After DataFrame time' , ticks_start)
    print (' Mongo time' , ticks_after_mongo - ticks_start)
    print (' DataFrame time' , ticks_after_DataFrame - ticks_after_mongo)
    print ("Memory ",'{:d}'.format(process.memory_info().rss))

    if(postgres_connect):
      cursor.close()
      postgres_connect.close()
      print("PostgreSQL connection is closed")


    print("End Daily Clusters Generation")

