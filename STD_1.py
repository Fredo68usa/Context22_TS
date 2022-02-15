$ cat STD_1.py
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
import csv
import psycopg2
import numpy as np
import pandas as pd
from matplotlib import pyplot
from sklearn.linear_model import LinearRegression
from scipy import stats

myListMeta_IP = []
myListMeta_Env = []
myListMeta_SubEnv = []
myListMeta_FQDN = []
cnt = 0

#Access to PostGreSQL
postgres_connect = None
cursor = None

CURRENT_TIMESTAMP = None


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
    DayOfWeek = input("Day Of Week (number between 0 & 6 - 0 = sunday : ")
    QueryNbr = input(" Query Nbr please : ")
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
    # queryNbr = "'#3'"
    # print ( queryNbr )
    queryNbr = "'#" + QueryNbr +"'"
    print ( queryNbr )
    select_Query = "select query from query where queryName = " + queryNbr
    cursor.execute(select_Query)
    query_to_study_t  = cursor.fetchone()
    query_to_study  = query_to_study_t[0]
    print (query_to_study)

    date_start = datetime.datetime(2020, 1, 1, 5)
    # query_to_study ='6458269868343302931:DEERFIELD&IDERAPRDSQLSVC:172.21.90.53:MS SQL SERVER:EVENT_MNGMT' # query 3
    # query_to_study ='4327225362513598429:DEERFIELD&RPTADMIN:172.16.122.138:MS SQL SERVER:AMERIGROUP'    # query  9
    # query_to_study = '6132971208689058408:EDT_DS:172.16.137.42:IOPD-PROD.AIMSPECIALTYHEALTH.C:MREFR'  # query 10
    # "HashHash User Datastore"
    DayOfWeek = int(DayOfWeek)
    mydb = myclient["AIMAnalytics"]
    mycol = mydb["ENRICHED_FULL_SQL_FINAL"]

    print ("Memory ",'{:d}'.format(process.memory_info().rss))
    ticks_start = time.time()
    print ('Starting time' , ticks_start)
    Nbr = mycol.find({'HashHash User Datastore': query_to_study , 'Timestamp':{'$gte': date_start},'DayOfWeek': DayOfWeek }).count()
    print (' Nbr of Documents to Process',Nbr)
    if Nbr == 0 :
       exit(0)
    Extraction = mycol.find({'HashHash User Datastore': query_to_study , 'Timestamp':{'$gte': date_start},'DayOfWeek': DayOfWeek },{'_id':0,'Records Affected':1,'DayOfYear':1,'WeekNbr':1,'DayOfWeek':1})
    print (' Mongo find completed')
    ticks_after_mongo = time.time()
    print ('After Mongo time' , ticks_start)
    print ("Memory ",'{:d}'.format(process.memory_info().rss))
    ExtrDf = pd.DataFrame(list(Extraction))
    print (' DataFrame Conversion completed')
    ticks_after_DataFrame = time.time()
    print ('After DataFrame time' , ticks_start)
    print (' Mongo time' , ticks_after_mongo - ticks_start)
    print (' DataFrame time' , ticks_after_DataFrame - ticks_after_mongo)
    print ("Memory ",'{:d}'.format(process.memory_info().rss))


    # building a Time Series for ARIMA later on
    TimeSerie = pd.pivot_table(ExtrDf , values = 'Records Affected', index=['WeekNbr'], aggfunc=np.sum)
    print ('Type of pivot ', type(TimeSerie))
    print(TimeSerie.shape)
    print(TimeSerie)



    print (' Start of STD' )
