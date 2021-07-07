import os
import json
import psutil
import getpass
import datetime as dt
import pymongo
import sys
import psycopg2
import urllib.parse
from pymongo.errors import BulkWriteError
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime
import numpy as np
import pandas as pd


class  TS_Long_Large_Query :


 def __init__(self,param_json):

#      with open(str(param_json)) as f:
#           self.param_data = json.load(f)

     with open(str(param_json)) as f:
          self.param_data = json.load(f)


     print("param_json : ", param_json )
     self.QueryNbr = input(" Query Nbr please : ")
     self.queryNbr = "#" + self.QueryNbr
     print(self.queryNbr)

     dateStart = input(" Date to Start from yyyy-mm-dd : ")
     self.date_start = dt.datetime.strptime(dateStart[:10],'%Y-%m-%d')
     # self.date_start = datetime.datetime(2020, int(monthStart) , 1, 5)
     dateEnd = input(" Date to End from yyyy-mm-dd : ")
     self.date_end = dt.datetime.strptime(dateEnd[:10],'%Y-%m-%d')
     # self.date_end = datetime.datetime(2020, int(monthStart) , 1, 5)


     self.procType = input("What data do you want to study ? 1: Extraction , 2: Count of SQLs, 3: Response Time  --  Your Answer  = ")
     print ('Type of Data to be Processed ', self.procType )

     self.Extract = {
          '_id':0,
          # 'Timestamp':1,
          'Response Time':1,
          'Records Affected':1,
          'DayOfYear':1,
          # 'WeekOfYear':1,
          'HashHash User Datastore':1,
          # 'Timestamp':1,
          'Year':1
          # 'DayOfWeek':1
          }

 def open_PostGres(self):

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
 def open_Mongo(self):

   # --- Password ----
   self.mongoUserName =  self.param_data["mongoUserName"]
   self.mongoPwd =  self.param_data["mongoPwd"]
   # ----- Connection
   username = urllib.parse.quote_plus(self.mongoUserName)
   password = urllib.parse.quote_plus(self.mongoPwd)

   myclient = pymongo.MongoClient('mongodb://%s:%s@127.0.0.1:27117/admin' % (username,password))
   return(myclient)


 def get_query(self):

    # -- Open PostGres
    postgres_connect=p1.open_PostGres()
    cursor = postgres_connect.cursor()
    print ( postgres_connect.get_dsn_parameters(),"\n")
    cursor.execute("SELECT version();")
    record = cursor.fetchone()
    print("You are connected to - ", record,"\n")
    # queryNbr = "'#" + QueryNbr +"'"
    print ( p1.queryNbr )
    select_Query = "select query from query where queryName = " + "'" + p1.queryNbr + "'"
    cursor.execute(select_Query)
    query_to_study_t  = cursor.fetchone()
    query_to_study  = query_to_study_t[0]
    print (query_to_study)
    return(query_to_study)

 def get_series(self,myclient,query_to_study):

     mydb = myclient["AIMAnalytics"]
     print("p1.date_start",p1.date_start)
     mycol = mydb["ENRICHED_FULL_SQL_PYTHON"]
 
     ToBeFoundAll = {
             'HashHash User Datastore': query_to_study ,
             'Timestamp':{'$gte': p1.date_start},
             'Timestamp':{'$lte': p1.date_end}
             }
     Nbr = mycol.find(ToBeFoundAll).count()

     print (' Nbr of MongoDB Documents to Process',Nbr)
     if Nbr == 0 :
         exit(0)

     NextStep = input(" Do you want to continue ? (Y/N) ")
     if NextStep == "N" :
        exit(0)
     Extraction = mycol.find(ToBeFoundAll, p1.Extract)
     criteria = ToBeFoundAll
     
    
     print (' Mongo find completed')

     # print ("Extraction" , Extraction)

     return(Extraction)


 def InputInflux(self,Extraction):
     # -- Insert in InfluxDB

     # You can generate a Token from the "Tokens Tab" in the UI
     token = "KAhTOJpoan3nbxiTNPST5c2ZUstTpTbfspMKBKTO0HUYP4xfwnSuIAZxW6ty5mKYmkNDN9gqQGc1k8KB5xXjOw=="
     org = "context22"
     bucket = "context22"

     client = InfluxDBClient(url="http://sdcdiapplnx9160:8086", token=token)
     write_api = client.write_api(write_options=SYNCHRONOUS)
     # measurementTagged = "Query " + p1.queryNbr + " Tagged "
     measurementTagged = p1.queryNbr 
     measurement = "Query " + p1.queryNbr

     if p1.procType == "1":
        measurement = "Query " + p1.queryNbr + " Extractions"

     if p1.procType == "2":
        measurement = "Query " + p1.queryNbr + " Count of SQLS"

     if p1.procType == "3":
        measurement = "Query " + p1.queryNbr + " Resp. Time"

     print ("Start Ingesting into InfluxDB ")
     # for extract in Extraction:
         # print ("Extract ", extract)
     for extract in Extraction:
         if extract[2] is None :
            print ("None in Extract")
            extract[2] = 0
         if int(extract[2]) > -1 :
            # Compute Date from DayOfYear
            day_num = str(extract[1])
            print (" Type = " , type(extract[0]))
            year  =  str(int(extract[0]))
            recs  =  extract[2]
            # adjusting day num
            day_num.rjust(3 + len(day_num), '0')
            # converting to date
            ts = datetime.strptime(year + "-" + day_num, "%Y-%j").strftime("%m-%d-%Y")

            # Computing DayOfWeek
            tag1 = ts.weekday()
            
            # print ("Exttracted > -1 ", recs )
            # print(tag1 , ts , recs, type(recs))
            point = Point(measurement).tag("DayOfWeek", tag1 ).field("Extracted", recs).time(ts, WritePrecision.S)
            write_api.write(bucket, org, point)



     print ("End Of  Ingesting into InfluxDB ")


 def aggreg_extract(self, ExtrDf):

      # TSExtract = pd.pivot_table(ExtrDf , values = 'Records Affected', index=['Year','DayOfYear','DayOfWeek','Timestamp'], aggfunc=[np.sum]).reset_index()
      TSExtract = pd.pivot_table(ExtrDf , values = 'Records Affected', index=['Year','DayOfYear'], aggfunc=[np.sum]).reset_index()
      TSENumPy = TSExtract.to_numpy()
      # print(TSENumPy[:])
      # exit(0)
      return(TSENumPy)

 def aggreg_countOfSqls(self, ExtrDf):
      # TSCount = pd.pivot_table(ExtrDf , values = 'Records Affected', index=['Year','DayOfYear','DayOfWeek','Timestamp'], aggfunc=len ).reset_index()
      TSCount = pd.pivot_table(ExtrDf , values = 'Records Affected', index=['Year','DayOfYear'], aggfunc=len ).reset_index()
      TSCount = TSCount.to_numpy()
      # print(TSCount[:])
      # exit(0)
      return(TSCount)
      # print(TSNumPy[:])

 def aggreg_respTime(self, ExtrDf):
      # TSResp = pd.pivot_table(ExtrDf , values = 'Response Time', index=['Year','DayOfYear','DayOfWeek','Timestamp'], aggfunc=[np.sum] ).reset_index()
      TSResp = pd.pivot_table(ExtrDf , values = 'Response Time', index=['Year','DayOfYear'], aggfunc=[np.sum] ).reset_index()
      TSResp = TSResp.to_numpy()
     
      return(TSResp)

if __name__ == '__main__':
    print("Start  Recording of Time Series - Extraction - Response Time -")


    p1 = TS_Long_Large_Query('param_data.json')

    myclient = p1.open_Mongo()

    query_to_study = p1.get_query()

    myclient.close()

    Extraction = p1.get_series(myclient,query_to_study)
    TS = pd.DataFrame(list(Extraction))
    # print("TS ts Before = " , type(TS['Timestamp']), TS['Timestamp'])
    # TS['Timestamp'] =  TS['Timestamp'][0:10]+"T00:00:00Z"
    # TS['Timestamp'] =  pd.DatetimeIndex(TS['Timestamp']).normalize()
    # print("TS After = " , TS.columns)

    if p1.procType == "1":
       TS = p1.aggreg_extract(TS)
    elif p1.procType == "2":
       TS = p1.aggreg_countOfSqls(TS)
    else:
       TS = p1.aggreg_respTime(TS)

    p1.InputInflux(TS)


