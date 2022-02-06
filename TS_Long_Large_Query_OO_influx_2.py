$ cat TS_Long_Large_Query_OO_influx.py
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
import pandas as pd
from matplotlib import pyplot as plt
from statsmodels.tsa.seasonal import seasonal_decompose
from statsmodels.tsa.holtwinters import SimpleExpSmoothing
from statsmodels.tsa.holtwinters import ExponentialSmoothing


class  TS_Long_Large_Query :


 def __init__(self,param_json):

#      with open(str(param_json)) as f:
#           self.param_data = json.load(f)

     with open(str(param_json)) as f:
          self.param_data = json.load(f)


     print("param_json : ", param_json )
     self.QueryNbr = input(" Query Nbr please : ")
     self.queryNbr =  self.QueryNbr
     print(self.queryNbr)

     self.query_to_study = None

     self.Start = int(input(" Week to Start From  : "))
     self.End = int(input(" Week to End to (included) : "))

     self.Extraction = None

     self.procType = input("What data do you want to study ? 1: Extraction , 2: Count of SQLs, 3: Response Time  --  Your Answer  = ")
     print ('Type of Data to be Processed ', self.procType )


     self.processType = input("What Processing  ? 1: InfluxDBOnly , 2: Predict Gen Only, 3: Both  --  Your Answer  = ")
     # Display ...
     self.Extract = {
          '_id':0,
          'Timestamp':1,
          'Response Time':1,
          'Records Affected':1,
          'DayOfYear':1,
          'WeekOfYear':1,
          'HashHash User Datastore':1,
          'Timestamp':1,
          'Year':1,
          'DayOfWeek':1
          }


     self.groupip = {
       "$group" :  { "_id" :
                   { "Day of Year" : "$DayOfYear" } ,
               "From" : { "$min" : "$Timestamp" },
               "To" : { "$max" : "$Timestamp" },
               "DayOfWeek" : { "$max" : "$DayOfWeek" },
               "Extract" : { "$sum" : "$Records Affected" },
               "Resp Time" : { "$sum" : "$Response Time" },
               "CountOfSQLs" : { "$sum" : 1 }}
       }

     self.project = {
        "$project" : {
        "_id" : 0,
        "Server Host Name" : "$_id.Day Of Year",
        "From" : "From",
        "count" : 1,
        "_sum_Records Affected" : 1,
        "_min_Timestamp" : 1
        }
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
    print ( p1.queryNbr , self.queryNbr)
    select_Query = "select query from topquery where querynbr = " + self.queryNbr
    cursor.execute(select_Query)
    query_to_study_t  = cursor.fetchone()
    print (query_to_study_t)
    if query_to_study_t is None:
       print (" Query doesn't exist")
       exit(0)
    else:
       p1.query_to_study  = query_to_study_t[0]
       print (p1.query_to_study)
    # return(query_to_study)

 def get_series(self,myclient,query_to_study):

     mydb = myclient["AIMAnalytics"]
     print("p1.Start",p1.Start)
     mycol = mydb["ENRICHED_FULL_SQL_PYTHON"]

     ToBeFoundAll = { "$and" : [
              {'HashHash User Datastore': p1.query_to_study}  ,
              {'WeekOfYear':{'$gte': p1.Start}} ,
              {'WeekOfYear':{'$lte': p1.End}}
             ] }


     print ("ToBeFoundAll" , ToBeFoundAll)

     Nbr = mycol.find(ToBeFoundAll).count()

     print (' Nbr of MongoDB Documents to Process',Nbr)
     if Nbr == 0 :
         exit(0)

     NextStep = input(" Do you want to continue ? (Y/N) ")
     if NextStep == "N" :
        exit(0)

     # ToBeFoundAllAgg = [ { "$match" : ToBeFoundAll } , p1.groupip ]
     ToBeFoundAllAgg = [ { "$match" : ToBeFoundAll } , p1.groupip , p1.project]

     # print (ToBeFoundAllAgg)

     self.Extraction = list(mycol.aggregate(ToBeFoundAllAgg))

     # extract = list(self.Extraction)
     # print('extract' , list(extract))


     print (' Mongo find completed')


 def InputInflux(self):
     # -- Insert in InfluxDB

     # You can generate a Token from the "Tokens Tab" in the UI
     token = "IA7lu0jF4KkUEGs1QFi04hvIkuBs1ewUT6XWwdKbELbJ5FPKpBcHxbUbp-MjJKbcu-6H8JXI2bjz_bBd1jTmlw=="
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

     if p1.processType == "1" or p1.processType == "3":
        p1.InfluxIngest(measurement)

     if p1.processType == "2" or p1.processType == "3":
        p1.GenPredict(measurement)


 def InfluxIngest (self,measurement):

     print ("Start Ingesting into InfluxDB ")
     # for extract in Extraction:
     # print ("Type of p1.Extraction" , type(self.Extraction))
     for pointTemp in self.Extraction:
         # print ("pointTemp" , pointTemp)
         tag1 = pointTemp["DayOfWeek"]
         if p1.procType == "1":
            recs = pointTemp["Extract"]
            # print ("recs " , recs )
         if p1.procType == "2":
            recs = pointTemp["CountOfSQLs"]
         if p1.procType == "3":
            recs = pointTemp["Resp Time"]
         ts = pointTemp["To"]
         point = Point(measurement).tag("DayOfWeek", tag1 ).field("Extracted", recs).time(ts, WritePrecision.S)
         # print (point)
         write_api.write(bucket, org, point)


 def GenPredict (self,measurement):
     print ("Start Generation of Predictions ")
     print (type(self.Extraction))

     TS=[]
     for pointTemp in self.Extraction:
         string = str(pointTemp).replace("'", '"')
         print (pointTemp)
         print (string)
         a_json = json.loads(string)
         tag1 = pointTemp["DayOfWeek"]
         if p1.procType == "1":
            recs = pointTemp["Extract"]
            wks = pointTemp["_id"]
            TS.append(recs)
            # TS[1].append(doy)
         if p1.procType == "2":
            recs = pointTemp["CountOfSQLs"]
         if p1.procType == "3":
            recs = pointTemp["Resp Time"]
         ts = pointTemp["To"]

     TSDF = pd.DataFrame.from_dict(TS, columns=['Extract'])
     print (TSDF)

     decompose_result = seasonal_decompose(TSDF['Extract'], model='multiplicative')
     decompose_result.plot()

     print ("End Of  Ingesting into InfluxDB ")



if __name__ == '__main__':
    print("Start  Recording of Time Series - Extraction - Response Time -")

    p1 = TS_Long_Large_Query('param_data.json')

    myclient = p1.open_Mongo()

    query_to_study = p1.get_query()

    myclient.close()

    p1.get_series(myclient,query_to_study)

    p1.InputInflux()

    print("End of Recording of Time Series - Extraction - Response Time -")

