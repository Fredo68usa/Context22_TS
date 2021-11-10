$ cat TS_Long_Large_Query_OO.py
import os
import psutil
import getpass
import datetime as dt
import pymongo
import sys
import psycopg2
import urllib.parse
from pymongo.errors import BulkWriteError


class  TS_Long_Large_Query :

  

 def __init__(self):

     self.process = psutil.Process(os.getpid())
     # self.WOY = int(str(sys.argv[1]))

     self.QueryNbr = input(" Query Nbr please : ")
     self.queryNbr = "'#" + self.QueryNbr +"'"
     print(self.queryNbr)

     dateStart = input(" Date to Start from yyyy-mm-dd : ")
     self.date_start = dt.datetime.strptime(dateStart[:10],'%Y-%m-%d')
     # self.date_start = datetime.datetime(2020, int(monthStart) , 1, 5)
     dateEnd = input(" Date to End from yyyy-mm-dd : ")
     self.date_end = dt.datetime.strptime(dateEnd[:10],'%Y-%m-%d')
     # self.date_end = datetime.datetime(2020, int(monthStart) , 1, 5)


     self.dayOfWeek = input("Day Of Week Monday-Tuesday .... etc ...., Just return if all ....")
     print ('DayOfWeek', self.dayOfWeek )

     self.Extract = {
          '_id':0,
          'Timestamp':1,
          'Response Time':1,
          'Records Affected':1,
          'DayOfYear':1,
          'WeekOfYear':1,
          'HashHash User Datastore':1,
          'Timestamp':1,
          'DayOfWeek':1
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
    select_Query = "select query from query where queryName = " + p1.queryNbr
    cursor.execute(select_Query)
    query_to_study_t  = cursor.fetchone()
    query_to_study  = query_to_study_t[0]
    print (query_to_study)
    return(query_to_study)

 def get_series(self,myclient,query_to_study):

     mydb = myclient["AIMAnalytics"]


     if int(p1.QueryNbr) < 104:
           mycol = mydb["ENRICHED_FULL_SQL_FINAL"]
     else:
           mycol = mydb["ENRICHED_FULL_SQL_PYTHON"]

     if p1.dayOfWeek == ""   :
          ToBeFoundAll = {
             'HashHash User Datastore': query_to_study ,
             'Timestamp':{'$gte': p1.date_start},
             'Timestamp':{'$lte': p1.date_end}
             }
          Nbr = mycol.find(ToBeFoundAll).count()
     else:
          # DayOfWeek = p1.dayOfWeek
          # print ('Day Of Week ', DayOfWeek )
          ToBeFoundWeek = {
             'HashHash User Datastore': query_to_study ,
             'Timestamp':{'$gte': p1.date_start},
             'Timestamp':{'$lte': p1.date_end},
             'DayOfWeek': p1.dayOfWeek
             }
          Nbr = mycol.find(ToBeFoundWeek).count()

     print (' Nbr of Documents to Process',Nbr)
     if Nbr == 0 :
         exit(0)

     NextStep = input(" Do you want to continue ? (Y/N) ")
     if NextStep == "N" :
        exit(0)
     if p1.dayOfWeek == "" :
        Extraction = mycol.find(ToBeFoundAll, p1.Extract)
        criteria = ToBeFoundAll
     else:
        Extraction = mycol.find(ToBeFoundWeek, p1.Extract )
        criteria = ToBeFoundWeek
     print (' Mongo find completed')


     # -- Insert
     try:
         mydb = myclient["AIM_QUERIES_ANALYSIS"]
         mycol = mydb["AIM_LONG_QUERIES"]
         Deletion = mycol.delete_many(criteria)
      #  x = mycol.delete_many({'HashHash User Datastore': query_to_study})
         x = mycol.insert_many(Extraction)
         print('Insert Succeeded ')
     except BulkWriteError as bwe:
         print('Insert FAILED ')
         print(bwe.details)
         pass

if __name__ == '__main__':
    print("Start  Recording of Time Series - Extraction - Response Time -")


    p1 = TS_Long_Large_Query()

    myclient = p1.open_Mongo()

    query_to_study = p1.get_query()

    p1.get_series(myclient,query_to_study)



(env)
sonargd@sdcpiapplnx143 Time_Series
$
