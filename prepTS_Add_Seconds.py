import pandas as pd
import csv
import numpy as np

pathToProcess = "/opt/sonarg/sonarw/agg_out/"
fileToProcess = "TS_Query_7.1.csv"
datafile = pathToProcess + fileToProcess
fileProcessed = "TS_Query_7.1_P.csv"

df=pd.read_csv(pathToProcess + fileToProcess)

# print(df.head())

# print(df[["DayOfYear","Records Affected",'Timestamp']])

dfs=df[["DayOfYear","Records Affected",'Timestamp']]
# print ("Nbr of Records", dfs.count() )
dfdf=pd.DataFrame(dfs)
# print (dfdf.head())
acount =  dfdf['Timestamp'].count()
print('acount ', acount, type(acount))

# Adding Column
# dfdf.insert(3, "TS", range(dfdf.count()) , True)
# dfdf.insert(3, "TS", [0,1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19,20,21] , True)
# dfdf.insert(3, "TS", np.arange(22) , True)
dfdf.insert(3, "TS", np.arange(acount) , True)

for index, value in dfs['Timestamp'].items():
   b = pd.Timestamp(value)
   c = b.timestamp()
   # print (c)
   dfdf['TS'][index]= c


print (dfdf.head())
datafileProcessed = pathToProcess + fileProcessed
dfdf.to_csv(datafileProcessed, index=False)
