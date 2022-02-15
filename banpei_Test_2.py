$ cat banpei_Test.py
import pandas as pd
import banpei
import numpy as np

pathToProcess = "/opt/sonarg/sonarw/agg_out/"
pathToProcess = "./"
fileToProcess = "TS_Query_10.1.csv"
fileToProcess = "Query_11_Data.csv"
fileToProcess = "Query_21_Data.csv"

# raw_data = pd.read_csv(pathToProcess + 'DailyExtract_' + fileToProcess)
raw_data = pd.read_csv(pathToProcess +  fileToProcess)
data = raw_data['Records Affected']
print ('Nbr of recs to Process : ', len(data))

banpei_nbr = 30
# banpei_nbr = 20
model = banpei.SST(w=banpei_nbr)

results = model.detect(data, is_lanczos=True)

anomalies = np.array([data , results ] )

print (anomalies.shape)

rAnomalies = np.reshape(anomalies, (len(results), 2))

print (rAnomalies.shape)
print (rAnomalies)
# print(type(results),len(results))
# print(results)

# numpy.savetxt(pathToProcess + 'Banpei_' + fileToProcess, results)

sonargd@sdcpiapplnx143 Outliers_Detection
$
