import pandas as pd
import banpei
import numpy as np

pathToProcess = "/opt/sonarg/sonarw/agg_out/"
fileToProcess = "TS_Query_10.1.csv"

raw_data = pd.read_csv(pathToProcess + 'DailyExtract_' + fileToProcess)
data = raw_data['Records Affected']
print ('Nbr of recs to Process : ', len(data))
model = banpei.SST(w=30)

results = model.detect(data, is_lanczos=True)

anomalies = np.array([data , results ] )

print (anomalies.shape)

rAnomalies = np.reshape(anomalies, (len(results), 2))

print (rAnomalies.shape)
print (rAnomalies)
# print(type(results),len(results))
# print(results)

# numpy.savetxt(pathToProcess + 'Banpei_' + fileToProcess, results)
