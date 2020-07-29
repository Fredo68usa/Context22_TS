from pandas import read_csv
import datetime as dt
from matplotlib import pyplot
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error
import numpy as np
import math


def parser(x):
          print ( ' x = ' , ' - ', x )
          date1=dt.datetime.strptime(x[:19], '%Y-%m-%dT%H:%M:%S')
          # print (date1)
          # date2=date1.timestamp()
          # print (date1,date2)
          return (date1)
        # return dt.datetime.strptime(x[:19], '%Y-%m-%dT%H:%M:%S')

# wd = '/opt/sonarg/sonarw/agg_out/'
wd = './'

queri = input('Enter queri number : ')
case = input('Enter case 1(working week)  or 2(sunday) : ')

series = read_csv(wd + 'TS_Query_' + queri + '.' + case +'.csv', index_col=1, parse_dates=[1],usecols = ['Timestamp','Records Affected'], date_parser=parser)
series.sort_values(by=['Timestamp'], inplace=True)

print('Series ',series.head())
print(series.shape)

# Data Cleaning
series_clean = series.dropna()
X=series_clean.values
print ('Null ' , series_clean.isnull().sum())


size = int(len(X) * 0.66)
train, test = X[0:size], X[size:len(X)]
history = [ x for x in train]
predictions = list()

# print ('Predictions :' , predictions)

for t in range(len(test)):
    model = ARIMA(history, order=(5,2,0))
    model_fit = model.fit(disp=0)
    output = model_fit.forecast()
    yhat = output[0]
    predictions.append(yhat)
    obs = test[t]
    history.append(obs)
    delta = yhat-obs
    percent = (delta/obs)/100
    print ('predicted=%f, expected=%f , delta= %f , percent= %f' % (yhat, obs,delta, percent))
    # print (len(predictions), "" , len(history))


error = mean_squared_error(test, predictions)
print ('=========================')
print('Test MSE: %.3f' % error)

# Plot
pyplot.plot(test)
pyplot.plot(predictions, color='red')
pyplot.show()
pyplot.savefig('Predictions'+ queri + '.' + case +'.png')

print ('=========================')
print('Done')
print ('=========================')
