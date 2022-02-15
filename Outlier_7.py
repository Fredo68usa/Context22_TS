$ more Outlier_7.py
from pandas import read_csv
import pandas as pd
import datetime as dt
from matplotlib import pyplot
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error

# Normality testing
from scipy.stats import normaltest
from scipy.stats import shapiro


def parser(x):
#       return datetime.strptime('190'+x, '%Y-%m')
        # return dt.datetime.strptime(x[:19], '%Y-%m-%dT%H:%M:%S')
        x=dt.datetime.strptime(x[:19], '%Y-%m-%dT%H:%M:%S')
        print(' -- x  --' , x)
        return x

wd = '/opt/sonarg/sonarw/agg_out/'

queri = input('Enter queri number : ')
case = input('Enter case 1(working week)  or 2(sunday) : ')
# No Need for Index
# series_df = read_csv(wd + 'TS_Query_'+queri+'.'+case+'.csv', index_col=1, parse_dat
es=[1],usecols = ['Timestamp','Records Affected'], date_parser=parser)
series_df = read_csv(wd + 'TS_Query_'+queri+'.'+case+'.csv', parse_dates=[1],usecols
= ['Timestamp','Records Affected'], date_parser=parser)
print('Series ',series_df.head())
forecast = []

series_df.info()
series_df.shape

# Data Cleaning
series_df_clean = series_df.dropna()
data=series_df_clean.values
print ('Nbr of X values ' , len(data))
print ('X values ' , data)
ind=series_df_clean.index
print ('Type X index ' , type(ind))
# print ('X values ' , ind)


print ('Type of data field : ', type(data))

# normality test

print('Done')
print ('=========================')



sonargd@sdcpiapplnx143 Archive
$ cat Outlier_7.py
from pandas import read_csv
import pandas as pd
import datetime as dt
from matplotlib import pyplot
from statsmodels.tsa.arima_model import ARIMA
from sklearn.metrics import mean_squared_error

# Normality testing
from scipy.stats import normaltest
from scipy.stats import shapiro


def parser(x):
#       return datetime.strptime('190'+x, '%Y-%m')
        # return dt.datetime.strptime(x[:19], '%Y-%m-%dT%H:%M:%S')
        x=dt.datetime.strptime(x[:19], '%Y-%m-%dT%H:%M:%S')
        print(' -- x  --' , x)
        return x

wd = '/opt/sonarg/sonarw/agg_out/'

queri = input('Enter queri number : ')
case = input('Enter case 1(working week)  or 2(sunday) : ')
# No Need for Index
# series_df = read_csv(wd + 'TS_Query_'+queri+'.'+case+'.csv', index_col=1, parse_dates=[1],usecols = ['Timestamp','Records Affected'], date_parser=parser)
series_df = read_csv(wd + 'TS_Query_'+queri+'.'+case+'.csv', parse_dates=[1],usecols = ['Timestamp','Records Affected'], date_parser=parser)
print('Series ',series_df.head())
forecast = []

series_df.info()
series_df.shape

# Data Cleaning
series_df_clean = series_df.dropna()
data=series_df_clean.values
print ('Nbr of X values ' , len(data))
print ('X values ' , data)
ind=series_df_clean.index
print ('Type X index ' , type(ind))
# print ('X values ' , ind)


print ('Type of data field : ', type(data))

# normality test

print('Done')
print ('=========================')



