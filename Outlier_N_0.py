from dateutil.parser import parse
import matplotlib.pyplot as plt
# import seaborn as sns
import numpy as np
import pandas as pd

# Normality testing
from scipy.stats import normaltest
from scipy.stats import shapiro


plt.rcParams.update({'figure.figsize': (10, 7), 'figure.dpi': 120})

# Import as Dataframe

wd = '/opt/sonarg/sonarw/agg_out/'

queri = input('Enter queri number : ')
case = input('Enter case 1(working week)  or 2(sunday) : ')


df = pd.read_csv(wd + 'TS_Query_'+queri+'.'+case+'.csv', parse_dates=['Timestamp'])
print ('After Reading')
print(df.head())
print(df.shape)
data = df[['Timestamp','Records Affected']]
print (data)
print (type(data))

data=data.dropna()

print (type(data))
# ---- Distribution
print( data['Records Affected'].describe())
# data['Records Affected'].boxplot()
data.boxplot()
plt.show()
plt.savefig('BoxPlot_' + str(queri) + '.' + str(case) + '.png')

data.hist(bins=20)
plt.show()
plt.savefig('Hist_' + str(queri) + '.' + str(case) + '.png')

# normality test
print ('===========================')
print ('Normality Testing : Shapiro')
print ('---------------------------')
stat, p = shapiro(data['Records Affected'])
print('Statistics=%.3f, p=%.3f' % (stat, p))
# interpret
alpha = 0.05
if p > alpha:
        print('Sample looks Gaussian (fail to reject H0)')
else:
        print('Sample does NOT look Gaussian (reject H0)')

print('Done')
print ('=========================')


# normality test
print ('===========================')
print ("Normality Testing : D'Agostino")
print ('---------------------------')

stat, p = normaltest(data['Records Affected'])
print('Statistics=%.3f, p=%.3f' % (stat, p))
# interpret
alpha = 0.05
if p > alpha:
        print('Sample looks Gaussian (fail to reject H0)')
else:
        print('Sample does NOT look Gaussian (reject H0)')

print('Done')
print ('=========================')
