import yaml
import pandas as pd
import requests
import numpy as np
import matplotlib.pyplot as plt


# load dataframe 
df=pd.read_csv('20250409prod-combined_calc.csv',sep=";")  
df.plot(x='startedAt', y='availableSharing-Véronique', kind='line')
#df.plot()