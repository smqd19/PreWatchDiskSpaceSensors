import pandas as pd
import numpy as np
import tensorflow
import datetime
from datetime import date
import os
import warnings
import math
from tensorflow.keras.models import load_model
warnings.filterwarnings('ignore')
from sklearn.preprocessing import MinMaxScaler
import statsmodels.api as sm
from numpy import array
from statistics import mean,stdev
from src.dbservice.dbservicefactory import DbServiceFactory
from statistics import mean,stdev
from src.levels import Levels
import json
import logging
from statsmodels.tsa.ar_model import AutoReg

# Each new class must be named Plugin  
class Plugin:

    def __init__(self, dbservicefactory):
        # Initialization of sensor values
        self.dbservice = dbservicefactory.get_db_service()
        self.value=0
        self.m=0
        self.list1 = []
        self.result=0
        self.s=0
        self.r1=0
        self.r2=0
        self.sum=0
        self.query = {
            "name": "MYSQL SERVICE STATUS PRODDB VM"
        }
        self.payload = {
            "value": 0,
            "state": "NORMAL",
            "mean" : 0,
            "stdev": 0,
            "expected value":" "
            }
        self.data_historical = self.dbservice.execute_query("historicalsignaldatapoints",self.query) #dataset2
        self.data_current = self.dbservice.execute_query("currentsignaldatapoints",self.query)
        self.value = round(float(self.data_current['payload'][0]['data'][0][0]['value'])) #Current Value for comparison
        self.payload['value']=self.value
        self.start_date = date.today()
        self.data_historical['datetime'] = self.data_historical['__createdAt'].apply(lambda x: date(x.year,x.month,x.day))
        
        for i in range(len(self.data_historical['payload'])):
            try:
                self.list1.append(float(self.data_historical['payload'][i]['data'][0][0]['value'])) #Append to list for mean and stdev
            except KeyError:
                pass
        self.X_train = self.list1
        model = AutoReg(self.X_train,lags=180).fit()
        self.pred_future = model.predict(start=len(self.list1)+1,end=len(self.list1)+10080) #7days
        self.s =  round(stdev(self.list1),4)
        # For Future Dates
        self.m = round(mean(self.pred_future),4) # Mean of the values of historical data for the sensor
        self.sum = round(self.m-self.s-self.s,4)
        self.r1 = str(self.sum)
        self.sum = 0
        self.sum = round(self.m+self.s+self.s,4)
        self.r2 = str(self.sum)
        self.exp = self.r1 +" - "+ self.r2
        self.payload['expected value']= self.exp
        self.payload['stdev']=self.s
        self.result = self.m+self.s+self.s

    #define this function to return the name of the signal (this will appear on the oversight UI)
    def get_name(self):
        return "PREDICTIVE MYSQL SERVICE STATUS PRODDB VM"

    #define this function to return the interval that this signal must run
    def get_interval(self):
        return 60 #runs every 60 seconds or 1 minute

    #do all the processing here (do not rename this function)
    def process(self):

        #For more help with queries visit: https://www.w3schools.com/python/python_mongodb_query.asp

        #Fetch data into the dataframe
        try:
            if(self.start_date+datetime.timedelta(days=7)==date.today()):
                self.start_date = date.today()
                self.list1.clear()
                self.data_historical = self.dbservice.execute_query("historicalsignaldatapoints",self.query)
                self.data_historical['datetime'] = self.data_historical['__createdAt'].apply(lambda x: date(x.year,x.month,x.day))    
                for i in range(len(self.data_historical)):
                    try:
                        if(date.today()-datetime.timedelta(days=30)<=self.data_historical['datetime'][i]): #30 days old data only
                            self.list1.append(float(self.data_historical['payload'][i]['data'][0][0]['value'])) #Append to list for mean and stdev
                    except KeyError:
                        pass
                self.X_train = self.list1
                model = AutoReg(self.X_train,lags=180).fit()
                self.pred_future = model.predict(start=len(self.list1)+1,end=len(self.list1)+10080) #7days
                self.s =  round(stdev(self.list1),4)
                # For Future Dates
                self.m = round(mean(self.pred_future),4) # Mean of the values of historical data for the sensor
                self.sum = round(self.m-self.s-self.s,4)
                self.r1 = str(self.sum)
                self.sum = 0
                self.sum = round(self.m+self.s+self.s,4)
                self.r2 = str(self.sum)
                self.exp = self.r1 +" - "+ self.r2
                self.payload['expected value']= self.exp
                self.payload['stdev']=self.s
                self.result = self.m+self.s+self.s
                
            else:
                self.data_current = self.dbservice.execute_query("currentsignaldatapoints",self.query)
                self.value = float(self.data_current['payload'][0]['data'][0][0]['value']) #Current Value
                            
        except Exception as e:
            logging.error("Unable to execute query:",e)
        if self.value >= self.m+self.s and self.value < self.result:
            self.payload['value']=self.value
            self.payload['state']="WARNING"
            self.payload['mean']=self.m
            self.payload['stdev']=self.s
            self.payload['expected value']=self.exp
            return Levels.WARN,self.payload#, self.payload #Warning
        elif self.value >= self.result:
            self.payload['value']=self.value
            self.payload['state']="ALARMED"
            self.payload['mean']=self.m
            self.payload['stdev']=self.s
            self.payload['expected value']=self.exp
            return Levels.ALARM,self.payload #Alarmed
        else:
            self.payload['value']=self.value
            self.payload['state']="NORMAL"
            self.payload['mean']=self.m
            self.payload['stdev']=self.s
            self.payload['expected value']=self.exp
            return Levels.NORMAL,self.payload #Normal