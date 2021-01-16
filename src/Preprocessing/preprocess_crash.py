"""
Created on Tue Feb 11 13:38:57 2020

@author: Ronan Murphy 15397831; Conor Melody 15403058
Assignment 1: Data Engineering for Distributed MicroService Pipeline 

"""

import pandas as pd

CrashData = pd.read_csv("Motor_Vehicle_Collisions_-_Crashes.csv")

CrashData = CrashData.rename(columns = {"CRASH DATE": "Crash_Date",
                          "CRASH TIME": "Crash_Time"})

CrashData = CrashData.drop(['ZIP CODE', 'LATITUDE', 'LONGITUDE', 'LOCATION', 'ON STREET NAME', 'NUMBER OF PERSONS INJURED', 'NUMBER OF PERSONS KILLED',
'NUMBER OF PEDESTRIANS INJURED','NUMBER OF PEDESTRIANS KILLED', 'NUMBER OF CYCLIST INJURED', 'NUMBER OF CYCLIST KILLED',
'NUMBER OF MOTORIST INJURED', 'NUMBER OF MOTORIST KILLED', 'CONTRIBUTING FACTOR VEHICLE 1', 'CONTRIBUTING FACTOR VEHICLE 2',
'CONTRIBUTING FACTOR VEHICLE 3', 'CONTRIBUTING FACTOR VEHICLE 4', 'CONTRIBUTING FACTOR VEHICLE 5',
'COLLISION_ID', 'VEHICLE TYPE CODE 1', 'VEHICLE TYPE CODE 2', 'VEHICLE TYPE CODE 3', 'VEHICLE TYPE CODE 4',
'VEHICLE TYPE CODE 5', 'CROSS STREET NAME', 'OFF STREET NAME'], axis = 1)

CrashData["Crash_DateTime"] = CrashData["Crash_Date"] + [" "] + CrashData["Crash_Time"] #Combining Date and Time columns

CrashData = CrashData.reindex(columns= ['Crash_DateTime', 'Crash_Date', 'Crash_Time','BOROUGH'])
CrashData = CrashData.drop(['Crash_Date', 'Crash_Time'], axis = 1)
CrashData['Crash_DateTime'] = pd.to_datetime(CrashData['Crash_DateTime']) # Converting to DateTime objects so as to filter by year
CrashData = CrashData.sort_values("Crash_DateTime") #Sorting DataFrame by DateTime


CrashData = CrashData[CrashData["BOROUGH"].notna()] #Adding elements which are not NaN (Not a Number) values from borough column

#outputs the days of January 2018 which we have taxi trip data for
CrashData = CrashData[CrashData['Crash_DateTime'].dt.year == 2018]
CrashData = CrashData[CrashData['Crash_DateTime'].dt.month == 1]
CrashData = CrashData[CrashData['Crash_DateTime'].dt.day < 6]

#save data to new CSV file 
CrashData.to_csv(r'C:\Users\conor\Documents\ResearchTopicsInAI\Filtered_CrashData_v3.csv')

