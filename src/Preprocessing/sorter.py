"""
Created on Tue Feb 11 13:38:57 2020

@author: Ronan Murphy 15397831; Conor Melody 15403058
Assignment 1: Data Engineering for Distributed MicroService Pipeline 

"""
import pandas as pd
def main():
    #SCRIPT TO PRE-PROCESS THE RAW TAXI TRIP DATA
    
    #read raw csv data with pandas
    TripData = pd.read_csv("TaxiTripData.csv")
    
    #rename the columns for datetime 
    TripData = TripData.rename(columns = {"tpep_pickup_datetime": "Pickup_DateTime",
                          "tpep_dropoff_datetime": "Dropoff_DateTime"})
    
    #sort the csv by pickup datetime
    TripData = TripData.sort_values("Pickup_DateTime")
    
    #save the sorted result to a new csv file
    TripData.to_csv(r'C:\Users\ronan\Documents\AI Masters\Coursework\Research Topics\TripData.csv')
    
if __name__ == '__main__':
    main()