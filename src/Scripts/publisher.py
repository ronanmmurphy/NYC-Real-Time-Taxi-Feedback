"""
Created on Tue Feb 11 13:38:57 2020

@author: Ronan Murphy 15397831; Conor Melody 15403058
Assignment 1: Data Engineering for Distributed MicroService Pipeline 

"""
import stomp
import csv
import json

#this method sends the message to the queue assigned
def publish(conn, msg, destination):
    conn.send(body=msg, destination=destination)


def main():
    # connect stomp to local host with port 61613 for Python
    conn = stomp.Connection(host_and_ports=[('localhost', '61613')]) 
    conn.connect(login='system', passcode='manager', wait=True) 
    
    #open the sorted csv file 
    with open("TripData.csv", 'r') as readFile:
        #read CSV file skip the header row, iterating through each row
        csv_reader = csv.reader(readFile)
        next(csv_reader)
        for row in csv_reader:
            #convert the message to String using Json Object 'dumps'
            jsonData = json.dumps(row)
            #publish the queue every row creating stream of data
            publish(conn, jsonData,'/queue/test')
    #when the CSV is completed streaming publish an EXIT message 
    publish(conn, str('exit'), '/queue/test')
    
    #disconnect from the port when task completed
    conn.disconnect()
    
    
if __name__ == '__main__':
    main()
