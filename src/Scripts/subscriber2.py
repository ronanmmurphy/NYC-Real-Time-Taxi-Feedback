"""
Created on Tue Feb 11 13:38:57 2020

@author: Ronan Murphy 15397831; Conor Melody 15403058
Assignment 1: Data Engineering for Distributed MicroService Pipeline 

"""

import time
import stomp
import json
from datetime import datetime
import operator

#set EXIT message to False, test to tell when to disconnect from Activemq
EXIT = False
#initalise host and port
conn = stomp.Connection(host_and_ports=[('localhost', '61613')])

#same publish method as in previous script sends message to assigned queue
def publish(conn, msg, destination):
    conn.send(body=msg, destination=destination)
    
    

def main():
    #assign listner if any messaage is received dequeue this message
    conn.set_listener('', MyListener())
    conn.connect(login='system', passcode='manager', wait=True)
    #subscribes to previous queue
    conn.subscribe(destination='/queue/test2', id=2, ack='auto')
    
    #wait if incase a message is slow to send 
    while not EXIT:
        time.sleep(60)
        
    #disconnect from queue
    conn.disconnect()
    
class MyListener(stomp.ConnectionListener):
    #initialise global hour (j) and day (i) counter variables to 0 and 1 repectively
    global i
    global j
    i = 1
    j=0
    
    #initalise global hourly and daily window lists 
    global hourwindow
    global daywindow 
    hourwindow =[]
    daywindow =[]
    
    
    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        #get value of the message
        data =eval(message) 
        #convert the date and time String to Date-Time Object 
        #makes it easier to implement tumbling window
        date = datetime.strptime(data[0], '%d/%m/%Y %H:%M')
        
        print(date.day, date.hour)
        print(date.year, date.month)
        #filters dates to only perform windowing for January 2018
        if date.year == 2018:
            if date.month == 1:
                #call the global i,j variables and window lists
                global i
                global j
                global daywindow
                global hourwindow
                
                
                print("Current day and hour ",date.day, date.hour)
                print("i and j values", i, j)
                
                #append all taxi trips in current day and hour to hourly window
                if date.day == i and date.hour == j:
                    hourwindow.append(data[1])
                    
                #if the hour changes implement following
                if date.hour != j:
                    print("hour is diff")
                    
                    result = {}
                    
                    #this is the freqency of taxi trips within this hour
                    hourfreq = len(hourwindow)
                    
                    #this loop returns dictionary with count for trips in that
                    #that location in that hour
                    for locations in hourwindow:
                        result[locations] = result.get(locations, 0)+1
                    
                    #this finds the peak loction within the hour 
                    peaklocation = max(result.items(), key=operator.itemgetter(1))[0]
    
                    #outputs the freq at the peak hour
                    freqatpeak = result[peaklocation]
                    
                    #empty the window and add analytics from the window
                    #this includes current day and hour
                    hourwindow = []
                    hourwindow.append(hourfreq)
                    hourwindow.append(peaklocation)
                    hourwindow.append(freqatpeak)
                    hourwindow.append(i)
                    hourwindow.append(j)
                    hourwindow.append("Hourly Window")
                    
                    #append the hourly window to the daily window
                    daywindow.append(hourwindow)
                    
                    #convert hourlywindow to Json string using 'dumps' function
                    #publish this to the next queue
                    jsonData = json.dumps(hourwindow,  default = str)
                    publish(conn, jsonData,  '/queue/test3')
                    
                    #clear the windo and append piece of data in next hour
                    hourwindow =[]
                    hourwindow.append(data[1])
                    
                    #change j count to new hour value
                    j = date.hour
                    
                    
                if date.day != i:
                    print("day is diff")
                    
                    dayfreq = 0
                    # this will return the peak hour frequency of the day
                    peakhourvalue = max(map(lambda x: x[0], daywindow))
                    #this is the hour of the peak hour frequnecy of the day
                    peakhour = max(daywindow, key =lambda y : y[0])[4]
                    
                    #peakhourvalue = max(map(lambda x: x[2], daywindow))
                    #locID = max(daywindow, key =lambda y : y[2])[1]
                    #peakhour = max(daywindow, key =lambda y : y[2])[4]
                    
                    #calculate the total frequency of trips for the day
                    for windows in daywindow:
                        dayfreq += windows[0]
                    
                    #append the analytics to the daily window
                    daywindow = []
                    daywindow.append("Daily Window")
                    daywindow.append(i)
                    daywindow.append(peakhour)
                    daywindow.append(dayfreq)
                    daywindow.append(peakhourvalue)
                    # convert the daily list to json String using 'dumps'
                    jsonData1 = json.dumps(daywindow,  default = str)
                    #publish this to next queue
                    publish(conn, jsonData1, '/queue/test4')
                    
                    #clear the window and assign i and j counters to current hour and day
                    daywindow = []
                    j =date.hour
                    i = date.day
        
        if 'exit' in message:
            #for the last hour of the last day in January collect the final day and hour windows
            
            result = {}
            print("Last hour")
            
            #this is the freqency of taxi trips within this hour
            hourfreq = len(hourwindow)
                    
            #this loop returns dictionary with count for trips in that
            #that location in that hour
            for locations in hourwindow:
                result[locations] = result.get(locations, 0)+1
                    
            #this finds the peak loction within the hour 
            peaklocation = max(result.items(), key=operator.itemgetter(1))[0]
    
            #outputs the freq at the peak hour
            freqatpeak = result[peaklocation]
                    
            #empty the window and add analytics from the window
            #this includes current day and hour
            hourwindow = []
            hourwindow.append(hourfreq)
            hourwindow.append(peaklocation)
            hourwindow.append(freqatpeak)
            hourwindow.append(i)
            hourwindow.append(j)
            hourwindow.append("Hourly Window")
                    
            #append the hourly window to the daily window
            daywindow.append(hourwindow)
                    
            #convert hourlywindow to Json string using 'dumps' function
            #publish this to the next queue
            jsonData = json.dumps(hourwindow,  default = str)
            publish(conn, jsonData,  '/queue/test3')
            
            
            dayfreq = 0
            # this will return the peak hour frequency of the day
            peakhourvalue = max(map(lambda x: x[0], daywindow))
            #this is the hour of the peak hour frequnecy of the day
            peakhour = max(daywindow, key =lambda y : y[0])[4]
            
            #peakhourvalue = max(map(lambda x: x[2], daywindow))
            #locID = max(daywindow, key =lambda y : y[2])[1]
            #peakhour = max(daywindow, key =lambda y : y[2])[4]
                    
            #calculate the total frequency of trips for the day
            for windows in daywindow:
                dayfreq += windows[0]
                    
            print("Last Day")
                
            #append the analytics to the daily window
            daywindow = []
            daywindow.append("Daily Window")
            daywindow.append(i)
            daywindow.append(peakhour)
            daywindow.append(dayfreq)
            daywindow.append(peakhourvalue)
            # convert the daily list to json String using 'dumps'
            jsonData1 = json.dumps(daywindow,  default = str)
            #publish this to next queue
            publish(conn, jsonData1, '/queue/test4')
            
            #change EXIT message to assign end of stream
            global EXIT
            EXIT = True
        
        
if __name__ == '__main__':
    main()
