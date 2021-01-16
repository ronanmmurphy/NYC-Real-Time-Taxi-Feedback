"""
Created on Tue Feb 11 13:38:57 2020

@author: Ronan Murphy 15397831; Conor Melody 15403058
Assignment 1: Data Engineering for Distributed MicroService Pipeline 

"""
import stomp
import pandas as pd
import csv
import json
from datetime import datetime
import time

#set EXIT message to False, test to tell when to disconnect from Activemq
EXIT = False
#initalise host and port
conn = stomp.Connection(host_and_ports=[('localhost', '61613')])

#same publish method as in previous script sends message to assigned queue
def publish(conn, msg, destination):
    conn.send(body=msg, destination=destination)
    
def main():
    #this method reads in the sorted crash data for the month of January 
    with open("Filtered_CrashData_v3.csv", 'r') as readFile:
        csv_reader = csv.reader(readFile)
        next(csv_reader) #This removes the row containing the column titles
        JanuaryCrash_DateLoc_data = []
        #iterate through rows in CSV and convert time to Date-Time object
        for row in csv_reader:
                DateTime_Obj = datetime.strptime(row[1], '%Y-%m-%d %H:%M:%S')
                Crash_DateLoc = [DateTime_Obj.day, DateTime_Obj.hour , row[2]]
                JanuaryCrash_DateLoc_data.append(Crash_DateLoc)
           
        #add day, time and location to pandas dateframe
        Crashdf = pd.DataFrame(JanuaryCrash_DateLoc_data ,columns=['Crash_Day', 'Crash_Time', 'CrashLocation'])


        i = 1 #Initial day Value
        j = 0 #Initial hour Value

        #convert the values in the datafrane to a list 
        crashes_list = Crashdf.values.tolist()
        #keep index to decide when to close last window
        currentindex = 0
        length = len(crashes_list) #Total number of crashes over 5 days
        everycrashincident = []
        #enter dictionary for boroughs assigning counts to zero
        crashes_dict = {'BROOKLYN' :0, 'QUEENS':0, 'STATEN ISLAND':0, 'MANHATTAN':0, 'BRONX':0}
        
        #iterate through crashes 
        for crash in crashes_list:
            #if the crash is in current day and hour increment count for location
            if crash[0] == i and crash[1] == j:
                crashes_dict[crash[2]] = crashes_dict.get(crash[2], 0) + 1
                currentindex += 1

            #if next hour output this list and count of crashes in each Borough
            if crash[0] == i and crash[1] != j:
                
                currentindex+=1
       
                crashes_dictASLIST = list(crashes_dict.items())
                crashes_dictASLIST.extend([i,j])
       
       
                everycrashincident.append(crashes_dictASLIST)
                crashes_dict = {'BROOKLYN' :0, 'QUEENS':0, 'STATEN ISLAND':0, 'MANHATTAN':0, 'BRONX':0}
                crashes_dict[crash[2]] = crashes_dict.get(crash[2], 0) + 1
       
                j = crash[1] #Reset the hour to the current crash hour
                
            #if there is a new day output the daily window for the crashes 
            if crash[0] != i:
       
                crashes_dictASLIST = list(crashes_dict.items())
                crashes_dictASLIST.extend([i,j]) #This is the last hour of each day
                everycrashincident.append(crashes_dictASLIST)
            
            
       
                i = crash[0]
                j = 0
       
                currentindex+=1
   
            #for the last window output daily and hourly windows
            if currentindex == length:
       
                crashes_dictASLIST = list(crashes_dict.items())
                crashes_dictASLIST.extend([i,j])
                everycrashincident.append(crashes_dictASLIST)
        #call global dataframe and add the data to each row, columns are Boroughs
        global df
        df = pd.DataFrame(everycrashincident, columns =["BROOKLYN", "QUEENS", "STATEN ISLAND", "MANHATTAN", "BRONX", "Day", "Hour"])
    
    
    #assign listner if any messaage is received dequeue this message
    conn.set_listener('', MyListener())
    conn.connect(login='system', passcode='manager', wait=True)
    #subscribes to previous queue
    conn.subscribe(destination='/queue/test4', id=4, ack='auto')
    
    #wait if incase a message is slow to send 
    while not EXIT:
        time.sleep(60)
        
    #disconnect from queue
    conn.disconnect()

    
class MyListener(stomp.ConnectionListener):
    def _init_(self,conn, df):
        self.df = df
        
    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        
        mess = eval(message)
        print(mess)
        if mess[0]== "Daily Window":
            day = mess[1] #Selecting day element from tumbling window output
            hour = mess[2] #Selecting hour element from tumbling window output
            
            #get crashes in peak hour 
            
            
            row_index = ((df[(df['Day'] == day) & (df['Hour'] == hour)].index))[0] #Need [0] here to access necessary element of object
            #print("row_index", row_index)
            df1 = df.loc[[row_index]] #This row is now a sub -DataFrame
            list3 = df1.values.tolist() # Converting row values to a list within a list

                
            l3 = []
            for element in list3: #This for loop extracts the one list from within the larger list
                for x in element:
                    l3.append(x)


            del l3[-1] #  Removes the Hour
            del l3[-1] #Removes the day
            crashes_forthishour = []
            for element in l3:
                crashes_forthishour.append(element[1])
   
            total_crashesinhour = sum(crashes_forthishour) #Total sum of crashes for this hour
            mess.extend([total_crashesinhour])
        else:
            day = mess[0] #Selecting day element from tumbling window output
            hour = mess[1] #Selecting hour element from tumbling window output
            loc = mess[3] #Selecting location element from tumbling window output
   
        
            row_index = (df[(df['Day'] == day) & (df['Hour'] == hour)].index)[0] #Need [0] here to access necessary element of object
            column_index = df.columns.get_loc(loc)
            numberofcrashes = (df.iloc[row_index, column_index][1])
            s = "Number of crashes in this hour at this location = {}".format(numberofcrashes)
            mess.extend([s])
        jsonData = json.dumps(mess,  default = str)
        publish(conn, jsonData, '/queue/test5')
        
        
        if 'exit' in message:
            
            global EXIT
            EXIT = True




if __name__ == '__main__':
    main()