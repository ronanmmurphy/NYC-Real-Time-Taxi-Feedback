"""
Created on Tue Feb 11 13:38:57 2020

@author: Ronan Murphy 15397831; Conor Melody 15403058
Assignment 1: Data Engineering for Distributed MicroService Pipeline 

"""
import stomp
import json
import csv
import time

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
    conn.subscribe(destination='/queue/test3', id=3, ack='auto')
    
    #wait if incase a message is slow to send 
    while not EXIT:
        time.sleep(60)
        
    #disconnect from queue
    conn.disconnect()
    
    
class MyListener(stomp.ConnectionListener):

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        
        new_message = []
        mess = eval(message)
        
        #call the locaiton lookup method to convert ID
        loc = lookup_location(mess[1])
        """
        if (mess[0] == "Daily Window"):
            new_message.append(mess[0])#daily window
            new_message.append(mess[4])#day
            new_message.append(mess[3])# peak hour
            new_message.append(mess[1]) # freq for day
            new_message.append(mess[2])# freq at peak hour
        """
        new_message.append(mess[3])#day
        new_message.append(mess[4])#hour
        new_message.append(mess[0])#freq of trips
        new_message.append(loc[0])#borroughs
        new_message.append(loc[1])#zone
        new_message.append(mess[2])#freq at peak borrough
        
        jsonData = json.dumps(new_message,  default = str)
        
        publish(conn, jsonData, '/queue/test4')
        
        if 'exit' in message:
            
            global EXIT
            EXIT = True
            
# function to Convert Location ID to Borough and Zone
def lookup_location(id):
    print(id)
    ID =eval(id)
    Location = []
    #open the CSV file and iterate through until it finds the ID
    #Output the Borough and zone and change to uppercase for comparison with crash data
    with open("TaxiZones.csv", 'r') as readFile:
        
        csv_reader = csv.reader(readFile)
        next(csv_reader)
        for row in csv_reader:
            title = eval(row[0])
            if title == ID:
                
                Location.append((row[1]).upper())
                Location.append((row[2]).upper())
        
                
    return Location



if __name__ == '__main__':
    main()
