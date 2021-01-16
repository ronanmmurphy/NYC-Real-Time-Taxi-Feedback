"""
Created on Tue Feb 11 13:38:57 2020

@author: Ronan Murphy 15397831; Conor Melody 15403058
Assignment 1: Data Engineering for Distributed MicroService Pipeline 

"""
import time
import stomp
import json

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
    conn.subscribe(destination='/queue/test', id=1, ack='auto')
    
    #wait if incase a message is slow to send 
    while not EXIT:
        time.sleep(60)
        
    #disconnect from queue
    conn.disconnect()
#listner class for when receives message
class MyListener(stomp.ConnectionListener):
    #method to deal with reveiving error message
    def on_error(self, headers, message):
        print('received an error "%s"' % message)
        
    #once message received will implement functionality in this message
    def on_message(self, headers, message):
        #new message list to output from this microservice
        new_message = []
        #convert String back to list value
        mess = eval(message)
        
        #if the location ID is 254 or 265 we ignore it as it is NaN
        if (mess[8] != "264" or"265" ):
            #append the Location ID and the Date and time
            new_message.append(mess[2])
            new_message.append(mess[8])
            
            #convert this new list to a json String object using 'dumps' function
            jsonData = json.dumps(new_message,  default = str)
            #publish this message to the next queue
            publish(conn, jsonData, '/queue/test2')
        #exit clause to disconnect from Activemq is recieves EXIT
        if 'exit' in message:
            
            global EXIT
            EXIT = True
        
if __name__ == '__main__':
    main()
