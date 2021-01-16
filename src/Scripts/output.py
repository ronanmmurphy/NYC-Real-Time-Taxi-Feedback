"""
Created on Tue Feb 11 13:38:57 2020

@author: Ronan Murphy 15397831; Conor Melody 15403058
Assignment 1: Data Engineering for Distributed MicroService Pipeline 

"""
import stomp
import time

#set EXIT message to False, test to tell when to disconnect from Activemq
EXIT = False
#initalise host and port
conn = stomp.Connection(host_and_ports=[('localhost', '61613')])

def main():
    print("Reporting on Taxi Trips in NYC....")
    print("\n")
    #assign listner if any messaage is received dequeue this message
    conn.set_listener('', MyListener())
    conn.connect(login='system', passcode='manager', wait=True)
    #subscribes to previous queue
    conn.subscribe(destination='/queue/test5', id=5, ack='auto')
    
    #wait if incase a message is slow to send 
    while not EXIT:
        time.sleep(60)
        
    #disconnect from queue
    conn.disconnect()
    print("\n")
    print("Report completed.")
    
    
class MyListener(stomp.ConnectionListener):

    def on_error(self, headers, message):
        print('received an error "%s"' % message)

    def on_message(self, headers, message):
        
        
        mess = eval(message)
        if mess[0] == "Daily Window":
            print("Daily Window: {}".format(mess[1]), "/01/2018")
            print("Total Taxi Trip Freqency in Day {}".format(mess[3]))
            #print("\n")
            print("The peak hour for the day was {}". format(mess[2]), ":00")
            print("The frequency of trips for this hour was {}".format(mess[4]))
            print("The total crashes for this hour {}".format(mess[5]))
            print("\n")
            
            
        else:
            print("Hourly Window: {}".format(mess[0]), "/01/2018")
            print("Time is {}". format(mess[1]), ":00")
            print("Total Taxi Trip Freqency in Hour {}".format(mess[2]))
            #print("\n")
            print("The Peak Location was in Zone {}".format(mess[4]), " and Borough {}".format(mess[3]))
            print("There were {}".format(mess[5]), "trips in this location at for this hour")
            print(mess[6])
            print("\n")
            
    
        if 'exit' in message:
            
            global EXIT
            EXIT = True


if __name__ == '__main__':
    main()
