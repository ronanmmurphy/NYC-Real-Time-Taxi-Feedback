# NYC-Real-Time-Taxi-Feedback
Created Micro-Services pipeline which processed, wrangled, and transformed traffic and incident information in order to provide Real-Time feedback to taxi drivers in NYC about the optimal route to take. This scalable pipeline was created using Python with service-oriented design principles. Apache ActiveMQ is the messaging service that transmitted information between Micro-Services. Updates of busiest locations, peak times, accidents, and the number of taxi trips at a location were provided through tumbling event windows with hourly and daily frequency.

Building a pipeline of Microservices relies on the assumption that these microservices pass data
between each other. Apache ActiveMQ sends data between microservices using json objects containing all necessary data.  In each microservice,
after the data had been operated on, the data was converted to JSON format using the “json.dumps”
method.

Data Pre-Processing:
Before sending any data to a microservice, it was necessary to pre-process the data in order to sort
the data by time. This was done as in a real-time scenario data related to taxi trip pickups and dropoffs
would come into the system according to the time of pickup. As the initial data we were given
was unordered this was a necessary step. 

Streaming:
Streaming data with ActiveMQ using Python involves using stomp to connect to the local host at a
certain port on the computer. The connect() function is then used to login with username and
password which then begins communication with the port. The function send() sends a message in
either String or Byte format to a specified queue. As previously mentioned we always convert the
message to a Json object using json.dumps() which transforms the json object to a string so it can be
sent to the next Microservice.

When we finish publishing the data we disconnect from the port. On the subscriber side we must set
a listener and connect to the same queue that we published to. This built-in listener class has a
method on_message() which performs tasks once it receives data from the queue it is listening to.
We can again publish from this method to the next microservice which creates the pipeline between
tasks. Essentially after our original publish microservice, for every following Microservice we
subscribe to the previous Microservice and publish to the next Microservice, except for our final
Listener which outputs results. When all trip data has been streamed through the pipeline we
publish an exit message to tell the output that all tasks have gone through the pipeline

1st Microservice (Publisher)
Our first Microservice was a Microservice which published the data from the sorted CSV file line by
line. This simulates the real-time nature of the taxi trip alerts a driver would receive.

2nd Microservice (Filtering)
Our second Microservice filters the data and only selects the columns necessary for further analytics.
These columns are the Location ID and the date and time. This Microservice also removes any taxi
trip which does not have a numbered location. 

3rd Microservice (Tumbling Windows)
This Microservice implements the daily and hourly tumbling windows necessary for the analysis. The
string dates and times from the TripData.csv file (which has been sorted by date) are first converted
to “datetime” objects and filters the data to give us January 2018 dates only. For each hourly
window, we publish the day, the hour, the number of taxi trips within the hour, the peak location of
the hour and the number of trips from that location in that hour. Each value is appended to a list and
this list is published for every hour. Each hourly window is then sent to the Microservice which
converts Location ID’s using a lookup to the location Burrough name and Zone, before being sent to
the Microservice which enriches the data with the Crash Data.
As we publish our hourly window, we append the hourly window list of values to a larger list which
we will use to publish our day window. For each day window, we first append the tag “Daily
Window” to a list so as to distinguish this list from each hourly window. As well as this, we include
the day, the busiest hour of the day, the number of taxi trips in this day and the number of taxi trips
in the busiest hour. Each Daily Window is sent to the Microservice which enriches the data with the
Crash Data.

4th Microservice (Location Lookup)
This Microservice converts Location ID’s to Burrough Name and Zone using the Taxi and Zone lookup
csv file.

5th Microservice (Data Enrichment with Traffic Accident Data)
This Microservice enriches the data by adding relevant traffic accident information to each window.
For each hourly window, we append the number of crashes for the hour at the peak location of the
hour to the list from the previous Microservice.
In the same way, for each Daily Window, we append the number of crashes at the peak hour of the
day.

6th Microservice (Final Listener)
This Microservice outputs the resulting data in an informative manner by saying what type of
window we have in each case, and what the elements in the list signify.


