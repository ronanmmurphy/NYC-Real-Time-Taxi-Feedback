# NYC-Real-Time-Taxi-Feedback
Created Micro-Services pipeline which processed, wrangled, and transformed traffic and incident information in order to provide Real-Time feedback to taxi drivers in NYC about the optimal route to take. This scalable pipeline was created using Python with service-oriented design principles. Apache ActiveMQ is the messaging service that transmitted information between Micro-Services. Updates of busiest locations, peak times, accidents, and the number of taxi trips at a location were provided through tumbling event windows with hourly and daily frequency.