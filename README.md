# Fire Stream App

A streaming prcoessing app and monktoring tool for fire events on earth. 

Some functionalities:
1. When fire event is streamed to the app, it classifies fire event between man-made or natural, by comaprisng the geohash of the event locations.
2. The app stores the classification result toghether with the fire even record on MongoDB for later analysis/visualization.
3. To simulate the incoming weather data and fire data, we created three servers and Kafka producers. 

Overall picture of the application:
![Architecture of application](images/architecture.png)
