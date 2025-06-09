# Fire Stream App

A streaming preprocessing app and monitoring tool for fire events on earth.
![Architecture of application](images/architecture.png)

## Some functionalities:
1. When a fire event is streamed to the app, it classifies the event as either man-made or natural by comparing the geohash of the event location.
2. The app stores the classification result together with the fire event record on MongoDB for later analysis/visualization.
3. To simulate the incoming weather data and fire data, we created three servers and Kafka producers.

## Note on running the app:
- change host ip
- 27017 port to be open for MongoDB
- 9092 port to be open for Kafka
- 5000 port to be open for the Spark app

## Setup
Use [uv](https://github.com/astral-sh/uv) to manage Python dependencies:

```bash
uv pip install -r uv.lock
```

## Docker
The project includes a `Dockerfile` and `docker-compose.yml` for running the
application together with Kafka and MongoDB. Build and start the services with:

```bash
docker compose up --build
```

This command builds the application image and launches MongoDB, Kafka, and
producer containers all at once.
