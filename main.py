import os
os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-streaming-kafka-0-10_2.12:3.3.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.3.0 pyspark-shell'

from pymongo import MongoClient
from pyspark.sql import SparkSession
from pyspark.sql.types import BooleanType, DoubleType
from pyspark.sql.functions import col, split, element_at, when, udf, lit, avg, window

from geohash import encode, decode
import datetime

from pyspark.sql.streaming import StreamingQueryException



hostip = "172.26.64.1"
spark = SparkSession.builder.master('local[*]').appName('mountain_fire_analyzer').getOrCreate()

df = (
    spark.readStream.format('kafka')
    .option('kafka.bootstrap.servers', f'{hostip}:9092')
    .option('subscribe', "main")
    .load()
    .drop("key", "topic", "partition", "offset", "timestamp", "timestampType")
    .withColumn("value", split("value", ","))
)

df.printSchema()


def process_climate_row(row, current_date):
    header = ["date", "air_temperature_celcius",
              "relative_humidity", "windspeed_knots", "max_wind_speed",
              "precipitation", "precipitation_status", "ghi", "fires"]

    # ignores the first three cols: source, latitude, longitude
    new_row = [
        current_date.strftime("%d/%m/%Y"),
        int(row[3]),
        float(row[4]),
        float(row[5]),
        float(row[6]),
        float(row[7][:-1]),
        row[7][-1],
        int(row[8]),
        []
    ]

    return dict(zip(header, new_row))


def process_hotspot_row(row, current_date):
    header = ['latitude', 'longitude', "datetime",
              'confidence', 'surface_temperature_celcius']

    # note: all values are already parsed to appropriate type
    new_row = [
        row[0],
        row[1],
        current_date.strftime("%d-%m-%Y") + "T" + row[-1][-8:],
        row[2],
        row[3]
    ]

    return dict(zip(header, new_row))


def process_batch_pymongo(df, epoch_idx):
    date_ = datetime.date(2023, 1, 1) + datetime.timedelta(days=epoch_idx)  # todo

    data = df.collect()
    data = [list(dt.asDict().values())[0] for dt in data]

    print(data)
    if not len(data):
        return

    # initialize mongodb connection
    try:
        host_ip = "172.26.64.1"
        client = MongoClient(f'mongodb://{host_ip}:27017/')
        database_name = "hotspot"
        db = client[database_name]
        collection = db["climate"]
        print("connected to database")
    except Exception as e:
        print(e)

    climate_data = []
    hotspot_terra = []
    hotspot_aqua = []

    # classifying data
    for dt in data:
        if dt[0] == "climate":
            climate_data.append(dt)
        elif dt[0] == "aqua":
            hotspot_aqua.append(dt)
        elif dt[0] == "terra":
            hotspot_terra.append(dt)
        else:
            raise ValueError("data must be climate or hotspot data")

    # there should be at least one climate data in every batch
    if not len(climate_data):
        return

    # joining hotspot data
    hotspot_data = []
    for terra_data in hotspot_terra:
        terra_enc = encode(float(terra_data[1]), float(terra_data[2]), precision=5)
        for aqua_data in hotspot_aqua:
            aqua_enc = encode(float(aqua_data[1]), float(aqua_data[2]), precision=5)

            # if hashed values are the same
            if terra_enc == aqua_enc:
                latitude, longitude = decode(aqua_enc)
                hotspot_data.append([float(latitude), float(longitude), (int(terra_data[3]) + int(aqua_data[3])) / 2,
                                     (int(terra_data[4]) + int(aqua_data[4])) / 2, terra_data[5]])
            else:
                # different -> append both of them separately
                hotspot_data.append(
                    [float(terra_data[1]), float(terra_data[2]), int(terra_data[3]), int(terra_data[4]), terra_data[5]])
                hotspot_data.append(
                    [float(aqua_data[1]), float(aqua_data[2]), int(aqua_data[3]), int(aqua_data[4]), terra_data[5]])

    # find corresponding climate and hotspot data
    climate = climate_data[0]
    climate_enc = encode(float(climate[1]), float(climate[2]), precision=3)
    for hotspot in hotspot_data:
        hotspot_enc = encode(hotspot[0], hotspot[1], precision=3)

        if climate_enc == hotspot_enc:
            # access database
            # create an embeded document
            climate = process_climate_row(climate, date_)
            hotspot = process_hotspot_row(hotspot, date_)
            if climate["air_temperature_celcius"] > 20 and climate["ghi"] > 180:
                hotspot["cause"] = "natural"
            else:
                hotspot["cause"] = "other"

            climate["fires"].append(hotspot)
            collection.insert_one(climate)
            print("inserted ", climate)
            return

    # create a document with just climate data
    climate = process_climate_row(climate, date_)
    collection.insert_one(climate)
    print("inserted ", climate)

console_logger = (
    df
    .writeStream
    .outputMode('append')
    .format('console')
    .trigger(processingTime='10 seconds')
    .foreachBatch(process_batch_pymongo)
)


if __name__ == "__main__":
    try:
        query = console_logger.start()
        query.awaitTermination()
    except KeyboardInterrupt:
        print('Interrupted by CTRL-C. Stopped query')
    except StreamingQueryException as exc:
        print(exc)
    finally:
        query.stop()