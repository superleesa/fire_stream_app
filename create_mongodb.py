from pymongo import MongoClient
import csv


HOST_IP = "172.26.64.1"


def process_climate_row(row):
    header = ["station", "date", "air_temperature_celcius",
              "relative_humidity", "windspeed_knots", "max_wind_speed",
              "precipitation", "precipitation_status", "ghi", "fires"]

    row = list(row)
    new_row = [
        int(row[0]),
        row[1],
        int(row[2]),
        float(row[3]),
        float(row[4]),
        float(row[5]),
        float(row[6][:-1]),
        row[6][-1],
        int(row[7]),
        []
    ]

    return dict(zip(header, new_row))


def process_hotspot_row(row):
    header = ['latitude', 'longitude', "datetime",
              'confidence', 'surface_temperature_celcius']

    row = list(row)
    new_row = [
        float(row[0]),
        float(row[1]),
        row[2],
        int(row[3]),
        int(row[5]),
    ]

    return dict(zip(header, new_row))


if __name__ == "__main__":
    # connect to mongo and initialize a database for the hotspot data
    client = MongoClient(f"mongodb://{HOST_IP}:27017/")
    database_name = "hotspot"
    db = client[database_name]
    collection = db["climate"]
    client.list_databases()

    # fill the DB with dummy weather data records from csv
    with open("climate_historic.csv") as file:
        reader = csv.reader(file)
        header = None
        for i, row in enumerate(reader):
            if i != 0:
                # insert into mongodb
                result = collection.insert_one(process_climate_row(row))
                print(result)

    # add dummy fire events to matching weather date records
    with open("hotspot_historic.csv") as file:
        reader = csv.reader(file)
        for i, row in enumerate(reader):
            if i != 0:
                filter_ = {"date": row[4]}
                update = {"$push": {"fires": process_hotspot_row(row)}}
                result = collection.update_one(filter_, update)
                print(result)
