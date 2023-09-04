from kafka3 import KafkaProducer
import csv
from time import sleep
from random import randint
from timeit import default_timer
from datetime import datetime
from datetime import timedelta

hostip = "172.26.64.1"


def publish_message(producer_instance, topic_name, value):
    try:
        value_bytes = bytes(value, encoding="utf-8")
        producer_instance.send(topic_name, value=value_bytes)
        producer_instance.flush()
        print('Message published successfully. Data: ' + str(data))
    except Exception as ex:
        print('Exception in publishing message.')
        print(str(ex))


def get_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=[f'{hostip}:9092'],
                                  api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka.')
        print(str(ex))
    finally:
        return _producer


if __name__ == "__main__":
    topic = "main"
    producer = get_kafka_producer()

    # randomely send climate data
    with open("climate_streaming.csv") as file:
        csv_iterable = csv.reader(file)
        csv_rows = list(csv_iterable)

    num_rows = len(csv_rows)
    created_date = datetime.now()  # (psedo)
    id_ = 0
    for _ in range(300):
        created_date += timedelta(days=1)
        id_ += 1

        row_idx_to_send = randint(0, num_rows - 1)
        row = ["climate"] + list(csv_rows[row_idx_to_send]) + [created_date.strftime("%d/%m/%Y")]

        # send this
        data = ",".join(row)
        publish_message(producer, topic, data)

        sleep(10)