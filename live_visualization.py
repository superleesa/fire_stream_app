from kafka3 import KafkaConsumer
import matplotlib.pyplot as plt
from datetime import datetime


KAFKA_TOPIC = 'main'
HOSTIP = "172.26.64.1"


def annotate_max(x, y, ax):
    ymax = max(y)
    xpos = y.index(ymax)
    xmax = x[xpos]
    text = 'Max: Time={}, Value={}'.format(str(xmax), str(ymax))
    print(text)
    ax.annotate(text, xy=(xmax, ymax), xytext=(xmax, ymax + 5), arrowprops=dict(facecolor='red', shrink=0.05), )


def annotate_min(x, y, ax):
    ymin = min(y)
    xpos = y.index(ymin)
    xmin = x[xpos]
    text = 'Min: Time={}, Value={}'.format(str(xmin), str(ymin))
    ax.annotate(text, xy=(xmin, ymin), xytext=(xmin, ymin + 5), arrowprops=dict(facecolor='orange', shrink=0.05), )


def connect_and_get_kafka_consumer():
    _consumer = None
    try:
         _consumer = KafkaConsumer(KAFKA_TOPIC,
                                   consumer_timeout_ms=10000,
                                   auto_offset_reset='earliest',
                                   bootstrap_servers=[f'{HOSTIP}:9092'],
                                   value_deserializer=lambda x: x.decode('ascii'),
                                   api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _consumer


def init_plots():
    width = 9.5
    height = 6
    fig = plt.figure(figsize=(width, height))
    ax = fig.add_subplot(1, 1, 1)
    ax.set_xlabel("Time (sec)")
    ax.set_ylabel('Air Temperature (celcius)')

    fig.suptitle('Chnage in Air Temperature data visualization')  # giving figure a title
    fig.show()  # displaying the figure
    fig.canvas.draw()  # drawing on the canvas
    return fig, ax


def consume_messages(consumer, fig, ax):
    try:
        x, y = [], []
        for message in consumer:
            message = message.value.split(",")

            if "climate" not in message:
                continue

            x.append(datetime.now())
            y.append(int(message[3]))

            # we start plotting only when we have 10 data points
            if len(x) > 10:
                ax.clear()

                ax.plot(x, y)
                ax.set_xlabel("Time (sec)")
                ax.set_ylabel("Air Temperature (celcius)")
                ax.set_title('Creation Time Vs Air Temeprature')
                ax.set_ylim(0, 40)
                ax.set_yticks([0, 5, 10, 15, 20, 25, 30, 35, 40])
                ax.tick_params(labelrotation=45)

                annotate_max(x[-10:], y[-10:], ax)
                annotate_min(x[-10:], y[-10:], ax)

                fig.canvas.draw()
                x.pop(0)
                y.pop(0)

            plt.pause(1)

        plt.close('all')
    except Exception as ex:
        print(str(ex))


if __name__ == "__main__":
    consumer = connect_and_get_kafka_consumer()
    fig, ax = init_plots()
    consume_messages(consumer, fig, ax)
