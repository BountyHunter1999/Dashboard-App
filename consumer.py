import os
from confluent_kafka import Consumer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS")


class KafkaConsumer:
    def __init__(self, topic, broker=BOOTSTRAP_SERVERS, group="group1"):
        self.broker = broker
        self.group = group
        self.con = Consumer(
            {
                "bootstrap.servers": self.broker,
                "group.id": self.group,
                "auto.offset.reset": "earliest",
            }
        )
        self.topic = topic
        self.con.subscribe([self.topic])

    def read_messages(self):
        try:
            msg = self.con.poll(0.1)
            if msg is None:
                return 0
            elif msg.error():
                return 0
            print("Recieved message: {}".format(msg.value().decode("utf-8")))
            return 1
        except Exception as e:
            print("Exception during reading message :: {}".format(e))
            return 0
