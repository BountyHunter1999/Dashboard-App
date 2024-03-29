import json
import time
import os
from confluent_kafka import Producer
from dotenv import load_dotenv

load_dotenv()

BOOTSTRAP_SERVERS = os.environ.get("BOOTSTRAP_SERVERS")


class KafkaProducer:
    def __init__(self):
        self.bootstrap_servers = BOOTSTRAP_SERVERS
        self.topic = "data_log"
        self.p = Producer({"bootstrap.servers": self.bootstrap_servers})

    def delivery_report(self, err, msg):
        """
        Called once for each message produced to indicate delivery result.
        Triggered by poll() or flush().
        """
        if err is not None:
            print("Message delivery failed: {}".format(err))
        else:
            print("Message delivered to {} [{}]".format(msg.topic(), msg.partition()))

    def produce(self, msg):
        serialized_message = json.dumps(msg)
        self.p.produce(self.topic, serialized_message, callback=self.delivery_report)
        # Trigger any available delivery report callbacks from previous produce() calls
        self.p.poll(0)
        time.sleep(1)
