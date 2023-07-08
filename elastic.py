import os
from elasticsearch import Elasticsearch
from loadenv import load_dotenv

load_dotenv()


class Elastic:
    def __init__(self):
        self.host = os.environ.get("ES_HOST")
        self.port = os.environ.get("ES_PORT")
        self.es = None
        self.connect()
        self.INDEX_NAME = "my-log"

    def connect(self):
        self.es = Elasticsearch({"host": self.host, "port": self.port})
        if self.es.ping():
            print("ES connected successfully")
        else:
            print("Not Connected")

    def create_index(self):
        if self.es.indices.exists(self.INDEX_NAME):
            print("Deleting '%s' index..." % (self.INDEX_NAME))
            res = self.es.indices.delete(index=self.INDEX_NAME)
            print(" response: '%s'" % (res))
            request_body = {
                "settings": {"number_of_shards": 1, "number_of_replicas": 0}
            }
            print("Creating '%s' index..." % (self.INDEX_NAME))
            res = self.es.indices.create(
                index=self.INDEX_NAME, body=request_body, ignore=400
            )
            print(" response: '%s'" % (res))

    def push_to_index(self, message):
        try:
            response = self.es.index(
                index=self.INDEX_NAME, doc_type="log", body=message
            )
            print("Write response is :: {}\n\n".format(response))
        except Exception as e:
            print("Exception is :: {}".format(str(e)))
