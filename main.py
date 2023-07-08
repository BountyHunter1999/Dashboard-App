from log_parser import LogParser
from producer import KafkaProducer

if __name__ == "__main__":
    logFile = LogParser.read_log_file()
    logFileGen = LogParser.fetch_log(logFile)
    producer = KafkaProducer()
    while True:
        try:
            data = next(logFileGen)
            serialized_data = LogParser.serialize_log(data)
            print("Message is :: {}".format(serialized_data))
            producer.produce(serialized_data)
        except StopIteration:
            exit()
        except KeyboardInterrupt:
            print("Printing last message before exiting :: {}".format(serialized_data))
            exit()