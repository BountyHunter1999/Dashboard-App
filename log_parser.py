"""
Parse the log and return messages in a python dictionary format
2023-07-08T12:25:46.808392+0000 | INFO | filename.py:89 |
"""
import os
from dotenv import load_dotenv

load_dotenv()


class LogParser:
    @staticmethod
    def fetch_log(file_obj):
        for row in file_obj:
            yield row

    @staticmethod
    def read_log_file():
        filename = os.environ.get("LOG_FILE")
        return open(filename, "r")

    @staticmethod
    def serialize_log(log):
        log.strip()
        get_message = log.split(" ")
        if len(get_message):
            message = " ".join(get_message[3:])
            message_type = get_message[2]
            if message_type not in ["INFO", "ERROR", "CRITICAL", "WARNING"]:
                return None
            _datetime = get_message[0]
            # _date = _datetime.split("T")[0]
            # _timestamp = _datetime.split("T")[1]
        log_dict = {
            "message": message.strip(),
            "timestamp": _datetime,
            "type": message_type,
        }
        return log_dict
