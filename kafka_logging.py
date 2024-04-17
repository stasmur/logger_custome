import asyncio
import json

from loguru._better_exceptions import ExceptionFormatter  # noqa
from loguru import logger
from kafka_producer import KafkaProducer
from logger_main import Logging


class KafkaLogging(Logging):
    """
    Class for logging to Kafka.
    """

    def __init__(self, creds: dict, topic, loop=None):
        """
        Constructor for KafkaLogging class.

        Parameters:
        - creds: dictionary containing Kafka credentials.
        - topic: Kafka topic for logging.
        - loop: asyncio event loop for asynchronous operations.
        - task_id: task identifier.
        - file_log: name of the log file.
        """
        super().__init__()
        self.topic = topic
        self.loop = loop or asyncio.get_event_loop()
        self.producer = KafkaProducer()
        self.producer.initiate_producer(creds)
        self._handler_id = None

    def send_log_to_kafka(self, log_data):
        """
        Method for sending logs to Kafka.

        Parameters:
        - log_data: dictionary containing log data.
        """
        message = json.dumps(log_data).encode('utf-8')
        self.producer.produce(self.topic, message)

    def log_to_kafka(self, level, message, **kwargs):
        """
        Method for logging messages to Kafka.

        Parameters:
        - level: logging level (INFO, ERROR, DEBUG, WARNING, CRITICAL).
        - message: message to log.
        - **kwargs: additional key-value pairs for the log.
        """
        log_data = {
            "level": level.upper(),
            "message": message,
            **kwargs
        }
        self.send_log_to_kafka(log_data)

    def init_logger(self):
        self.producer.start_producer(self.loop)

    def del_logger(self):
        logger.remove(handler_id=self._handler_id)
        self.producer.close()

    def info(self, message, **kwargs):
        """
        Method for logging informational messages to Kafka.

        Parameters:
        - message: informational message.
        - **kwargs: additional key-value pairs for the log.
        """
        self.log_to_kafka("INFO", message, **kwargs)

    def error(self, message, **kwargs):
        """
        Method for logging errors to Kafka.

        Parameters:
        - message: error message.
        - **kwargs: additional key-value pairs for the log.
        """
        self.log_to_kafka("ERROR", message, **kwargs)

    def debug(self, message, **kwargs):
        """
        Method for logging debug messages to Kafka.

        Parameters:
        - message: debug message.
        - **kwargs: additional key-value pairs for the log.
        """
        self.log_to_kafka("DEBUG", message, **kwargs)

    def warning(self, message, **kwargs):
        """
        Method for logging warning messages to Kafka.

        Parameters:
        - message: warning message.
        - **kwargs: additional key-value pairs for the log.
        """
        self.log_to_kafka("WARNING", message, **kwargs)

    def critical(self, message, **kwargs):
        """
        Method for logging critical errors to Kafka.

        Parameters:
        - message: critical message.
        - **kwargs: additional key-value pairs for the log.
        """
        self.log_to_kafka("CRITICAL", message, **kwargs)
