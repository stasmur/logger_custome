from confluent_kafka import Producer, KafkaException
from threading import Thread
from typing import Any, Dict
from loguru import logger


class KafkaProducer:
    def __init__(self):
        """
        Constructor for KafkaProducer class.

        Attributes:
        - _loop: asyncio event loop used for working with asynchronous tasks.
        - _producer: Producer object from confluent_kafka library for sending messages.
        - _cancelled: flag indicating termination of operation.
        - _poll_thread: thread for polling Kafka Producer.
        """
        self._loop = None
        self._producer = None
        self._cancelled = False
        self._poll_thread = None

    def initiate_producer(self, creds: dict):
        """
        Initializes the Kafka producer with the provided credentials.

        Parameters:
        - creds: dictionary containing credentials for setting up Kafka producer.
        """
        producer_config = {
            "message.send.max.retries": 1,
            "default.topic.config": {
                "request.required.acks": 1,
            },
            "compression.type": "lz4"
        }
        producer_config.update(creds)  # Update producer configuration with data from creds

        self._producer = Producer(**producer_config)

    def start_producer(self, loop=None):
        """
        Method to initialize and start the Kafka producer.

        Parameters:
        - loop: asyncio event loop for use in asynchronous operations.
        """
        self._loop = loop
        if self._producer is None:
            raise RuntimeError("Producer is not initialized. Call initiate_producer() first.")

        self._poll_thread = Thread(target=self._poll_loop)
        self._poll_thread.start()

    def _poll_loop(self):
        """
        Method for polling Kafka producer for new messages and sending them.
        """
        while not self._cancelled:
            self._producer.poll(0.1)

    def close(self):
        """
        Method to stop the Kafka producer and terminate its operation.
        It is advisable to turn off together with the application.
        """
        self._cancelled = True
        self._poll_thread.join()

    def produce(self, topic, messages):
        """
        Method to send a message to the specified Kafka topic.

        Parameters:
        - topic: string, name of the topic to send the message to.
        - messages: message to send to the topic.

        Returns:
        - Future: Future object representing the result of sending the message.
        """
        result = self._loop.create_future()

        def ack(err, msg):
            """
            Callback function called when a message is successfully sent to Kafka.

            Parameters:
            - err: KafkaException object representing the error if one occurred.
            - msg: message that was sent to Kafka.
            """
            error_dict: Dict[str, Any] = {"exception": []}
            if err:
                logger.error(error_dict)
                self._loop.call_soon_threadsafe(result.set_exception, KafkaException(err))
            else:
                self._loop.call_soon_threadsafe(result.set_result, True)

        self._producer.produce(topic, messages, on_delivery=ack)
        return result
