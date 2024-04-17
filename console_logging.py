import sys

from logger_main import Logging
from loguru import logger


class ConsoleLogging(Logging):
    """
    class to write logs to console
    """

    def __init__(self):
        super().__init__()
        self._handler_id = None

    def init_logger(self):
        self._handler_id = logger.add(sys.stdout, format=self.formatter, level=self.level)
        return logger

    def del_logger(self):
        logger.remove(handler_id=self._handler_id)
