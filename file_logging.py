from logger_main import Logging
from loguru import logger


class FileLogging(Logging):
    """
    class to save log into file
    """

    def __init__(self, file="log", rotate=False):
        super().__init__()
        self.file = file
        self.rotate = rotate
        self._handler_id = None
        self.log_path = "logs/{time:YYYY-MM-DD HH}/" + f"{self.file}.log"

    def init_logger(self):
        if self.rotate is False:
            self._handler_id = logger.add(f'logs/{self.date}-{self.file}.log',
                                          format=self.formatter,
                                          colorize=True,
                                          level=self.level)
        else:

            self._handler_id = logger.add(self.log_path,
                                          level=self.level,
                                          format=self.formatter,
                                          colorize=True,
                                          rotation="60 min",
                                          compression="zip")

        return logger

    def del_logger(self):
        logger.remove(handler_id=self._handler_id)
