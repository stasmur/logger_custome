import multiprocessing
import sys
import ast
import traceback
import threading
import json
import os

from abc import abstractmethod

from loguru import logger
from loguru._better_exceptions import ExceptionFormatter  #
from datetime import datetime


# from configs import project_directory - add to the folder structure the config folder with the project address


class Logging:
    def __init__(self, level='DEBUG'):
        """
        Initializes the Logging object.

        Parameters:
        - task_id: task identifier (default is '9999').
        - file_log: name of the file for logging (default is 'logs.log').
        """
        self.date = datetime.now().date()
        self.level = level
        self.exception_formatter = ExceptionFormatter()
        logger.remove(handler_id=0)

    @staticmethod
    def record_exception(exception: None or Exception):
        """
        Generates a string with a formatted exception message.

        Example output:
        Traceback (most recent call last):

          File "D:/Python/project_scanner_v2/venv/Lib/site-packages/loguru/_logger.py", line 1251, in catch_wrapper
            return function(*args, **kwargs)
                   |         |       -> {}
                   |         -> ()
                   -> <function SomeClass.some_foo_for_except_decorator at 0x000002DAF8220540>

          File "D:/Python/project_scanner_v2/utils/logger_main.py", line 96, in some_foo_for_except_decorator
            a.get('some')
            -> None

        AttributeError: 'NoneType' object has no attribute 'get'

        Parameters:
        - exception: exception object.

        Returns:
        - String with a formatted exception message.
        """
        try:
            if exception and not isinstance(exception, Exception):
                type_, value, traceback_ = (exception.type, exception.value, exception.traceback)
            elif isinstance(exception, Exception):
                if isinstance(exception, BaseException):
                    type_, value, traceback_ = (type(exception), exception, exception.__traceback__)
                elif isinstance(exception, tuple):
                    type_, value, traceback_ = exception
                else:
                    type_, value, traceback_ = sys.exc_info()
            if 'catcher' in traceback_.tb_frame.f_locals:
                from_decorator = True
            else:
                from_decorator = False
            lines = Logging.exception_formatter.format_exception(type_, value, traceback_,
                                                                 from_decorator=from_decorator)
            res = ''.join(lines)
            return res
        except Exception as e:
            return f'''can't parse exception: {e.args}, main_exception: {exception}'''

    def serialize(self, record):
        """
        Serializes the log record into JSON format.

        Parameters:
        - record: log record.

        Returns:
        - JSON string.
        """
        try:
            message = ast.literal_eval(record["message"])
            if not isinstance(message, dict) and not isinstance(message, list) and not isinstance(message, tuple):
                message = record["message"]
        except (SyntaxError, ValueError):
            message = record["message"]

        place_info = self.get_place_info(record)
        thread_id = threading.current_thread().name
        process_name = multiprocessing.current_process().name
        subset = {
            "timestamp": record["time"].timestamp(),
            "level": record['level'].name if not record['exception'] else 'EXCEPTION',
            "message": message if not record['exception'] else self.record_exception(record['exception']),
            "place": place_info,
            "thread_name": thread_id,
            "process_name": process_name
        }
        try:
            return json.dumps(subset)
        except TypeError:
            subset['message'] = str(message)
            return json.dumps(subset)
        except Exception as e:
            return json.dumps(''.join(traceback.format_exception(e)))

    def get_place_info(self, record):
        file_path = os.path.relpath(record["file"].path, start=os.getcwd())
        method_name = record['function'] if record['function'] != '<module>' else record['module']
        line_number = record["line"]
        return f"{file_path}:{method_name}:{line_number}"

    def formatter(self, record):
        """
        Formats the log record.

        Parameters:
        - record: log record.

        Returns:
        - Formatted string.
        """
        record["extra"]["serialized"] = self.serialize(record)
        return "{extra[serialized]}\n"

    @abstractmethod
    def init_logger(self):
        raise NotImplementedError("you must implement 'init_logger' method.")

    @abstractmethod
    def del_logger(self):
        raise NotImplementedError("you must implement 'del_logger' method.")
