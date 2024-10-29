import sys
import logging
import time

from functools import wraps


def setup_logging():
    # Create a StreamHandler for stdout
    stdout_handler = logging.StreamHandler(sys.stdout)
    stdout_handler.setFormatter(
        logging.Formatter('%(asctime)s - [%(processName)s] - [%(threadName)s] - %(name)s - %(levelname)s - %(message)s')
    )

    # Get the root logger and set its level
    root_logger = logging.getLogger()
    root_logger.setLevel(logging.INFO)

    # Remove any existing handlers and add the stdout handler
    root_logger.handlers = []
    root_logger.addHandler(stdout_handler)


def get_logger(module_name):
    return logging.getLogger(module_name)


def timelog(_logger):
    """
    A decorator that logs the execution time of a function.

    Parameters:
        _logger (logging.Logger): The logger instance to use for logging.
    """

    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            start_time = time.time()
            result = func(*args, **kwargs)
            end_time = time.time()
            elapsed_time = end_time - start_time
            _logger.info(f"func - '{func.__name__}' - time: {elapsed_time:.4f}")
            return result

        return wrapper

    return decorator