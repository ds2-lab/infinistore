import logging
import sys
from typing import Optional


def initialize_logger(add_handler: bool = False) -> logging.Logger:
    if add_handler:
        handler = set_file_handler("load_times.log")
    else:
        handler = set_std_handler()
    formatter = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")
    handler.setFormatter(formatter)
    handler.setLevel(logging.INFO)
    logger = get_logger("default", handler)
    return logger


def set_file_handler(filename: str) -> logging.FileHandler:
    handler = logging.FileHandler(filename)
    return handler

def set_std_handler() -> logging.StreamHandler:
    handler = logging.StreamHandler(sys.stdout)
    return handler

def get_logger(module_name: str, handler: Optional[logging.FileHandler]) -> logging.Logger:
    logger = logging.getLogger(module_name)
    logger.setLevel(logging.INFO)
    if handler:
        logger.addHandler(handler)
    return logger
