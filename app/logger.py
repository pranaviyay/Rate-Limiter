"""
Logger
------
Structured JSON logging for the rate limiter service.
Every significant event logs client_id, endpoint, and outcome.
"""

import logging
import json
import time
from typing import Any


class JSONFormatter(logging.Formatter):
    """Formats log records as single-line JSON — easy to parse in log aggregators."""

    def format(self, record: logging.LogRecord) -> str:
        log = {
            "timestamp": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "level": record.levelname,
            "message": record.getMessage(),
        }
        # Merge any extra fields passed via logger.info(..., extra={...})
        for key, value in record.__dict__.items():
            if key not in (
                "args", "asctime", "created", "exc_info", "exc_text",
                "filename", "funcName", "id", "levelname", "levelno",
                "lineno", "module", "msecs", "message", "msg",
                "name", "pathname", "process", "processName",
                "relativeCreated", "stack_info", "thread", "threadName",
            ):
                log[key] = value
        return json.dumps(log)


def get_logger(name: str = "ratelimiter") -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler()
        handler.setFormatter(JSONFormatter())
        logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        logger.propagate = False
    return logger


logger = get_logger()
