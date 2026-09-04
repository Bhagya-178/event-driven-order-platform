import json
import logging
import sys
from datetime import datetime, timezone
from typing import Optional

class JSONFormatter(logging.Formatter):
    """
    Standardized JSON Formatter for structured logging across all services.
    Includes timestamps, log levels, service name, correlation IDs, and extra fields.
    """
    def __init__(self, service_name: str = "service"):
        super().__init__()
        self.service_name = service_name

    def format(self, record: logging.LogRecord) -> str:
        log_entry = {
            "timestamp": datetime.now(timezone.utc).isoformat(),
            "service": self.service_name,
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }

        # Correlation and trace IDs if present
        if hasattr(record, "correlation_id") and record.correlation_id:
            log_entry["correlation_id"] = str(record.correlation_id)
        if hasattr(record, "order_id") and record.order_id:
            log_entry["order_id"] = str(record.order_id)
        if hasattr(record, "event_id") and record.event_id:
            log_entry["event_id"] = str(record.event_id)

        # Exception details
        if record.exc_info:
            log_entry["exception"] = self.formatException(record.exc_info)

        return json.dumps(log_entry)

def setup_structured_logging(service_name: str, level: str = "INFO"):
    """
    Configures the root logger with the structured JSONFormatter.
    """
    logger = logging.getLogger()
    logger.setLevel(getattr(logging, level.upper(), logging.INFO))

    # Remove existing handlers
    for handler in list(logger.handlers):
        logger.removeHandler(handler)

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JSONFormatter(service_name=service_name))
    logger.addHandler(handler)
    return logger
