"""
Logging configuration for webhdfsmagic.

Inspired by sparkmagic's logging system to provide detailed operation tracing
and debugging capabilities.
"""

import logging
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Optional


class WebHDFSLogger:
    """
    Centralized logger for webhdfsmagic operations.

    Logs are written to ~/.webhdfsmagic/logs/ with rotation support.
    """

    _instance: Optional["WebHDFSLogger"] = None
    _logger: Optional[logging.Logger] = None

    def __new__(cls):
        """Singleton pattern to ensure one logger instance."""
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialize_logger()
        return cls._instance

    def _initialize_logger(self):
        """Initialize the logging system."""
        # Create logs directory
        log_dir = Path.home() / ".webhdfsmagic" / "logs"
        log_dir.mkdir(parents=True, exist_ok=True)

        # Log file with date
        log_file = log_dir / "webhdfsmagic.log"

        # Create logger
        self._logger = logging.getLogger("webhdfsmagic")
        self._logger.setLevel(logging.DEBUG)

        # Prevent duplicate handlers
        if self._logger.handlers:
            return

        # File handler with rotation (10MB per file, keep 5 backups)
        file_handler = RotatingFileHandler(
            log_file,
            maxBytes=10 * 1024 * 1024,  # 10 MB
            backupCount=5,
            encoding="utf-8",
        )
        file_handler.setLevel(logging.DEBUG)

        # Detailed format for file
        file_formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(levelname)s - [%(filename)s:%(lineno)d] - %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        file_handler.setFormatter(file_formatter)

        # Console handler (only warnings and errors)
        console_handler = logging.StreamHandler()
        console_handler.setLevel(logging.WARNING)
        console_formatter = logging.Formatter("%(levelname)s: %(message)s")
        console_handler.setFormatter(console_formatter)

        # Add handlers
        self._logger.addHandler(file_handler)
        self._logger.addHandler(console_handler)

        # Initial log entry
        self._logger.info("=" * 80)
        self._logger.info("WebHDFS Magic Logger initialized")
        self._logger.info(f"Log directory: {log_dir}")
        self._logger.info("=" * 80)

    @property
    def logger(self) -> logging.Logger:
        """Get the logger instance."""
        return self._logger

    def log_operation_start(self, operation: str, **kwargs):
        """Log the start of an operation with parameters."""
        self._logger.info(f">>> Starting operation: {operation}")
        for key, value in kwargs.items():
            # Mask sensitive information
            if "password" in key.lower():
                value = "***MASKED***"
            self._logger.debug(f"    {key}: {value}")

    def log_operation_end(self, operation: str, success: bool = True, **kwargs):
        """Log the end of an operation."""
        status = "SUCCESS" if success else "FAILED"
        self._logger.info(f"<<< Operation completed: {operation} - {status}")
        for key, value in kwargs.items():
            self._logger.debug(f"    {key}: {value}")

    def log_http_request(self, method: str, url: str, **kwargs):
        """Log HTTP request details."""
        self._logger.debug(f"HTTP Request: {method} {url}")
        if kwargs:
            for key, value in kwargs.items():
                if key == "auth":
                    value = f"({value[0]}, ***)"
                self._logger.debug(f"    {key}: {value}")

    def log_http_response(self, status_code: int, url: str, **kwargs):
        """Log HTTP response details."""
        self._logger.debug(f"HTTP Response: {status_code} from {url}")
        for key, value in kwargs.items():
            self._logger.debug(f"    {key}: {value}")

    def log_error(self, operation: str, error: Exception, **kwargs):
        """Log an error with full context."""
        self._logger.error(f"ERROR in {operation}: {type(error).__name__}: {str(error)}")
        for key, value in kwargs.items():
            self._logger.error(f"    {key}: {value}")
        self._logger.exception("Full traceback:")

    def debug(self, message: str):
        """Log a debug message."""
        self._logger.debug(message)

    def info(self, message: str):
        """Log an info message."""
        self._logger.info(message)

    def warning(self, message: str):
        """Log a warning message."""
        self._logger.warning(message)

    def error(self, message: str):
        """Log an error message."""
        self._logger.error(message)


# Global logger instance
def get_logger() -> WebHDFSLogger:
    """Get the global logger instance."""
    return WebHDFSLogger()
