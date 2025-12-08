"""Tests for the logging module."""

from pathlib import Path

from webhdfsmagic.logger import get_logger


def test_logger_singleton():
    """Test that logger follows singleton pattern."""
    logger1 = get_logger()
    logger2 = get_logger()
    assert logger1 is logger2, "Logger should be a singleton"


def test_logger_creates_log_directory():
    """Test that logger creates the logs directory."""
    get_logger()  # Initialize logger
    log_dir = Path.home() / ".webhdfsmagic" / "logs"
    assert log_dir.exists(), "Log directory should be created"


def test_logger_creates_log_file():
    """Test that logger creates a log file."""
    get_logger()  # Initialize logger
    log_file = Path.home() / ".webhdfsmagic" / "logs" / "webhdfsmagic.log"
    assert log_file.exists(), "Log file should be created"


def test_log_operation_start():
    """Test logging operation start."""
    logger = get_logger()
    logger.log_operation_start("test_op", param1="value1", param2="value2")

    log_file = Path.home() / ".webhdfsmagic" / "logs" / "webhdfsmagic.log"
    content = log_file.read_text()
    assert "Starting operation: test_op" in content
    assert "param1: value1" in content
    assert "param2: value2" in content


def test_log_operation_end():
    """Test logging operation end."""
    logger = get_logger()
    logger.log_operation_end("test_op", success=True, result="completed")

    log_file = Path.home() / ".webhdfsmagic" / "logs" / "webhdfsmagic.log"
    content = log_file.read_text()
    assert "Operation completed: test_op - SUCCESS" in content
    assert "result: completed" in content


def test_log_http_request():
    """Test logging HTTP request."""
    logger = get_logger()
    logger.log_http_request(
        method="GET",
        url="http://test.com/api",
        operation="LISTSTATUS",
        path="/test",
    )

    log_file = Path.home() / ".webhdfsmagic" / "logs" / "webhdfsmagic.log"
    content = log_file.read_text()
    assert "HTTP Request: GET http://test.com/api" in content
    assert "operation: LISTSTATUS" in content


def test_log_http_response():
    """Test logging HTTP response."""
    logger = get_logger()
    logger.log_http_response(
        status_code=200,
        url="http://test.com/api",
        headers={"Content-Type": "application/json"},
    )

    log_file = Path.home() / ".webhdfsmagic" / "logs" / "webhdfsmagic.log"
    content = log_file.read_text()
    assert "HTTP Response: 200 from http://test.com/api" in content


def test_log_error():
    """Test logging errors."""
    logger = get_logger()
    try:
        raise ValueError("Test error")
    except ValueError as e:
        logger.log_error("test_operation", e, context="test")

    log_file = Path.home() / ".webhdfsmagic" / "logs" / "webhdfsmagic.log"
    content = log_file.read_text()
    assert "ERROR in test_operation: ValueError: Test error" in content
    assert "context: test" in content


def test_password_masking():
    """Test that passwords are masked in logs."""
    logger = get_logger()
    logger.log_operation_start("auth_test", username="user", password="secret123")

    log_file = Path.home() / ".webhdfsmagic" / "logs" / "webhdfsmagic.log"
    content = log_file.read_text()
    assert "username: user" in content
    assert "password: ***MASKED***" in content
    assert "secret123" not in content


def test_log_file_rotation_configuration():
    """Test that log rotation is configured."""
    logger = get_logger()
    handlers = logger.logger.handlers

    # Find the RotatingFileHandler
    file_handler = None
    for handler in handlers:
        if hasattr(handler, "maxBytes"):
            file_handler = handler
            break

    assert file_handler is not None, "RotatingFileHandler should be configured"
    assert file_handler.maxBytes == 10 * 1024 * 1024, "Max size should be 10MB"
    assert file_handler.backupCount == 5, "Should keep 5 backup files"


def test_logger_levels():
    """Test different logging levels."""
    logger = get_logger()

    logger.debug("Debug message")
    logger.info("Info message")
    logger.warning("Warning message")
    logger.error("Error message")

    log_file = Path.home() / ".webhdfsmagic" / "logs" / "webhdfsmagic.log"
    content = log_file.read_text()

    assert "Debug message" in content
    assert "Info message" in content
    assert "Warning message" in content
    assert "Error message" in content
