"""Tests unitaires pour commands/base.py - BaseCommand."""

import pytest

from webhdfsmagic.client import WebHDFSClient
from webhdfsmagic.commands.base import BaseCommand


class ConcreteCommand(BaseCommand):
    """Concrete implementation for testing BaseCommand."""

    def execute(self, *args, **kwargs):
        """Test implementation."""
        return "executed"


@pytest.fixture
def client():
    """Create a test client."""
    return WebHDFSClient(
        knox_url="http://test-knox",
        webhdfs_api="/test-api",
        auth_user="test_user",
        auth_password="test_pass",
        verify_ssl=False
    )


@pytest.fixture
def command(client):
    """Create a concrete command instance."""
    return ConcreteCommand(client)


def test_base_command_init(command, client):
    """Test BaseCommand initialization."""
    assert command.client == client
    assert isinstance(command.client, WebHDFSClient)


def test_base_command_execute_implementation(command):
    """Test that concrete implementation can execute."""
    result = command.execute()
    assert result == "executed"


def test_base_command_abstract_raises_error():
    """Test that BaseCommand cannot be instantiated directly."""
    client = WebHDFSClient(
        knox_url="http://test",
        webhdfs_api="/api",
        auth_user="user",
        auth_password="pass",
        verify_ssl=False
    )
    # Cannot instantiate abstract class
    with pytest.raises(TypeError):
        BaseCommand(client)


def test_validate_path_valid_absolute_path(command):
    """Test validate_path with valid absolute path."""
    result = command.validate_path("/user/hadoop/data")
    assert result == "/user/hadoop/data"


def test_validate_path_valid_root_path(command):
    """Test validate_path with root path."""
    result = command.validate_path("/")
    assert result == "/"


def test_validate_path_empty_raises_error(command):
    """Test validate_path with empty path raises ValueError."""
    with pytest.raises(ValueError, match="Path cannot be empty"):
        command.validate_path("")


def test_validate_path_relative_path_raises_error(command):
    """Test validate_path with relative path raises ValueError."""
    with pytest.raises(ValueError, match="Path must be absolute"):
        command.validate_path("relative/path")


def test_validate_path_no_leading_slash_raises_error(command):
    """Test validate_path without leading slash raises ValueError."""
    with pytest.raises(ValueError, match="Path must be absolute"):
        command.validate_path("user/hadoop")


def test_handle_error_without_context(command):
    """Test handle_error without context."""
    error = ValueError("Test error message")
    result = command.handle_error(error)
    assert result == "Error: Test error message"


def test_handle_error_with_context(command):
    """Test handle_error with context information."""
    error = ValueError("Test error message")
    result = command.handle_error(error, context="File operation failed")
    assert result == "File operation failed: Error: Test error message"


def test_handle_error_with_exception_types(command):
    """Test handle_error with different exception types."""
    # Test with IOError
    error = OSError("IO operation failed")
    result = command.handle_error(error, "Read failed")
    assert "Read failed: Error: IO operation failed" in result

    # Test with RuntimeError
    error = RuntimeError("Runtime issue")
    result = command.handle_error(error)
    assert "Error: Runtime issue" == result


def test_handle_error_empty_context(command):
    """Test handle_error with empty context string."""
    error = ValueError("Test error")
    result = command.handle_error(error, context="")
    assert result == "Error: Test error"
