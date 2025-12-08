"""
Base command class for WebHDFS operations.

All command implementations inherit from BaseCommand to ensure
consistent interface and error handling.
"""

from abc import ABC, abstractmethod
from typing import Any

from ..client import WebHDFSClient


class BaseCommand(ABC):
    """Base class for all WebHDFS commands."""

    def __init__(self, client: WebHDFSClient):
        """
        Initialize command with WebHDFS client.

        Args:
            client: WebHDFSClient instance
        """
        self.client = client

    @abstractmethod
    def execute(self, *args, **kwargs) -> Any:
        """
        Execute the command.

        Args:
            *args: Positional arguments
            **kwargs: Keyword arguments

        Returns:
            Command result (type depends on implementation)

        Raises:
            NotImplementedError: Must be implemented by subclasses
        """
        raise NotImplementedError("Subclasses must implement execute()")

    def validate_path(self, path: str) -> str:
        """
        Validate and normalize HDFS path.

        Args:
            path: HDFS path to validate

        Returns:
            Normalized path

        Raises:
            ValueError: If path is invalid
        """
        if not path:
            raise ValueError("Path cannot be empty")

        if not path.startswith("/"):
            raise ValueError(f"Path must be absolute (start with /): {path}")

        return path

    def handle_error(self, error: Exception, context: str = "") -> str:
        """
        Handle and format error messages.

        Args:
            error: Exception that occurred
            context: Additional context information

        Returns:
            Formatted error message
        """
        error_msg = f"Error: {str(error)}"
        if context:
            error_msg = f"{context}: {error_msg}"
        return error_msg
