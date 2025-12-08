"""
Utility functions for WebHDFS Magic.

This module provides common utility functions for formatting,
path manipulation, and data processing.
"""

from datetime import datetime
from typing import Any


def format_permissions(perm: int) -> str:
    """
    Convert a numeric permission to a UNIX-style string (e.g., "rwx").

    Args:
        perm: Permission as an integer (0-7)

    Returns:
        Permission string (e.g., "rwx", "r--", "r-x")

    Example:
        >>> format_permissions(7)
        'rwx'
        >>> format_permissions(4)
        'r--'
        >>> format_permissions(5)
        'r-x'
    """
    return "".join(
        ["r" if perm & 4 else "-", "w" if perm & 2 else "-", "x" if perm & 1 else "-"]
    )


def format_full_permissions(permission_int: int) -> str:
    """
    Convert octal permission to full UNIX-style string.

    Args:
        permission_int: Octal permission as integer (e.g., 0o755)

    Returns:
        Full permission string (e.g., "rwxr-xr-x")

    Example:
        >>> format_full_permissions(0o755)
        'rwxr-xr-x'
        >>> format_full_permissions(0o644)
        'rw-r--r--'
    """
    return (
        f"{format_permissions((permission_int >> 6) & 7)}"
        f"{format_permissions((permission_int >> 3) & 7)}"
        f"{format_permissions(permission_int & 7)}"
    )


def format_size(size_bytes: int, human_readable: bool = False) -> str:
    """
    Format file size for display.

    Args:
        size_bytes: Size in bytes
        human_readable: If True, convert to human-readable format (KB, MB, GB)

    Returns:
        Formatted size string

    Example:
        >>> format_size(1024)
        '1024'
        >>> format_size(1024, human_readable=True)
        '1.0 KB'
        >>> format_size(1048576, human_readable=True)
        '1.0 MB'
    """
    if not human_readable:
        return str(size_bytes)

    for unit in ["B", "KB", "MB", "GB", "TB"]:
        if size_bytes < 1024.0:
            return f"{size_bytes:.1f} {unit}"
        size_bytes /= 1024.0
    return f"{size_bytes:.1f} PB"


def format_timestamp(timestamp_ms: int) -> datetime:
    """
    Convert HDFS timestamp (milliseconds) to datetime.

    Args:
        timestamp_ms: Timestamp in milliseconds since epoch

    Returns:
        datetime object

    Example:
        >>> format_timestamp(1609459200000)
        datetime.datetime(2021, 1, 1, 0, 0)
    """
    return datetime.fromtimestamp(timestamp_ms / 1000)


def parse_hdfs_path(path: str) -> tuple[str, str, str]:
    """
    Parse HDFS path into components.

    Args:
        path: HDFS path (e.g., "/user/hadoop/data/file.csv")

    Returns:
        Tuple of (directory, filename, extension)

    Example:
        >>> parse_hdfs_path("/user/hadoop/data/file.csv")
        ('/user/hadoop/data', 'file.csv', '.csv')
        >>> parse_hdfs_path("/user/hadoop/data/")
        ('/user/hadoop/data', '', '')
    """
    import os

    directory = os.path.dirname(path)
    filename = os.path.basename(path)
    _, extension = os.path.splitext(filename) if filename else ("", "")

    return directory, filename, extension


def normalize_hdfs_path(path: str) -> str:
    """
    Normalize HDFS path (remove trailing slashes, etc.).

    Args:
        path: HDFS path

    Returns:
        Normalized path

    Example:
        >>> normalize_hdfs_path("/user/hadoop/data/")
        '/user/hadoop/data'
        >>> normalize_hdfs_path("/user//hadoop///data")
        '/user/hadoop/data'
    """
    # Remove trailing slashes except for root
    normalized = path.rstrip("/") if path != "/" else path
    # Remove double slashes
    while "//" in normalized:
        normalized = normalized.replace("//", "/")
    return normalized


def format_file_entry(file_status: dict[str, Any]) -> dict[str, Any]:
    """
    Format a WebHDFS FileStatus entry for display.

    Args:
        file_status: FileStatus dictionary from WebHDFS API

    Returns:
        Formatted entry dictionary

    Example:
        >>> entry = {
        ...     "pathSuffix": "file.csv",
        ...     "type": "FILE",
        ...     "length": 1024,
        ...     "owner": "hadoop",
        ...     "group": "supergroup",
        ...     "permission": "644",
        ...     "modificationTime": 1609459200000,
        ...     "replication": 3,
        ...     "blockSize": 134217728
        ... }
        >>> result = format_file_entry(entry)
        >>> result['name']
        'file.csv'
        >>> result['type']
        'FILE'
    """
    permission_int = int(file_status["permission"], 8)

    return {
        "name": file_status["pathSuffix"],
        "type": "DIR" if file_status["type"] == "DIRECTORY" else "FILE",
        "size": file_status.get("length", 0),
        "owner": file_status["owner"],
        "group": file_status["group"],
        "permissions": format_full_permissions(permission_int),
        "block_size": file_status.get("blockSize", 0),
        "modified": format_timestamp(file_status["modificationTime"]),
        "replication": file_status.get("replication", 1),
    }
