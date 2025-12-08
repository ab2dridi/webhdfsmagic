"""
Directory operation commands for WebHDFS.

This module implements commands for directory operations:
- ls: List directory contents
- mkdir: Create directories
- rm: Remove files/directories
"""

import fnmatch
from typing import Union

import pandas as pd

from ..utils import format_file_entry
from .base import BaseCommand


class ListCommand(BaseCommand):
    """List directory contents."""

    def execute(self, path: str) -> Union[pd.DataFrame, dict]:
        """
        List files and directories in HDFS path.

        Args:
            path: HDFS directory path

        Returns:
            DataFrame with file information or dict for empty directory

        Example:
            >>> cmd = ListCommand(client)
            >>> df = cmd.execute("/user/hadoop")
            >>> print(df.columns)
            Index(['name', 'type', 'size', 'owner', 'group', 'permissions',
                   'block_size', 'modified', 'replication'], dtype='object')
        """
        result = self.client.execute("GET", "LISTSTATUS", path)

        file_statuses = result.get("FileStatuses", {}).get("FileStatus", [])

        # Check if directory is empty
        if not file_statuses:
            return {"empty_dir": True, "path": path}

        # Format entries
        entries = [format_file_entry(f) for f in file_statuses]

        return pd.DataFrame(entries)


class MkdirCommand(BaseCommand):
    """Create directory on HDFS."""

    def execute(self, path: str) -> str:
        """
        Create a directory on HDFS.

        Args:
            path: HDFS directory path to create

        Returns:
            Success message

        Example:
            >>> cmd = MkdirCommand(client)
            >>> result = cmd.execute("/data/2025")
            'Directory /data/2025 created.'
        """
        self.client.execute("PUT", "MKDIRS", path)
        return f"Directory {path} created."


class RmCommand(BaseCommand):
    """Remove files/directories from HDFS."""

    def execute(
        self,
        pattern: str,
        recursive: bool = False,
        format_ls_func: callable = None
    ) -> str:
        """
        Remove file(s) or directory from HDFS.

        Supports wildcards for batch deletion.

        Args:
            pattern: HDFS path or pattern (supports * and ?)
            recursive: If True, delete directories recursively
            format_ls_func: Function to list directory (required for wildcards)

        Returns:
            Success/error message(s)

        Example:
            >>> cmd = RmCommand(client)
            >>> result = cmd.execute("/data/old/*.csv", recursive=True, format_ls_func=ls_func)
            'file1.csv deleted\\nfile2.csv deleted'
        """
        import os

        # Handle wildcards
        if "*" in pattern or "?" in pattern:
            if not format_ls_func:
                raise ValueError("format_ls_func required for wildcard deletion")

            base_dir = os.path.dirname(pattern) or "/"
            file_pattern = os.path.basename(pattern)
            result = format_ls_func(base_dir)

            # Handle empty directory
            if isinstance(result, dict) and result.get("empty_dir"):
                return f"No files match the pattern {pattern}"

            df = result
            matching_files = df[df["name"].apply(lambda x: fnmatch.fnmatch(x, file_pattern))]

            if matching_files.empty:
                return f"No files match the pattern {pattern}"

            responses = []
            for _, row in matching_files.iterrows():
                file_path = f"{base_dir.rstrip('/')}/{row['name']}"
                try:
                    recursive_val = "true" if recursive else "false"
                    self.client.execute("DELETE", "DELETE", file_path, recursive=recursive_val)
                    responses.append(f"{file_path} deleted")
                except Exception as e:
                    responses.append(f"Error deleting {file_path}: {str(e)}")
            return "\n".join(responses)
        else:
            # Single file/directory
            recursive_val = "true" if recursive else "false"
            self.client.execute("DELETE", "DELETE", pattern, recursive=recursive_val)
            return f"{pattern} deleted"
