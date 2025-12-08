"""Tests for directory operations commands (ls, mkdir, rm)."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from webhdfsmagic.client import WebHDFSClient
from webhdfsmagic.commands.directory_ops import ListCommand, MkdirCommand, RmCommand


def test_ls(monkeypatch, magics_instance):
    """Test the ls command by mocking the LISTSTATUS response."""
    fake_response = MagicMock()
    fake_data = {
        "FileStatuses": {
            "FileStatus": [
                {
                    "pathSuffix": "file1.txt",
                    "type": "FILE",
                    "permission": "755",
                    "owner": "hdfs",
                    "group": "hdfs",
                    "modificationTime": 1600000000000,
                    "length": 1024,
                    "blockSize": 134217728,
                    "replication": 3,
                }
            ]
        }
    }
    fake_response.content = json.dumps(fake_data).encode("utf-8")
    fake_response.status_code = 200
    fake_response.json.return_value = fake_data
    fake_response.raise_for_status = MagicMock()

    def flexible_request(*args, **kwargs):
        return fake_response

    monkeypatch.setattr(requests, "request", flexible_request)
    df = magics_instance._format_ls("/fake-dir")
    assert len(df) == 1


def test_ls_empty_directory(monkeypatch, magics_instance):
    """Test the ls command on an empty directory - should return {'empty_dir': True}."""
    fake_response = MagicMock()
    fake_data = {
        "FileStatuses": {
            "FileStatus": []  # Empty directory
        }
    }
    fake_response.content = json.dumps(fake_data).encode("utf-8")
    fake_response.status_code = 200
    fake_response.json.return_value = fake_data
    fake_response.raise_for_status = MagicMock()

    def flexible_request(*args, **kwargs):
        return fake_response

    monkeypatch.setattr(requests, "request", flexible_request)
    result = magics_instance.hdfs("ls /empty-dir")
    assert isinstance(result, dict)
    assert result["empty_dir"] is True
    assert result["path"] == "/empty-dir"


# Tests unitaires pour ListCommand


@pytest.fixture
def client():
    """Create test client."""
    return WebHDFSClient(
        knox_url="http://test",
        webhdfs_api="/api",
        auth_user="user",
        auth_password="pass",
        verify_ssl=False
    )


def test_list_command_execute(client):
    """Test ListCommand.execute with files."""
    list_cmd = ListCommand(client)

    with patch.object(client, 'execute') as mock_execute:
        mock_execute.return_value = {
            "FileStatuses": {
                "FileStatus": [
                    {
                        "pathSuffix": "file1.txt",
                        "type": "FILE",
                        "permission": "644",
                        "owner": "user",
                        "group": "group",
                        "modificationTime": 1609459200000,
                        "length": 1024,
                        "blockSize": 134217728,
                        "replication": 3
                    }
                ]
            }
        }

        result = list_cmd.execute("/test")

        assert isinstance(result, pd.DataFrame)
        assert len(result) == 1
        assert result.iloc[0]["name"] == "file1.txt"


def test_list_command_empty_directory(client):
    """Test ListCommand.execute with empty directory."""
    list_cmd = ListCommand(client)

    with patch.object(client, 'execute') as mock_execute:
        mock_execute.return_value = {
            "FileStatuses": {
                "FileStatus": []
            }
        }

        result = list_cmd.execute("/empty")

        assert isinstance(result, dict)
        assert result["empty_dir"] is True
        assert result["path"] == "/empty"


# Tests unitaires pour MkdirCommand


def test_mkdir_command_execute(client):
    """Test MkdirCommand.execute."""
    mkdir_cmd = MkdirCommand(client)

    with patch.object(client, 'execute') as mock_execute:
        mock_execute.return_value = {"boolean": True}

        result = mkdir_cmd.execute("/test/newdir")

        assert "Directory /test/newdir created" in result
        mock_execute.assert_called_once_with("PUT", "MKDIRS", "/test/newdir")


def test_mkdir_command_with_nested_path(client):
    """Test MkdirCommand.execute with nested path."""
    mkdir_cmd = MkdirCommand(client)

    with patch.object(client, 'execute') as mock_execute:
        mock_execute.return_value = {"boolean": True}

        result = mkdir_cmd.execute("/a/b/c/d")

        assert "Directory /a/b/c/d created" in result


# Tests unitaires pour RmCommand


def test_rm_command_single_file(client):
    """Test RmCommand.execute with single file."""
    rm_cmd = RmCommand(client)

    with patch.object(client, 'execute') as mock_execute:
        mock_execute.return_value = {"boolean": True}

        result = rm_cmd.execute("/test/file.txt", recursive=False)

        assert "/test/file.txt deleted" in result
        mock_execute.assert_called_once_with(
            "DELETE", "DELETE", "/test/file.txt", recursive="false"
        )


def test_rm_command_recursive(client):
    """Test RmCommand.execute with recursive flag."""
    rm_cmd = RmCommand(client)

    with patch.object(client, 'execute') as mock_execute:
        mock_execute.return_value = {"boolean": True}

        result = rm_cmd.execute("/test/dir", recursive=True)

        assert "/test/dir deleted" in result
        mock_execute.assert_called_once_with(
            "DELETE", "DELETE", "/test/dir", recursive="true"
        )


def test_rm_command_wildcard_with_matches(client):
    """Test RmCommand.execute with wildcard matching files."""
    rm_cmd = RmCommand(client)

    def mock_format_ls(path):
        return pd.DataFrame([
            {"name": "file1.csv", "type": "FILE"},
            {"name": "file2.csv", "type": "FILE"},
            {"name": "other.txt", "type": "FILE"}
        ])

    with patch.object(client, 'execute') as mock_execute:
        mock_execute.return_value = {"boolean": True}

        result = rm_cmd.execute("/data/*.csv", recursive=False, format_ls_func=mock_format_ls)

        assert "file1.csv deleted" in result
        assert "file2.csv deleted" in result
        assert "other.txt" not in result


def test_rm_command_wildcard_no_matches(client):
    """Test RmCommand.execute with wildcard no matches."""
    rm_cmd = RmCommand(client)

    def mock_format_ls(path):
        return pd.DataFrame([
            {"name": "file1.txt", "type": "FILE"}
        ])

    result = rm_cmd.execute("/data/*.csv", recursive=False, format_ls_func=mock_format_ls)

    assert "No files match the pattern" in result


def test_rm_command_wildcard_empty_directory(client):
    """Test RmCommand.execute with wildcard in empty directory."""
    rm_cmd = RmCommand(client)

    def mock_format_ls(path):
        return {"empty_dir": True, "path": path}

    result = rm_cmd.execute("/empty/*.csv", recursive=False, format_ls_func=mock_format_ls)

    assert "No files match the pattern" in result


def test_rm_command_wildcard_without_format_ls_func(client):
    """Test RmCommand.execute with wildcard but no format_ls_func raises error."""
    rm_cmd = RmCommand(client)

    with pytest.raises(ValueError, match="format_ls_func required"):
        rm_cmd.execute("/data/*.csv", recursive=False)


def test_rm_command_wildcard_with_error(client):
    """Test RmCommand.execute with wildcard where some deletions fail."""
    rm_cmd = RmCommand(client)

    def mock_format_ls(path):
        return pd.DataFrame([
            {"name": "file1.csv", "type": "FILE"},
            {"name": "file2.csv", "type": "FILE"}
        ])

    def mock_execute_with_error(method, op, path, **kwargs):
        if "file2" in path:
            raise Exception("Permission denied")
        return {"boolean": True}

    with patch.object(client, 'execute', side_effect=mock_execute_with_error):
        result = rm_cmd.execute("/data/*.csv", recursive=False, format_ls_func=mock_format_ls)

        assert "file1.csv deleted" in result
        assert "Error deleting" in result
        assert "file2.csv" in result

