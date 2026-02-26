"""Tests for permission operations commands (chmod, chown)."""

from unittest.mock import patch

import pandas as pd
import pytest

from webhdfsmagic.client import WebHDFSClient
from webhdfsmagic.commands.permission_ops import ChmodCommand, ChownCommand


def test_chmod_recursive(monkeypatch, magics_instance):
    """Test the chmod -R command for recursive permission changes."""

    # Mock _format_ls to return different data based on path
    def mock_format_ls(path):
        if path == "/demo":
            return pd.DataFrame(
                [
                    {
                        "name": "subdir",
                        "type": "DIR",
                        "permission": "755",
                        "owner": "user",
                        "group": "group",
                        "size": 0,
                        "modified": "2025-01-01",
                        "replication": 0,
                        "permissions": "rwxr-xr-x",
                        "block_size": 0,
                    },
                    {
                        "name": "file.txt",
                        "type": "FILE",
                        "permission": "644",
                        "owner": "user",
                        "group": "group",
                        "size": 100,
                        "modified": "2025-01-01",
                        "replication": 3,
                        "permissions": "rw-r--r--",
                        "block_size": 134217728,
                    },
                ]
            )
        elif path == "/demo/subdir":
            return pd.DataFrame(
                [
                    {
                        "name": "nested_file.txt",
                        "type": "FILE",
                        "permission": "644",
                        "owner": "user",
                        "group": "group",
                        "size": 50,
                        "modified": "2025-01-01",
                        "replication": 3,
                        "permissions": "rw-r--r--",
                        "block_size": 134217728,
                    },
                ]
            )
        else:
            return pd.DataFrame()

    monkeypatch.setattr(magics_instance, "_format_ls", mock_format_ls)

    # Mock client.execute to track permission calls
    execute_calls = []

    def mock_execute(method, operation, path, **params):
        execute_calls.append((method, operation, path, params))
        return {}  # Return empty dict for SETPERMISSION

    monkeypatch.setattr(magics_instance.client, "execute", mock_execute)

    # Test chmod -R
    result = magics_instance.hdfs("chmod -R 777 /demo")
    assert "Recursive chmod 777 applied on /demo" in result

    # Verify that permissions were set for root, subdirectory, files
    assert len(execute_calls) >= 3  # Root, subdir, and files
    paths_set = [call[2] for call in execute_calls if call[1] == "SETPERMISSION"]
    assert "/demo" in paths_set
    assert "/demo/subdir" in paths_set
    assert "/demo/file.txt" in paths_set


def test_chmod_non_recursive(monkeypatch, magics_instance):
    """Test the chmod command without -R flag."""

    # Mock client.execute
    def mock_execute(method, operation, path, **params):
        return {}

    monkeypatch.setattr(magics_instance.client, "execute", mock_execute)

    # Test chmod without -R
    result = magics_instance.hdfs("chmod 755 /demo/file.txt")
    assert "Permission 755 set for /demo/file.txt" in result


def test_chown_recursive(monkeypatch, magics_instance):
    """Test the chown -R command for recursive owner/group changes."""

    # Mock _format_ls to return different data based on path
    def mock_format_ls(path):
        if path == "/demo":
            return pd.DataFrame(
                [
                    {
                        "name": "subdir",
                        "type": "DIR",
                        "permission": "755",
                        "owner": "olduser",
                        "group": "oldgroup",
                        "size": 0,
                        "modified": "2025-01-01",
                        "replication": 0,
                        "permissions": "rwxr-xr-x",
                        "block_size": 0,
                    },
                    {
                        "name": "file.txt",
                        "type": "FILE",
                        "permission": "644",
                        "owner": "olduser",
                        "group": "oldgroup",
                        "size": 100,
                        "modified": "2025-01-01",
                        "replication": 3,
                        "permissions": "rw-r--r--",
                        "block_size": 134217728,
                    },
                ]
            )
        elif path == "/demo/subdir":
            return pd.DataFrame(
                [
                    {
                        "name": "nested_file.txt",
                        "type": "FILE",
                        "permission": "644",
                        "owner": "olduser",
                        "group": "oldgroup",
                        "size": 50,
                        "modified": "2025-01-01",
                        "replication": 3,
                        "permissions": "rw-r--r--",
                        "block_size": 134217728,
                    },
                ]
            )
        else:
            return pd.DataFrame()

    monkeypatch.setattr(magics_instance, "_format_ls", mock_format_ls)

    # Mock client.execute to track calls
    execute_calls = []

    def mock_execute(method, operation, path, **params):
        execute_calls.append((method, operation, path, params))
        return {}

    monkeypatch.setattr(magics_instance.client, "execute", mock_execute)

    # Test chown -R with owner:group
    result = magics_instance.hdfs("chown -R newuser:newgroup /demo")
    assert "Recursive chown newuser:newgroup applied on /demo" in result

    # Verify that owner/group were set for root, subdirectory, files
    assert len(execute_calls) >= 3  # Root, subdir, and files
    paths_set = [call[2] for call in execute_calls if call[1] == "SETOWNER"]
    assert "/demo" in paths_set
    assert "/demo/subdir" in paths_set
    assert "/demo/file.txt" in paths_set


def test_chown_recursive_owner_only(monkeypatch, magics_instance):
    """Test the chown -R command with only owner (no group)."""

    # Mock _format_ls to return a simple file structure
    def mock_format_ls(path):
        if path == "/demo":
            return pd.DataFrame(
                [
                    {
                        "name": "file.txt",
                        "type": "FILE",
                        "permission": "644",
                        "owner": "olduser",
                        "group": "oldgroup",
                        "size": 100,
                        "modified": "2025-01-01",
                        "replication": 3,
                        "permissions": "rw-r--r--",
                        "block_size": 134217728,
                    },
                ]
            )
        else:
            return pd.DataFrame()

    monkeypatch.setattr(magics_instance, "_format_ls", mock_format_ls)

    # Mock client.execute to track calls
    execute_calls = []

    def mock_execute(method, operation, path, **params):
        execute_calls.append((method, operation, path, params))
        return {}

    monkeypatch.setattr(magics_instance.client, "execute", mock_execute)

    # Test chown -R with owner only (no group)
    result = magics_instance.hdfs("chown -R newuser /demo")
    assert "Recursive chown newuser:None applied on /demo" in result

    # Verify that owner was set
    paths_set = [call[2] for call in execute_calls if call[1] == "SETOWNER"]
    assert "/demo" in paths_set
    assert "/demo/file.txt" in paths_set


def test_chown_non_recursive(monkeypatch, magics_instance):
    """Test the chown command without -R flag."""

    # Mock client.execute
    def mock_execute(method, operation, path, **params):
        return {}

    monkeypatch.setattr(magics_instance.client, "execute", mock_execute)

    # Test chown without -R
    result = magics_instance.hdfs("chown hdfs:hadoop /demo/file.txt")
    assert "Owner hdfs:hadoop set for /demo/file.txt" in result


# Tests unitaires directs pour ChmodCommand


@pytest.fixture
def client():
    """Create test client."""
    return WebHDFSClient(
        knox_url="http://test",
        webhdfs_api="/api",
        auth_user="user",
        auth_password="pass",
        verify_ssl=False,
    )


def test_chmod_command_non_recursive(client):
    """Test ChmodCommand.execute without recursion."""
    chmod_cmd = ChmodCommand(client)

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {}

        result = chmod_cmd.execute("/test/file.txt", "644", recursive=False)

        assert "Permission 644 set for /test/file.txt" in result
        mock_execute.assert_called_once_with(
            "PUT", "SETPERMISSION", "/test/file.txt", permission="644"
        )


def test_chmod_command_recursive_requires_format_ls_func(client):
    """Test ChmodCommand.execute raises error without format_ls_func."""
    chmod_cmd = ChmodCommand(client)

    with pytest.raises(ValueError, match="format_ls_func required"):
        chmod_cmd.execute("/test", "755", recursive=True, format_ls_func=None)


def test_chmod_command_recursive_with_empty_dir(client):
    """Test ChmodCommand.execute handles empty directory."""
    chmod_cmd = ChmodCommand(client)

    def mock_format_ls(path):
        return {"empty_dir": True, "path": path}

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {}

        result = chmod_cmd.execute("/empty", "755", recursive=True, format_ls_func=mock_format_ls)

        assert "Recursive chmod 755 applied on /empty" in result
        # Should only set permission on root directory
        assert mock_execute.call_count == 1


def test_chmod_command_recursive_with_exception(client):
    """Test ChmodCommand.execute handles exceptions in format_ls_func."""
    chmod_cmd = ChmodCommand(client)

    def mock_format_ls(path):
        raise Exception("Error listing directory")

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {}

        result = chmod_cmd.execute("/test", "755", recursive=True, format_ls_func=mock_format_ls)

        # Should still succeed and set permission on root
        assert "Recursive chmod 755 applied on /test" in result


# Tests unitaires directs pour ChownCommand


def test_chown_command_non_recursive(client):
    """Test ChownCommand.execute without recursion."""
    chown_cmd = ChownCommand(client)

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {}

        result = chown_cmd.execute("/test/file.txt", "hadoop", "supergroup", recursive=False)

        assert "Owner hadoop:supergroup set for /test/file.txt" in result
        mock_execute.assert_called_once_with(
            "PUT", "SETOWNER", "/test/file.txt", owner="hadoop", group="supergroup"
        )


def test_chown_command_owner_only(client):
    """Test ChownCommand.execute with owner only."""
    chown_cmd = ChownCommand(client)

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {}

        result = chown_cmd.execute("/test/file.txt", "hadoop", group=None, recursive=False)

        assert "Owner hadoop:None set for /test/file.txt" in result
        mock_execute.assert_called_once_with(
            "PUT", "SETOWNER", "/test/file.txt", owner="hadoop", group=None
        )


def test_chown_command_recursive_requires_format_ls_func(client):
    """Test ChownCommand.execute raises error without format_ls_func."""
    chown_cmd = ChownCommand(client)

    with pytest.raises(ValueError, match="format_ls_func required"):
        chown_cmd.execute("/test", "hadoop", "supergroup", recursive=True, format_ls_func=None)


def test_chown_command_recursive_with_empty_dir(client):
    """Test ChownCommand.execute handles empty directory."""
    chown_cmd = ChownCommand(client)

    def mock_format_ls(path):
        return {"empty_dir": True, "path": path}

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {}

        result = chown_cmd.execute(
            "/empty", "user", "group", recursive=True, format_ls_func=mock_format_ls
        )

        assert "Recursive chown user:group applied on /empty" in result
        # Should only set owner on root directory
        assert mock_execute.call_count == 1


def test_chown_command_recursive_with_exception(client):
    """Test ChownCommand.execute handles exceptions in format_ls_func."""
    chown_cmd = ChownCommand(client)

    def mock_format_ls(path):
        raise Exception("Error listing directory")

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {}

        result = chown_cmd.execute(
            "/test", "user", "group", recursive=True, format_ls_func=mock_format_ls
        )

        # Should still succeed and set owner on root
        assert "Recursive chown user:group applied on /test" in result
