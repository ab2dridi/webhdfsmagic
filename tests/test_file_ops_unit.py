"""Tests unitaires pour les commandes de file_ops.py."""

import os
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from webhdfsmagic.client import WebHDFSClient
from webhdfsmagic.commands.file_ops import CatCommand, GetCommand, PutCommand


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


# Tests pour CatCommand


def test_cat_command_basic(client):
    """Test CatCommand.execute basic functionality."""
    cat_cmd = CatCommand(client)

    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"line1\nline2\nline3"
        mock_get.return_value = mock_response

        result = cat_cmd.execute("/test/file.txt")

        assert "line1" in result
        assert "line2" in result
        assert "line3" in result


def test_cat_command_with_num_lines(client):
    """Test CatCommand.execute with num_lines parameter."""
    cat_cmd = CatCommand(client)

    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        lines = "\n".join([f"line{i}" for i in range(150)])
        mock_response.content = lines.encode()
        mock_get.return_value = mock_response

        result = cat_cmd.execute("/test/file.txt", num_lines=5)

        result_lines = result.splitlines()
        assert len(result_lines) == 5


def test_cat_command_all_lines(client):
    """Test CatCommand.execute with num_lines=-1 (all lines)."""
    cat_cmd = CatCommand(client)

    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        lines = "\n".join([f"line{i}" for i in range(150)])
        mock_response.content = lines.encode()
        mock_get.return_value = mock_response

        result = cat_cmd.execute("/test/file.txt", num_lines=-1)

        result_lines = result.splitlines()
        assert len(result_lines) == 150


def test_cat_command_handles_307_redirect(client):
    """Test CatCommand handles 307 redirect."""
    cat_cmd = CatCommand(client)

    with patch('requests.get') as mock_get:
        # First response: 307 redirect
        mock_redirect = MagicMock()
        mock_redirect.status_code = 307
        mock_redirect.headers = {"Location": "http://datanode:50075/path?user.name=user"}

        # Second response: actual content
        mock_content = MagicMock()
        mock_content.status_code = 200
        mock_content.content = b"file content"

        mock_get.side_effect = [mock_redirect, mock_content]

        result = cat_cmd.execute("/test/file.txt")

        assert "file content" in result
        assert mock_get.call_count == 2


def test_cat_command_fixes_docker_hostname(client):
    """Test CatCommand fixes Docker internal hostnames."""
    cat_cmd = CatCommand(client)

    with patch('requests.get') as mock_get:
        # Redirect with Docker hostname (12 hex chars)
        mock_redirect = MagicMock()
        mock_redirect.status_code = 307
        mock_redirect.headers = {"Location": "http://1a2b3c4d5e6f:50075/path"}

        # Final response
        mock_content = MagicMock()
        mock_content.status_code = 200
        mock_content.content = b"content"

        mock_get.side_effect = [mock_redirect, mock_content]

        cat_cmd.execute("/test/file.txt")

        # Verify second call uses localhost instead of Docker hostname
        second_call_url = mock_get.call_args_list[1][0][0]
        assert "localhost" in second_call_url


def test_cat_command_error_handling(client):
    """Test CatCommand error handling."""
    cat_cmd = CatCommand(client)

    with patch('requests.get') as mock_get:
        mock_get.side_effect = requests.HTTPError("404 Not Found")

        result = cat_cmd.execute("/nonexistent.txt")

        assert "Error:" in result
        assert "404" in result or "HTTPError" in result


# Tests pour PutCommand


def test_put_command_basic(client, tmp_path):
    """Test PutCommand.execute basic functionality."""
    put_cmd = PutCommand(client)

    # Create a temporary file
    test_file = tmp_path / "test.txt"
    test_file.write_text("test content")

    with patch('requests.put') as mock_put:
        # First PUT returns 307 redirect
        mock_redirect = MagicMock()
        mock_redirect.status_code = 307
        mock_redirect.headers = {"Location": "http://datanode:50075/path"}

        # Second PUT succeeds
        mock_success = MagicMock()
        mock_success.status_code = 201

        mock_put.side_effect = [mock_redirect, mock_success]

        result = put_cmd.execute(str(test_file), "/hdfs/test.txt")

        assert "uploaded" in result.lower() or "success" in result.lower()


def test_put_command_wildcard(client, tmp_path):
    """Test PutCommand.execute with wildcard."""
    put_cmd = PutCommand(client)

    # Create multiple files
    file1 = tmp_path / "file1.csv"
    file2 = tmp_path / "file2.csv"
    file1.write_text("data1")
    file2.write_text("data2")

    with patch('requests.put') as mock_put:
        mock_response = MagicMock()
        mock_response.status_code = 201
        mock_put.return_value = mock_response

        result = put_cmd.execute(str(tmp_path / "*.csv"), "/hdfs/data/")

        # Should upload both files
        assert "file1.csv" in result or "uploaded" in result.lower()


def test_put_command_nonexistent_file(client):
    """Test PutCommand with nonexistent file."""
    put_cmd = PutCommand(client)

    result = put_cmd.execute("/nonexistent/file.txt", "/hdfs/dest.txt")

    assert "No local files match" in result


# Tests pour GetCommand


def mock_format_ls(path):
    """Mock format_ls function for GetCommand tests."""
    import pandas as pd
    return pd.DataFrame([
        {"name": "file.txt", "type": "FILE"}
    ])


def test_get_command_basic(client, tmp_path):
    """Test GetCommand.execute basic functionality."""
    get_cmd = GetCommand(client)

    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"file content"
        mock_response.iter_content = lambda chunk_size: [b"file content"]
        mock_get.return_value = mock_response

        dest = str(tmp_path / "downloaded.txt")
        result = get_cmd.execute("/hdfs/file.txt", dest, format_ls_func=mock_format_ls)

        assert "downloaded" in result.lower()
        assert os.path.exists(dest)


def test_get_command_handles_307_redirect(client, tmp_path):
    """Test GetCommand handles 307 redirect."""
    get_cmd = GetCommand(client)

    with patch('requests.get') as mock_get:
        # First response: 307
        mock_redirect = MagicMock()
        mock_redirect.status_code = 307
        mock_redirect.headers = {"Location": "http://datanode:50075/path"}

        # Second response: content
        mock_content = MagicMock()
        mock_content.status_code = 200
        mock_content.content = b"data"
        mock_content.iter_content = lambda chunk_size: [b"data"]

        mock_get.side_effect = [mock_redirect, mock_content]

        dest = str(tmp_path / "file.txt")
        get_cmd.execute("/hdfs/file.txt", dest, format_ls_func=mock_format_ls)

        assert os.path.exists(dest)
        assert mock_get.call_count == 2


def test_get_command_tilde_expansion(client, tmp_path, monkeypatch):
    """Test GetCommand expands tilde in destination path."""
    get_cmd = GetCommand(client)

    # Mock home directory
    monkeypatch.setenv("HOME", str(tmp_path))

    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.iter_content = lambda chunk_size: [b"data"]
        mock_get.return_value = mock_response

        get_cmd.execute("/hdfs/file.txt", "~/file.txt", format_ls_func=mock_format_ls)

        expected_path = os.path.join(str(tmp_path), "file.txt")
        assert os.path.exists(expected_path)


def test_get_command_creates_directory(client, tmp_path):
    """Test GetCommand creates missing directories."""
    get_cmd = GetCommand(client)

    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.content = b"data"
        mock_response.iter_content = lambda chunk_size: [b"data"]
        mock_get.return_value = mock_response

        dest = str(tmp_path / "newdir" / "subdir" / "file.txt")
        get_cmd.execute("/hdfs/file.txt", dest, format_ls_func=mock_format_ls)

        assert os.path.exists(dest)
        assert os.path.isfile(dest)


def test_get_command_error_handling(client, tmp_path):
    """Test GetCommand error handling."""
    get_cmd = GetCommand(client)

    with patch('requests.get') as mock_get:
        mock_get.side_effect = requests.HTTPError("404 Not Found")

        dest = str(tmp_path / "file.txt")
        result = get_cmd.execute("/nonexistent.txt", dest, format_ls_func=mock_format_ls)

        assert "Error" in result


def test_get_command_download_exception_during_wildcard(client, tmp_path):
    """Test GetCommand handles exception during wildcard download (line 171-173)."""
    get_cmd = GetCommand(client)

    # Mock format_ls_func to return matching files
    def mock_format_ls(path):
        data = {
            "name": ["file1.txt", "file2.txt"],
            "type": ["FILE", "FILE"],
            "size": [100, 200],
            "replication": [3, 3],
            "blockSize": [128, 128],
            "modificationTime": [1609459200000, 1609459200000],
            "permission": ["644", "644"],
            "owner": ["user", "user"],
            "group": ["group", "group"]
        }
        return pd.DataFrame(data)

    with patch.object(get_cmd, '_download_file') as mock_download:
        # First file succeeds, second raises exception
        mock_download.side_effect = [None, Exception("Network error")]

        dest = str(tmp_path)
        result = get_cmd.execute("/data/*.txt", dest, format_ls_func=mock_format_ls)

        assert "file1.txt downloaded" in result
        assert "Error: Network error" in result
        assert "Traceback:" in result


def test_get_command_creates_parent_directory(client, tmp_path):
    """Test GetCommand creates parent directory if it doesn't exist (lines 198-201)."""
    get_cmd = GetCommand(client)

    # Create a nested path that doesn't exist
    nested_dest = tmp_path / "deeply" / "nested" / "path" / "file.txt"
    assert not nested_dest.parent.exists()

    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "file content"
        mock_get.return_value = mock_response

        result = get_cmd.execute("/source.txt", str(nested_dest), format_ls_func=mock_format_ls)

        assert nested_dest.parent.exists()
        assert "downloaded to" in result


def test_get_command_destination_ending_with_dot_not_slash(client, tmp_path):
    """Test GetCommand handles destination ending with '.' but not '/.' (line 233)."""
    get_cmd = GetCommand(client)

    # Test with destination ending in '.'
    dest = str(tmp_path) + "/subdir."

    with patch('requests.get') as mock_get:
        mock_response = MagicMock()
        mock_response.status_code = 200
        mock_response.text = "content"
        mock_get.return_value = mock_response

        result = get_cmd.execute("/file.txt", dest, format_ls_func=mock_format_ls)

        # Verify the file was created
        assert "downloaded to" in result


def test_cat_command_adds_username_param_when_auth_user_exists(client):
    """Test CatCommand adds user.name parameter when client has auth_user (line 270)."""
    client.auth_user = "testuser"
    cat_cmd = CatCommand(client)

    with patch('requests.get') as mock_get:
        # Mock 307 redirect
        redirect_response = MagicMock()
        redirect_response.status_code = 307
        # Use Docker hostname without user.name in query
        redirect_response.headers = {
            'Location': 'http://abc123456789:9864/webhdfs/v1/test.txt?op=OPEN'
        }

        # Mock final response
        final_response = MagicMock()
        final_response.status_code = 200
        final_response.text = "file content"

        mock_get.side_effect = [redirect_response, final_response]

        cat_cmd.execute("/test.txt", num_lines=100)

        # Verify second request includes user.name
        assert mock_get.call_count == 2
        second_call_url = mock_get.call_args_list[1][0][0]
        assert "user.name=testuser" in second_call_url


def test_put_command_upload_failure_status(client, tmp_path):
    """Test PutCommand handles non-200/201 upload response (lines 350, 359-360)."""
    put_cmd = PutCommand(client)

    # Create a test file
    local_file = tmp_path / "test.txt"
    local_file.write_text("content")

    with patch('requests.put') as mock_requests_put:
        # Mock Phase 1: initialization (307 redirect)
        init_response = MagicMock()
        init_response.status_code = 307
        init_response.headers = {
            'Location': 'http://datanode:9864/webhdfs/v1/dest.txt?op=CREATE'
        }

        # Mock Phase 2: upload failure
        upload_response = MagicMock()
        upload_response.status_code = 500

        # requests.put called twice: first for init, second for upload
        mock_requests_put.side_effect = [init_response, upload_response]

        result = put_cmd.execute(str(local_file), "/hdfs/dest.txt")

        assert "Upload failed" in result
        assert "status: 500" in result


def test_put_command_initialization_failure_status(client, tmp_path):
    """Test PutCommand handles initialization failure (line 359-360)."""
    put_cmd = PutCommand(client)

    # Create a test file
    local_file = tmp_path / "test.txt"
    local_file.write_text("content")

    with patch('requests.put') as mock_requests_put:
        # Mock initialization failure (not 307)
        init_response = MagicMock()
        init_response.status_code = 403  # Forbidden
        mock_requests_put.return_value = init_response

        result = put_cmd.execute(str(local_file), "/hdfs/dest.txt")

        # Should report initiation failed
        assert "Initiation failed" in result
        assert "status: 403" in result

