"""Tests for file operations commands (cat, get, put)."""

import os
import tempfile
from unittest.mock import MagicMock

import pandas as pd
import pytest
import requests


def test_cat_default(monkeypatch, magics_instance, capsys):
    """Test the cat command with the default 100 lines."""
    fake_content = "\n".join([f"line {i}" for i in range(150)])
    fake_response = MagicMock()
    fake_response.content = fake_content.encode("utf-8")
    fake_response.status_code = 200
    monkeypatch.setattr(
        requests, "get", lambda url, auth, verify, allow_redirects=True: fake_response
    )
    magics_instance.hdfs("cat /fake-file")
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert len(lines) == 100


def test_cat_full(monkeypatch, magics_instance, capsys):
    """Test the cat command with '-n -1' to display the full file."""
    fake_content = "\n".join([f"line {i}" for i in range(50)])
    fake_response = MagicMock()
    fake_response.content = fake_content.encode("utf-8")
    fake_response.status_code = 200
    monkeypatch.setattr(
        requests, "get", lambda url, auth, verify, allow_redirects=True: fake_response
    )
    magics_instance.hdfs("cat /fake-file -n -1")
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert len(lines) == 50


def test_cat_with_n_option_after_path(monkeypatch, magics_instance, capsys):
    """Test the cat command with syntax: %hdfs cat /path/to/file -n 5."""
    fake_content = "\n".join([f"line {i}" for i in range(20)])
    fake_response = MagicMock()
    fake_response.content = fake_content.encode("utf-8")
    fake_response.status_code = 200
    monkeypatch.setattr(
        requests, "get", lambda url, auth, verify, allow_redirects=True: fake_response
    )
    magics_instance.hdfs("cat /demo/data/customers.csv -n 5 --raw")
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert len(lines) == 5


def test_cat_with_n_option_before_path(monkeypatch, magics_instance, capsys):
    """Test the cat command with syntax: %hdfs cat -n 5 /path/to/file."""
    fake_content = "\n".join([f"line {i}" for i in range(20)])
    fake_response = MagicMock()
    fake_response.content = fake_content.encode("utf-8")
    fake_response.status_code = 200
    monkeypatch.setattr(
        requests, "get", lambda url, auth, verify, allow_redirects=True: fake_response
    )
    magics_instance.hdfs("cat -n 5 /demo/data/customers.csv --raw")
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert len(lines) == 5


def test_cat_with_n_option_different_values(monkeypatch, magics_instance, capsys):
    """Test the cat command with different -n values for both syntaxes."""
    fake_content = "\n".join([f"line {i}" for i in range(50)])
    fake_response = MagicMock()
    fake_response.content = fake_content.encode("utf-8")
    fake_response.status_code = 200
    monkeypatch.setattr(
        requests, "get", lambda url, auth, verify, allow_redirects=True: fake_response
    )

    # Test with -n 10 after path
    magics_instance.hdfs("cat /fake-file -n 10")
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert len(lines) == 10

    # Test with -n 15 before path
    magics_instance.hdfs("cat -n 15 /fake-file")
    captured = capsys.readouterr()
    lines = captured.out.splitlines()
    assert len(lines) == 15


def test_cat_error_multiple_paths(magics_instance, capsys):
    """Test that cat returns an error when multiple paths are provided."""
    result = magics_instance.hdfs("cat /path1 /path2 -n 5")
    assert "Error: multiple file paths specified" in result


def test_cat_error_missing_n_value(magics_instance, capsys):
    """Test that cat returns an error when -n is provided without a value."""
    result = magics_instance.hdfs("cat /fake-file -n")
    assert "Error: -n option requires a number argument" in result


def test_cat_error_invalid_n_value(magics_instance, capsys):
    """Test that cat returns an error when -n has an invalid (non-numeric) value."""
    result = magics_instance.hdfs("cat /fake-file -n abc")
    assert "Error: invalid number of lines" in result


def test_get_with_tilde_home_directory(monkeypatch, magics_instance, tmp_path):
    """Test the get command with ~ (tilde) to represent home directory."""
    # Mock the home directory to use tmp_path for testing
    home_dir = str(tmp_path / "home")
    os.makedirs(home_dir, exist_ok=True)
    monkeypatch.setenv("HOME", home_dir)

    fake_content = b"test content from HDFS"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    monkeypatch.setattr(requests, "get", mock_get)

    # Test with ~/file.csv
    result = magics_instance.hdfs("get /hdfs/file.csv ~/downloaded_file.csv")
    assert "downloaded to" in result

    # Verify the file was written to the correct location
    expected_path = os.path.join(home_dir, "downloaded_file.csv")
    assert os.path.exists(expected_path)
    with open(expected_path, "rb") as f:
        assert f.read() == fake_content


def test_get_with_tilde_and_subdirectory(monkeypatch, magics_instance, tmp_path):
    """Test the get command with ~/subdir/file.csv."""
    # Mock the home directory to use tmp_path for testing
    home_dir = str(tmp_path / "home")
    os.makedirs(home_dir, exist_ok=True)
    monkeypatch.setenv("HOME", home_dir)

    fake_content = b"test content from HDFS"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    monkeypatch.setattr(requests, "get", mock_get)

    # Test with ~/subdir/file.csv (directory should be created automatically)
    result = magics_instance.hdfs("get /hdfs/file.csv ~/subdir/downloaded_file.csv")
    assert "downloaded to" in result

    # Verify the file was written to the correct location
    expected_path = os.path.join(home_dir, "subdir", "downloaded_file.csv")
    assert os.path.exists(expected_path)
    with open(expected_path, "rb") as f:
        assert f.read() == fake_content


def test_get_to_current_directory(monkeypatch, magics_instance, tmp_path):
    """Test the get command with . (current directory)."""
    # Change to tmp_path as current directory
    monkeypatch.chdir(tmp_path)

    fake_content = b"test content from HDFS"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    monkeypatch.setattr(requests, "get", mock_get)

    # Test with . (current directory)
    result = magics_instance.hdfs("get /hdfs/myfile.csv .")
    assert "downloaded to" in result

    # Verify the file was written to current directory
    expected_path = tmp_path / "myfile.csv"
    assert expected_path.exists()
    assert expected_path.read_bytes() == fake_content


def test_get_to_home_directory_shorthand(monkeypatch, magics_instance, tmp_path):
    """Test the get command with ~ or ~/ (home directory shorthand)."""
    # Mock the home directory to use tmp_path for testing
    home_dir = str(tmp_path / "home")
    os.makedirs(home_dir, exist_ok=True)
    monkeypatch.setenv("HOME", home_dir)

    fake_content = b"test content from HDFS"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    monkeypatch.setattr(requests, "get", mock_get)

    # Test with ~ (should download to home with original filename)
    result = magics_instance.hdfs("get /hdfs/original.csv ~")
    assert "downloaded to" in result
    expected_path = os.path.join(home_dir, "original.csv")
    assert os.path.exists(expected_path)

    # Test with ~/ (should also download to home with original filename)
    result = magics_instance.hdfs("get /hdfs/another.csv ~/")
    assert "downloaded to" in result
    expected_path = os.path.join(home_dir, "another.csv")
    assert os.path.exists(expected_path)


def test_get_with_absolute_path(monkeypatch, magics_instance, tmp_path):
    """Test the get command with an absolute path."""
    fake_content = b"test content from HDFS"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    monkeypatch.setattr(requests, "get", mock_get)

    # Test with absolute path
    absolute_path = str(tmp_path / "absolute_file.csv")
    result = magics_instance.hdfs(f"get /hdfs/file.csv {absolute_path}")
    assert "downloaded to" in result

    # Verify the file was written
    assert os.path.exists(absolute_path)
    with open(absolute_path, "rb") as f:
        assert f.read() == fake_content


def test_get_wildcard_to_directory_with_dot(monkeypatch, magics_instance, tmp_path):
    """Test the get command with wildcard to a directory ending with /. like /test_webhdfs/."""
    # Create destination directory
    dest_dir = tmp_path / "test_webhdfs"
    dest_dir.mkdir()

    # Mock the ls response to return multiple files
    fake_ls_data = pd.DataFrame([
        {"name": "file1.csv", "type": "FILE", "permission": "644", "owner": "user",
         "group": "group", "size": 100, "modified": "2025-01-01", "replication": 3},
        {"name": "file2.csv", "type": "FILE", "permission": "644", "owner": "user",
         "group": "group", "size": 200, "modified": "2025-01-01", "replication": 3},
    ])

    # Mock file content
    fake_content = b"test content from HDFS"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    # Mock _format_ls to return our fake data
    monkeypatch.setattr(magics_instance, "_format_ls", lambda path: fake_ls_data)
    monkeypatch.setattr(requests, "get", mock_get)

    # Test with wildcard and /. notation
    result = magics_instance.hdfs(f"get /demo/data/* {dest_dir}/.")
    assert "file1.csv downloaded to" in result
    assert "file2.csv downloaded to" in result

    # Verify files were written to the correct location
    assert (dest_dir / "file1.csv").exists()
    assert (dest_dir / "file2.csv").exists()
    assert (dest_dir / "file1.csv").read_bytes() == fake_content
    assert (dest_dir / "file2.csv").read_bytes() == fake_content


def test_get_wildcard_to_nonexistent_directory(monkeypatch, magics_instance, tmp_path):
    """Test the get command with wildcard to a non-existent directory (should create it)."""
    # Don't create the destination directory
    dest_dir = tmp_path / "new_directory"

    # Mock the ls response to return multiple files
    fake_ls_data = pd.DataFrame([
        {"name": "data1.csv", "type": "FILE", "permission": "644", "owner": "user",
         "group": "group", "size": 100, "modified": "2025-01-01", "replication": 3},
        {"name": "data2.csv", "type": "FILE", "permission": "644", "owner": "user",
         "group": "group", "size": 200, "modified": "2025-01-01", "replication": 3},
    ])

    # Mock file content
    fake_content = b"test data"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    # Mock _format_ls to return our fake data
    monkeypatch.setattr(magics_instance, "_format_ls", lambda path: fake_ls_data)
    monkeypatch.setattr(requests, "get", mock_get)

    # Test with wildcard to non-existent directory
    result = magics_instance.hdfs(f"get /hdfs/data/* {dest_dir}/")
    assert "data1.csv downloaded to" in result
    assert "data2.csv downloaded to" in result

    # Verify directory was created and files were written
    assert dest_dir.exists()
    assert dest_dir.is_dir()
    assert (dest_dir / "data1.csv").exists()
    assert (dest_dir / "data2.csv").exists()


def test_get_wildcard_to_current_directory(monkeypatch, magics_instance, tmp_path):
    """Test the get command with wildcard to current directory (.)."""
    # Change to tmp_path as current directory
    monkeypatch.chdir(tmp_path)

    # Mock the ls response to return multiple files
    fake_ls_data = pd.DataFrame([
        {"name": "test1.csv", "type": "FILE", "permission": "644", "owner": "user",
         "group": "group", "size": 100, "modified": "2025-01-01", "replication": 3},
        {"name": "test2.csv", "type": "FILE", "permission": "644", "owner": "user",
         "group": "group", "size": 200, "modified": "2025-01-01", "replication": 3},
    ])

    # Mock file content
    fake_content = b"test content"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    # Mock _format_ls to return our fake data
    monkeypatch.setattr(magics_instance, "_format_ls", lambda path: fake_ls_data)
    monkeypatch.setattr(requests, "get", mock_get)

    # Test with wildcard to current directory
    result = magics_instance.hdfs("get /hdfs/files/* .")
    assert "test1.csv downloaded to" in result
    assert "test2.csv downloaded to" in result

    # Verify files were written to current directory
    assert (tmp_path / "test1.csv").exists()
    assert (tmp_path / "test2.csv").exists()


def test_get_wildcard_to_directory_without_slash(monkeypatch, magics_instance, tmp_path):
    """Test the get command with wildcard to a directory without trailing / like /test_webhdfs."""
    # Create destination directory
    dest_dir = tmp_path / "test_webhdfs"
    dest_dir.mkdir()

    # Mock the ls response to return multiple files
    fake_ls_data = pd.DataFrame([
        {"name": "customers.csv", "type": "FILE", "permission": "644", "owner": "demo",
         "group": "demo", "size": 1024, "modified": "2025-01-01", "replication": 3},
        {"name": "customers2.csv", "type": "FILE", "permission": "644", "owner": "demo",
         "group": "demo", "size": 2048, "modified": "2025-01-01", "replication": 3},
        {"name": "file.csv", "type": "FILE", "permission": "644", "owner": "demo",
         "group": "demo", "size": 512, "modified": "2025-01-01", "replication": 3},
    ])

    # Mock file content
    fake_content = b"demo,data,content"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    # Mock _format_ls to return our fake data
    monkeypatch.setattr(magics_instance, "_format_ls", lambda path: fake_ls_data)
    monkeypatch.setattr(requests, "get", mock_get)

    # Test with wildcard to directory without trailing /
    result = magics_instance.hdfs(f"get /demo/data/* {dest_dir}")
    assert "customers.csv downloaded to" in result
    assert "customers2.csv downloaded to" in result
    assert "file.csv downloaded to" in result

    # Verify files were written to the directory
    assert (dest_dir / "customers.csv").exists()
    assert (dest_dir / "customers2.csv").exists()
    assert (dest_dir / "file.csv").exists()
    assert (dest_dir / "customers.csv").read_bytes() == fake_content


def test_get_single_file_to_current_directory(monkeypatch, magics_instance, tmp_path):
    """Test the get command with a single file to current directory (.)."""
    # Change to tmp_path as current directory
    monkeypatch.chdir(tmp_path)

    # Mock file content
    fake_content = b"single file content"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    monkeypatch.setattr(requests, "get", mock_get)

    # Test with single file to current directory
    result = magics_instance.hdfs("get /demo/data/file.csv .")
    assert "file.csv downloaded to" in result

    # Verify file was written to current directory with original filename
    assert (tmp_path / "file.csv").exists()
    assert (tmp_path / "file.csv").read_bytes() == fake_content


def test_get_wildcard_to_tilde_subdirectory(monkeypatch, magics_instance, tmp_path):
    """Test the get command with wildcard to ~/subdir/ path."""
    # Mock the home directory to use tmp_path for testing
    home_dir = str(tmp_path / "home")
    os.makedirs(home_dir, exist_ok=True)
    monkeypatch.setenv("HOME", home_dir)

    # Mock the ls response to return multiple files
    fake_ls_data = pd.DataFrame([
        {"name": "customers.csv", "type": "FILE", "permission": "644", "owner": "demo",
         "group": "demo", "size": 1024, "modified": "2025-01-01", "replication": 3},
        {"name": "file.csv", "type": "FILE", "permission": "644", "owner": "demo",
         "group": "demo", "size": 512, "modified": "2025-01-01", "replication": 3},
        {"name": "sales_20251205.csv", "type": "FILE", "permission": "644", "owner": "demo",
         "group": "demo", "size": 2048, "modified": "2025-01-01", "replication": 3},
    ])

    # Mock file content
    fake_content = b"demo data content"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    # Mock _format_ls to return our fake data
    monkeypatch.setattr(magics_instance, "_format_ls", lambda path: fake_ls_data)
    monkeypatch.setattr(requests, "get", mock_get)

    # Test with wildcard to ~/test_webhdfs/
    result = magics_instance.hdfs("get /demo/data/* ~/test_webhdfs/")
    assert "customers.csv downloaded to" in result
    assert "file.csv downloaded to" in result
    assert "sales_20251205.csv downloaded to" in result

    # Verify files were written to the correct location
    test_dir = os.path.join(home_dir, "test_webhdfs")
    assert os.path.exists(test_dir)
    assert os.path.exists(os.path.join(test_dir, "customers.csv"))
    assert os.path.exists(os.path.join(test_dir, "file.csv"))
    assert os.path.exists(os.path.join(test_dir, "sales_20251205.csv"))


class TestGetCommandAdvanced:
    """Advanced GetCommand tests for edge cases."""

    @pytest.fixture
    def get_command(self):
        """Create a GetCommand instance."""
        from unittest.mock import MagicMock

        from webhdfsmagic.client import WebHDFSClient
        from webhdfsmagic.commands.file_ops import GetCommand

        client = MagicMock(spec=WebHDFSClient)
        client.knox_url = "https://knox.example.com"
        client.webhdfs_api = "/webhdfs/v1"
        client.auth_user = "testuser"
        client.auth_password = "testpass"
        client.verify_ssl = False
        cmd = GetCommand(client)
        cmd.format_ls_func = None
        return cmd

    def test_download_with_307_redirect(self, get_command):
        """Test file download with 307 redirect."""
        from unittest.mock import Mock, patch

        with patch('requests.get') as mock_get:
            redirect_response = Mock()
            redirect_response.status_code = 307
            redirect_response.headers = {
                'Location': 'http://abc123def456:50075/webhdfs/v1/file.txt?op=OPEN'
            }

            final_response = Mock()
            final_response.status_code = 200
            final_response.iter_content = lambda chunk_size: [b'file content']
            final_response.raise_for_status = Mock()

            mock_get.side_effect = [redirect_response, final_response]

            with tempfile.TemporaryDirectory() as tmpdir:
                local_path = os.path.join(tmpdir, "test.txt")
                get_command._download_file("/file.txt", local_path)

                assert os.path.exists(local_path)
                with open(local_path, 'rb') as f:
                    assert f.read() == b'file content'

    def test_handle_redirect_with_docker_hostname(self, get_command):
        """Test redirect handling with Docker internal hostname."""
        from unittest.mock import Mock, patch

        import requests

        redirect_response = Mock(spec=requests.Response)
        redirect_response.status_code = 307
        redirect_response.headers = {
            'Location': 'http://abc123def456:50075/webhdfs/v1/file.txt?op=OPEN'
        }

        with patch('requests.get') as mock_get:
            final_response = Mock()
            final_response.status_code = 200
            mock_get.return_value = final_response

            get_command._handle_redirect(redirect_response)

            call_args = mock_get.call_args
            assert 'localhost:50075' in call_args[0][0]

    def test_handle_redirect_adds_username(self, get_command):
        """Test redirect handling adds username to query params."""
        from unittest.mock import Mock, patch

        import requests

        redirect_response = Mock(spec=requests.Response)
        redirect_response.headers = {
            'Location': 'http://datanode:50075/webhdfs/v1/file.txt?op=OPEN'
        }

        with patch('requests.get') as mock_get:
            mock_get.return_value = Mock()

            get_command._handle_redirect(redirect_response)

            call_args = mock_get.call_args[0][0]
            assert 'user.name=testuser' in call_args

    def test_download_multiple_with_error(self, get_command):
        """Test multiple file download with error handling."""
        from unittest.mock import patch

        import pandas as pd

        mock_df = pd.DataFrame({'name': ['file1.txt', 'file2.txt']})

        def mock_format_ls(path):
            return mock_df

        with patch.object(get_command, '_download_file') as mock_download:
            mock_download.side_effect = [None, Exception("Network error")]

            with tempfile.TemporaryDirectory() as tmpdir:
                result = get_command._download_multiple(
                    "/data/*.txt", tmpdir, tmpdir, mock_format_ls
                )

                assert "file1.txt downloaded" in result
                assert "Error:" in result
                assert "Network error" in result

    def test_resolve_path_with_slash_dot(self, get_command):
        """Test path resolution with /. at end."""
        result = get_command._resolve_local_path("/test/.", "/test/.", "file.txt")

        assert "file.txt" in result
        assert os.path.normpath(result) == result

    def test_resolve_path_ending_with_dot_no_slash(self, get_command):
        """Test path resolution ending with dot but no slash."""
        result = get_command._resolve_local_path("/test.", "/test.", "file.txt")

        assert "file.txt" in result

    def test_resolve_path_simple(self, get_command):
        """Test simple path resolution."""
        result = get_command._resolve_local_path("/test", "/test", "file.txt")

        assert "file.txt" in result
        assert "/test" in result


class TestPutCommandAdvanced:
    """Advanced PutCommand tests for edge cases."""

    @pytest.fixture
    def put_command(self):
        """Create a PutCommand instance."""
        from unittest.mock import MagicMock

        from webhdfsmagic.client import WebHDFSClient
        from webhdfsmagic.commands.file_ops import PutCommand

        client = MagicMock(spec=WebHDFSClient)
        client.knox_url = "https://knox.example.com"
        client.webhdfs_api = "/webhdfs/v1"
        client.auth_user = "testuser"
        client.auth_password = "testpass"
        client.verify_ssl = False
        return PutCommand(client)

    def test_fix_docker_hostname(self, put_command):
        """Test Docker hostname fixing in redirect URLs."""
        url = "http://1a2b3c4d5e6f:50075/webhdfs/v1/file.txt?op=CREATE&overwrite=true"

        fixed_url = put_command._fix_docker_hostname(url)

        assert "localhost:50075" in fixed_url
        assert "1a2b3c4d5e6f" not in fixed_url

    def test_fix_docker_hostname_adds_username(self, put_command):
        """Test hostname fixing adds username parameter."""
        url = "http://datanode:50075/webhdfs/v1/file.txt?op=CREATE"

        fixed_url = put_command._fix_docker_hostname(url)

        assert "user.name=testuser" in fixed_url

    def test_upload_with_307_redirect(self, put_command):
        """Test file upload with 307 redirect."""
        from unittest.mock import Mock, patch

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp:
            tmp.write("test content")
            tmp_path = tmp.name

        try:
            with patch('requests.put') as mock_put:
                init_response = Mock()
                init_response.status_code = 307
                init_response.headers = {
                    'Location': 'http://abc123def456:50075/webhdfs/v1/test.txt?op=CREATE'
                }

                upload_response = Mock()
                upload_response.status_code = 201

                mock_put.side_effect = [init_response, upload_response]

                result = put_command.execute(tmp_path, "/hdfs/test.txt")

                assert "uploaded to" in result
        finally:
            os.unlink(tmp_path)

    def test_upload_init_failure(self, put_command):
        """Test upload when initialization fails."""
        from unittest.mock import Mock, patch

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp:
            tmp.write("test content")
            tmp_path = tmp.name

        try:
            with patch('requests.put') as mock_put:
                init_response = Mock()
                init_response.status_code = 500
                mock_put.return_value = init_response

                result = put_command.execute(tmp_path, "/hdfs/test.txt")

                assert "Initiation failed" in result
                assert "500" in result
        finally:
            os.unlink(tmp_path)

    def test_upload_failure_after_redirect(self, put_command):
        """Test upload failure after successful redirect."""
        from unittest.mock import Mock, patch

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp:
            tmp.write("test content")
            tmp_path = tmp.name

        try:
            with patch('requests.put') as mock_put:
                init_response = Mock()
                init_response.status_code = 307
                init_response.headers = {'Location': 'http://datanode:50075/file.txt'}

                upload_response = Mock()
                upload_response.status_code = 500

                mock_put.side_effect = [init_response, upload_response]

                result = put_command.execute(tmp_path, "/hdfs/test.txt")

                assert "Upload failed" in result
        finally:
            os.unlink(tmp_path)

    def test_upload_with_exception(self, put_command):
        """Test upload with exception handling."""
        from unittest.mock import patch

        with tempfile.NamedTemporaryFile(mode='w', suffix='.txt', delete=False) as tmp:
            tmp.write("test content")
            tmp_path = tmp.name

        try:
            with patch('requests.put') as mock_put:
                mock_put.side_effect = Exception("Connection timeout")

                result = put_command.execute(tmp_path, "/hdfs/test.txt")

                assert "Error for" in result
                assert "Connection timeout" in result
        finally:
            os.unlink(tmp_path)


class TestBaseCommandMethods:
    """Test BaseCommand methods using CatCommand."""

    @pytest.fixture
    def command(self):
        """Create a CatCommand instance."""
        from unittest.mock import MagicMock

        from webhdfsmagic.client import WebHDFSClient
        from webhdfsmagic.commands.file_ops import CatCommand

        client = MagicMock(spec=WebHDFSClient)
        return CatCommand(client)

    def test_validate_path_empty(self, command):
        """Test path validation with empty path."""
        with pytest.raises(ValueError, match="Path cannot be empty"):
            command.validate_path("")

    def test_validate_path_not_absolute(self, command):
        """Test path validation with relative path."""
        with pytest.raises(ValueError, match="must be absolute"):
            command.validate_path("relative/path")

    def test_validate_path_success(self, command):
        """Test successful path validation."""
        result = command.validate_path("/absolute/path")
        assert result == "/absolute/path"

    def test_handle_error_with_context(self, command):
        """Test error handling with context."""
        error = Exception("Test error")
        result = command.handle_error(error, context="During upload")

        assert "During upload" in result
        assert "Test error" in result

    def test_handle_error_without_context(self, command):
        """Test error handling without context."""
        error = Exception("Test error")
        result = command.handle_error(error)

        assert "Error:" in result
        assert "Test error" in result
