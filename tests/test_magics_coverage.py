"""Additional tests to improve magics.py coverage."""

import json
from unittest.mock import MagicMock, patch

import pytest
from IPython.core.interactiveshell import InteractiveShell

from webhdfsmagic.magics import WebHDFSMagics


@pytest.fixture
def magics():
    """Create a WebHDFSMagics instance for testing."""
    shell = InteractiveShell.instance()
    return WebHDFSMagics(shell)


def test_hdfs_empty_command(magics):
    """Test calling %hdfs with no arguments returns help."""
    from IPython.display import HTML

    result = magics.hdfs("")
    assert isinstance(result, HTML)
    assert "Command" in result.data or "help" in result.data.lower()


def test_hdfs_unknown_command(magics):
    """Test calling %hdfs with unknown command."""
    result = magics.hdfs("invalid_command arg1 arg2")
    assert "Unknown command" in result
    assert "invalid_command" in result


def test_setconfig_no_args(magics):
    """Test setconfig without arguments."""
    result = magics.hdfs("setconfig")
    assert "Usage:" in result
    assert "setconfig" in result


def test_setconfig_invalid_json(magics):
    """Test setconfig with invalid JSON."""
    result = magics.hdfs("setconfig {invalid json}")
    assert "Error parsing JSON" in result


def test_setconfig_valid_json(magics):
    """Test setconfig with valid JSON."""
    config = {
        "knox_url": "https://test:8443/gateway/test",
        "webhdfs_api": "/webhdfs/v1",
        "username": "testuser",
        "password": "testpass",
        "verify_ssl": False,
    }
    result = magics.hdfs(f"setconfig {json.dumps(config)}")
    assert "successfully updated" in result.lower()
    assert magics.knox_url == config["knox_url"]
    assert magics.auth_user == config["username"]


def test_cat_no_args(magics):
    """Test cat command without arguments."""
    result = magics.hdfs("cat")
    assert "Usage:" in result
    assert "cat" in result


def test_cat_missing_n_value(magics):
    """Test cat command with -n but no value."""
    result = magics.hdfs("cat /test/file.txt -n")
    assert "Error" in result
    assert "-n option requires" in result


def test_cat_invalid_n_value(magics):
    """Test cat command with invalid -n value."""
    result = magics.hdfs("cat /test/file.txt -n abc")
    assert "Error" in result
    assert "invalid number" in result


def test_cat_multiple_files(magics):
    """Test cat command with multiple file paths."""
    result = magics.hdfs("cat /file1.txt /file2.txt")
    assert "Error" in result
    assert "multiple file paths" in result


def test_cat_n_option_before_path(magics):
    """Test cat command with -n option before file path."""
    with patch.object(magics.cat_cmd, "execute", return_value="file content"):
        result = magics.hdfs("cat -n 10 /test/file.txt")
        assert "file content" in result
        magics.cat_cmd.execute.assert_called_once_with("/test/file.txt", 10)


def test_cat_n_option_after_path(magics):
    """Test cat command with -n option after file path."""
    with patch.object(magics.cat_cmd, "execute", return_value="file content"):
        result = magics.hdfs("cat /test/file.txt -n 20")
        assert "file content" in result
        magics.cat_cmd.execute.assert_called_once_with("/test/file.txt", 20)


def test_cat_no_n_option(magics):
    """Test cat command without -n option uses default 100 lines."""
    with patch.object(magics.cat_cmd, "execute", return_value="file content"):
        result = magics.hdfs("cat /test/file.txt")
        assert "file content" in result
        magics.cat_cmd.execute.assert_called_once_with("/test/file.txt", 100)


def test_chmod_without_recursive(magics):
    """Test chmod command without -R flag."""
    with patch.object(magics.chmod_cmd, "execute", return_value="Permission changed"):
        result = magics.hdfs("chmod 755 /test/path")
        assert "Permission changed" in result
        magics.chmod_cmd.execute.assert_called_once_with(
            "/test/path", "755", False, magics._format_ls
        )


def test_chmod_with_recursive(magics):
    """Test chmod command with -R flag."""
    with patch.object(magics.chmod_cmd, "execute", return_value="Permission changed"):
        result = magics.hdfs("chmod -R 755 /test/path")
        assert "Permission changed" in result
        magics.chmod_cmd.execute.assert_called_once_with(
            "/test/path", "755", True, magics._format_ls
        )


def test_chown_without_recursive_no_group(magics):
    """Test chown command without -R flag and no group."""
    with patch.object(magics.chown_cmd, "execute", return_value="Owner changed"):
        result = magics.hdfs("chown testuser /test/path")
        assert "Owner changed" in result
        magics.chown_cmd.execute.assert_called_once_with(
            "/test/path", "testuser", None, False, magics._format_ls
        )


def test_chown_without_recursive_with_group(magics):
    """Test chown command without -R flag with group."""
    with patch.object(magics.chown_cmd, "execute", return_value="Owner changed"):
        result = magics.hdfs("chown testuser:testgroup /test/path")
        assert "Owner changed" in result
        magics.chown_cmd.execute.assert_called_once_with(
            "/test/path", "testuser", "testgroup", False, magics._format_ls
        )


def test_chown_with_recursive_with_group(magics):
    """Test chown command with -R flag and group."""
    with patch.object(magics.chown_cmd, "execute", return_value="Owner changed"):
        result = magics.hdfs("chown -R testuser:testgroup /test/path")
        assert "Owner changed" in result
        magics.chown_cmd.execute.assert_called_once_with(
            "/test/path", "testuser", "testgroup", True, magics._format_ls
        )


def test_put_missing_destination(magics):
    """Test put command without destination."""
    result = magics.hdfs("put /local/file.txt")
    assert "Usage:" in result
    assert "put" in result


def test_put_success(magics):
    """Test successful put command."""
    with patch.object(magics.put_cmd, "execute", return_value="File uploaded"):
        result = magics.hdfs("put /local/file.txt /hdfs/dest/")
        assert "File uploaded" in result
        magics.put_cmd.execute.assert_called_once_with("/local/file.txt", "/hdfs/dest/")


def test_get_missing_destination(magics):
    """Test get command without destination."""
    result = magics.hdfs("get /hdfs/file.txt")
    assert "Usage:" in result
    assert "get" in result


def test_get_success(magics):
    """Test successful get command."""
    with patch.object(magics.get_cmd, "execute", return_value="File downloaded"):
        result = magics.hdfs("get /hdfs/file.txt /local/dest/")
        assert "File downloaded" in result
        magics.get_cmd.execute.assert_called_once_with(
            "/hdfs/file.txt", "/local/dest/", magics._format_ls
        )


def test_mkdir_success(magics):
    """Test successful mkdir command."""
    with patch.object(magics.mkdir_cmd, "execute", return_value="Directory created"):
        result = magics.hdfs("mkdir /test/newdir")
        assert "Directory created" in result
        magics.mkdir_cmd.execute.assert_called_once_with("/test/newdir")


def test_rm_success(magics):
    """Test successful rm command."""
    with patch.object(magics.rm_cmd, "execute", return_value="File deleted"):
        result = magics.hdfs("rm /test/file.txt")
        assert "File deleted" in result


def test_rm_with_recursive_flag(magics):
    """Test rm command with -r flag."""
    with patch.object(magics.rm_cmd, "execute", return_value="Directory deleted"):
        result = magics.hdfs("rm -r /test/dir")
        assert "Directory deleted" in result
        # Verify recursive flag was passed
        magics.rm_cmd.execute.assert_called_once()
        call_args = magics.rm_cmd.execute.call_args
        assert call_args[0][1] is True  # recursive_flag should be True


def test_rm_with_uppercase_recursive_flag(magics):
    """Test rm command with -R flag (uppercase)."""
    with patch.object(magics.rm_cmd, "execute", return_value="Directory deleted"):
        result = magics.hdfs("rm -R /test/dir")
        assert "Directory deleted" in result
        # Verify recursive flag was passed
        magics.rm_cmd.execute.assert_called_once()
        call_args = magics.rm_cmd.execute.call_args
        assert call_args[0][1] is True  # recursive_flag should be True


def test_ls_empty_directory_dict_response(magics):
    """Test ls command returning empty directory dict."""
    with patch.object(magics.list_cmd, "execute", return_value={"empty_dir": True}):
        result = magics.hdfs("ls /empty/dir")
        assert isinstance(result, dict)
        assert result.get("empty_dir") is True


def test_ls_default_root_path(magics):
    """Test ls command without path defaults to root."""
    import pandas as pd

    mock_df = pd.DataFrame({"name": ["file1.txt"], "type": ["FILE"]})
    with patch.object(magics.list_cmd, "execute", return_value=mock_df):
        result = magics.hdfs("ls")
        assert isinstance(result, pd.DataFrame)
        magics.list_cmd.execute.assert_called_once_with("/")


def test_ls_with_specific_path(magics):
    """Test ls command with specific path."""
    import pandas as pd

    mock_df = pd.DataFrame({"name": ["file1.txt"], "type": ["FILE"]})
    with patch.object(magics.list_cmd, "execute", return_value=mock_df):
        result = magics.hdfs("ls /data/files")
        assert isinstance(result, pd.DataFrame)
        magics.list_cmd.execute.assert_called_once_with("/data/files")


def test_exception_handling_with_traceback(magics):
    """Test that exceptions are caught and return error message with traceback."""
    with patch.object(
        magics.list_cmd, "execute", side_effect=Exception("Test error")
    ):
        result = magics.hdfs("ls /test")
        assert "Error: Test error" in result
        assert "Traceback:" in result


def test_help_command(magics):
    """Test help command explicitly."""
    from IPython.display import HTML

    result = magics.hdfs("help")
    assert isinstance(result, HTML)
    assert "Command" in result.data or "help" in result.data.lower()


def test_format_ls_backward_compatibility(magics):
    """Test _format_ls wrapper for backward compatibility."""
    import pandas as pd

    mock_df = pd.DataFrame({"name": ["file1.txt"], "type": ["FILE"]})
    with patch.object(magics.list_cmd, "execute", return_value=mock_df):
        result = magics._format_ls("/test/path")
        assert isinstance(result, pd.DataFrame)
        magics.list_cmd.execute.assert_called_once_with("/test/path")


def test_execute_backward_compatibility(magics):
    """Test _execute wrapper for backward compatibility."""
    mock_response = {"FileStatuses": {"FileStatus": []}}
    with patch.object(magics.client, "execute", return_value=mock_response):
        result = magics._execute("GET", "LISTSTATUS", "/test", param1="value1")
        assert result == mock_response
        magics.client.execute.assert_called_once_with(
            "GET", "LISTSTATUS", "/test", param1="value1"
        )


def test_set_permission_backward_compatibility(magics):
    """Test _set_permission wrapper for backward compatibility."""
    with patch.object(magics.chmod_cmd, "_set_permission", return_value="Success"):
        result = magics._set_permission("/test/path", "755")
        assert result == "Success"
        magics.chmod_cmd._set_permission.assert_called_once_with("/test/path", "755")


def test_set_owner_backward_compatibility(magics):
    """Test _set_owner wrapper for backward compatibility."""
    with patch.object(magics.chown_cmd, "_set_owner", return_value="Success"):
        result = magics._set_owner("/test/path", "user", "group")
        assert result == "Success"
        magics.chown_cmd._set_owner.assert_called_once_with("/test/path", "user", "group")


def test_set_owner_backward_compatibility_no_group(magics):
    """Test _set_owner wrapper without group."""
    with patch.object(magics.chown_cmd, "_set_owner", return_value="Success"):
        result = magics._set_owner("/test/path", "user")
        assert result == "Success"
        magics.chown_cmd._set_owner.assert_called_once_with("/test/path", "user", None)


def test_load_ipython_extension():
    """Test loading extension into IPython."""
    from webhdfsmagic.magics import load_ipython_extension

    mock_ipython = MagicMock()
    load_ipython_extension(mock_ipython)
    mock_ipython.register_magics.assert_called_once()


def test_logger_integration(magics):
    """Test that logger is properly integrated."""
    assert hasattr(magics, "logger")
    assert magics.logger is not None


def test_logging_on_operation_start(magics):
    """Test that operations are logged at start."""
    with patch.object(magics.logger, "log_operation_start") as mock_log:
        with patch.object(magics.list_cmd, "execute", return_value=[]):
            magics.hdfs("ls /test")
            mock_log.assert_called_once()
            call_args = mock_log.call_args
            assert "hdfs ls" in str(call_args)


def test_logging_on_operation_end_success(magics):
    """Test that successful operations are logged."""
    with patch.object(magics.logger, "log_operation_end") as mock_log:
        with patch.object(magics.list_cmd, "execute", return_value=[]):
            magics.hdfs("ls /test")
            mock_log.assert_called_once()
            call_args = mock_log.call_args
            assert call_args[1]["success"] is True


def test_logging_on_error(magics):
    """Test that errors are logged."""
    with patch.object(magics.logger, "log_error") as mock_log:
        with patch.object(
            magics.list_cmd, "execute", side_effect=Exception("Test error")
        ):
            magics.hdfs("ls /test")
            mock_log.assert_called_once()
            call_args = mock_log.call_args
            assert "hdfs ls" in str(call_args)


def test_cat_no_file_path_after_parsing(magics):
    """Test cat command where file_path remains None after parsing."""
    result = magics.hdfs("cat -n 10")
    assert "Usage:" in result
    assert "cat" in result
