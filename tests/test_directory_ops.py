"""Tests for directory operations commands (ls, mkdir, rm, du, stat, mv)."""

import json
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
import requests

from webhdfsmagic.client import WebHDFSClient
from webhdfsmagic.commands.directory_ops import (
    DuCommand,
    ListCommand,
    MkdirCommand,
    MvCommand,
    RmCommand,
    StatCommand,
)


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
        verify_ssl=False,
    )


def test_list_command_execute(client):
    """Test ListCommand.execute with files."""
    list_cmd = ListCommand(client)

    with patch.object(client, "execute") as mock_execute:
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
                        "replication": 3,
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

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {"FileStatuses": {"FileStatus": []}}

        result = list_cmd.execute("/empty")

        assert isinstance(result, dict)
        assert result["empty_dir"] is True
        assert result["path"] == "/empty"


# Tests unitaires pour MkdirCommand


def test_mkdir_command_execute(client):
    """Test MkdirCommand.execute."""
    mkdir_cmd = MkdirCommand(client)

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {"boolean": True}

        result = mkdir_cmd.execute("/test/newdir")

        assert "Directory /test/newdir created" in result
        mock_execute.assert_called_once_with("PUT", "MKDIRS", "/test/newdir")


def test_mkdir_command_with_nested_path(client):
    """Test MkdirCommand.execute with nested path."""
    mkdir_cmd = MkdirCommand(client)

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {"boolean": True}

        result = mkdir_cmd.execute("/a/b/c/d")

        assert "Directory /a/b/c/d created" in result


# Tests unitaires pour RmCommand


def test_rm_command_single_file(client):
    """Test RmCommand.execute with single file."""
    rm_cmd = RmCommand(client)

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {"boolean": True}

        result = rm_cmd.execute("/test/file.txt", recursive=False)

        assert "/test/file.txt deleted" in result
        mock_execute.assert_called_once_with(
            "DELETE", "DELETE", "/test/file.txt", recursive="false"
        )


def test_rm_command_recursive(client):
    """Test RmCommand.execute with recursive flag."""
    rm_cmd = RmCommand(client)

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {"boolean": True}

        result = rm_cmd.execute("/test/dir", recursive=True)

        assert "/test/dir deleted" in result
        mock_execute.assert_called_once_with("DELETE", "DELETE", "/test/dir", recursive="true")


def test_rm_command_wildcard_with_matches(client):
    """Test RmCommand.execute with wildcard matching files."""
    rm_cmd = RmCommand(client)

    def mock_format_ls(path):
        return pd.DataFrame(
            [
                {"name": "file1.csv", "type": "FILE"},
                {"name": "file2.csv", "type": "FILE"},
                {"name": "other.txt", "type": "FILE"},
            ]
        )

    with patch.object(client, "execute") as mock_execute:
        mock_execute.return_value = {"boolean": True}

        result = rm_cmd.execute("/data/*.csv", recursive=False, format_ls_func=mock_format_ls)

        assert "file1.csv deleted" in result
        assert "file2.csv deleted" in result
        assert "other.txt" not in result


def test_rm_command_wildcard_no_matches(client):
    """Test RmCommand.execute with wildcard no matches."""
    rm_cmd = RmCommand(client)

    def mock_format_ls(path):
        return pd.DataFrame([{"name": "file1.txt", "type": "FILE"}])

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
        return pd.DataFrame(
            [{"name": "file1.csv", "type": "FILE"}, {"name": "file2.csv", "type": "FILE"}]
        )

    def mock_execute_with_error(method, op, path, **kwargs):
        if "file2" in path:
            raise Exception("Permission denied")
        return {"boolean": True}

    with patch.object(client, "execute", side_effect=mock_execute_with_error):
        result = rm_cmd.execute("/data/*.csv", recursive=False, format_ls_func=mock_format_ls)

        assert "file1.csv deleted" in result
        assert "Error deleting" in result
        assert "file2.csv" in result


# ---------------------------------------------------------------------------
# Tests for DuCommand
# ---------------------------------------------------------------------------

FAKE_LISTSTATUS_TWO_DIRS = {
    "FileStatuses": {
        "FileStatus": [
            {
                "pathSuffix": "alice",
                "type": "DIRECTORY",
                "permission": "755",
                "owner": "alice",
                "group": "hdfs",
                "modificationTime": 1700000000000,
                "length": 0,
                "blockSize": 0,
                "replication": 0,
            },
            {
                "pathSuffix": "bob",
                "type": "DIRECTORY",
                "permission": "755",
                "owner": "bob",
                "group": "hdfs",
                "modificationTime": 1700000000000,
                "length": 0,
                "blockSize": 0,
                "replication": 0,
            },
        ]
    }
}

FAKE_CS_ALICE = {
    "ContentSummary": {
        "length": 1_048_576,
        "spaceConsumed": 3_145_728,
        "fileCount": 10,
        "directoryCount": 2,
        "quota": -1,
        "spaceQuota": -1,
    }
}

FAKE_CS_BOB = {
    "ContentSummary": {
        "length": 524_288,
        "spaceConsumed": 1_572_864,
        "fileCount": 5,
        "directoryCount": 1,
        "quota": -1,
        "spaceQuota": -1,
    }
}


@pytest.fixture
def du_client():
    return WebHDFSClient(
        knox_url="http://test",
        webhdfs_api="/api",
        auth_user="user",
        auth_password="pass",
        verify_ssl=False,
    )


def test_du_command_lists_children(du_client):
    """du without -s iterates over children and returns real sizes."""
    du_cmd = DuCommand(du_client)

    def mock_execute(method, op, path, **kwargs):
        if op == "LISTSTATUS":
            return FAKE_LISTSTATUS_TWO_DIRS
        if op == "GETCONTENTSUMMARY":
            return FAKE_CS_ALICE if "alice" in path else FAKE_CS_BOB
        raise AssertionError(f"Unexpected op: {op}")

    with patch.object(du_client, "execute", side_effect=mock_execute):
        df = du_cmd.execute("/data/users")

    assert isinstance(df, pd.DataFrame)
    assert list(df.columns) == [
        "name",
        "type",
        "size",
        "space_consumed",
        "file_count",
        "dir_count",
        "error",
    ]
    assert len(df) == 2
    alice = df[df["name"] == "alice"].iloc[0]
    assert alice["size"] == 1_048_576
    assert alice["space_consumed"] == 3_145_728
    assert alice["file_count"] == 10
    assert alice["dir_count"] == 2
    bob = df[df["name"] == "bob"].iloc[0]
    assert bob["size"] == 524_288


def test_du_command_summary_mode(du_client):
    """du -s returns a single row for the path itself."""
    du_cmd = DuCommand(du_client)

    with patch.object(du_client, "execute", return_value=FAKE_CS_ALICE) as mock_exec:
        df = du_cmd.execute("/data/users/alice", summary=True)

    mock_exec.assert_called_once_with("GET", "GETCONTENTSUMMARY", "/data/users/alice")
    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert df.iloc[0]["size"] == 1_048_576
    assert df.iloc[0]["name"] == "/data/users/alice"


def test_du_command_human_readable(du_client):
    """du -h formats sizes as strings (e.g. '1.0 MB')."""
    du_cmd = DuCommand(du_client)

    with patch.object(du_client, "execute", return_value=FAKE_CS_ALICE):
        df = du_cmd.execute("/data/users/alice", summary=True, human_readable=True)

    assert df.iloc[0]["size"] == "1.0 MB"
    assert df.iloc[0]["space_consumed"] == "3.0 MB"


def test_du_command_empty_directory(du_client):
    """du on an empty directory returns empty_dir dict."""
    du_cmd = DuCommand(du_client)

    with patch.object(
        du_client,
        "execute",
        return_value={"FileStatuses": {"FileStatus": []}},
    ):
        result = du_cmd.execute("/data/empty")

    assert isinstance(result, dict)
    assert result["empty_dir"] is True
    assert result["path"] == "/data/empty"


def test_du_command_mixed_files_and_dirs(du_client):
    """du handles a mix of FILE and DIRECTORY entries."""
    du_cmd = DuCommand(du_client)

    liststatus = {
        "FileStatuses": {
            "FileStatus": [
                {
                    "pathSuffix": "subdir",
                    "type": "DIRECTORY",
                    "permission": "755",
                    "owner": "u",
                    "group": "g",
                    "modificationTime": 0,
                    "length": 0,
                    "blockSize": 0,
                    "replication": 0,
                },
                {
                    "pathSuffix": "file.csv",
                    "type": "FILE",
                    "permission": "644",
                    "owner": "u",
                    "group": "g",
                    "modificationTime": 0,
                    "length": 0,
                    "blockSize": 134217728,
                    "replication": 3,
                },
            ]
        }
    }
    cs = {
        "ContentSummary": {
            "length": 2048,
            "spaceConsumed": 6144,
            "fileCount": 1,
            "directoryCount": 0,
            "quota": -1,
            "spaceQuota": -1,
        }
    }

    def mock_execute(method, op, path, **kwargs):
        if op == "LISTSTATUS":
            return liststatus
        return cs

    with patch.object(du_client, "execute", side_effect=mock_execute):
        df = du_cmd.execute("/data/mixed")

    assert len(df) == 2
    assert df[df["name"] == "subdir"].iloc[0]["type"] == "DIR"
    assert df[df["name"] == "file.csv"].iloc[0]["type"] == "FILE"


def test_hdfs_du_magic_command(magics_instance):
    """Test %hdfs du via the magic dispatch."""

    def mock_execute(method, op, path, **kwargs):
        if op == "LISTSTATUS":
            return FAKE_LISTSTATUS_TWO_DIRS
        return FAKE_CS_ALICE if "alice" in path else FAKE_CS_BOB

    with patch.object(magics_instance.client, "execute", side_effect=mock_execute):
        result = magics_instance.hdfs("du /data/users")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 2


def test_hdfs_du_magic_summary_flag(magics_instance):
    """Test %hdfs du -s via the magic dispatch."""
    with patch.object(magics_instance.client, "execute", return_value=FAKE_CS_ALICE):
        result = magics_instance.hdfs("du -s /data/users/alice")

    assert isinstance(result, pd.DataFrame)
    assert len(result) == 1
    assert result.iloc[0]["size"] == 1_048_576


def test_hdfs_du_magic_human_readable_flag(magics_instance):
    """Test %hdfs du -h returns formatted size strings."""
    with patch.object(magics_instance.client, "execute", return_value=FAKE_CS_ALICE):
        result = magics_instance.hdfs("du -sh /data/users/alice")

    assert isinstance(result, pd.DataFrame)
    assert result.iloc[0]["size"] == "1.0 MB"


def test_hdfs_du_magic_no_path(magics_instance):
    """Test %hdfs du without path returns usage message."""
    result = magics_instance.hdfs("du")
    assert "Usage" in result


def test_hdfs_du_magic_empty_dir(magics_instance):
    """Test %hdfs du on empty directory returns empty_dir dict."""
    with patch.object(
        magics_instance.client,
        "execute",
        return_value={"FileStatuses": {"FileStatus": []}},
    ):
        result = magics_instance.hdfs("du /data/empty")

    assert isinstance(result, dict)
    assert result["empty_dir"] is True


def _make_http_error(status_code: int):
    """Helper to build a requests.HTTPError with a given status code."""
    response = MagicMock()
    response.status_code = status_code
    error = requests.exceptions.HTTPError(response=response)
    return error


def test_du_command_partial_permission_denied(du_client):
    """When one child returns 403, it appears with error=... and others are normal."""
    du_cmd = DuCommand(du_client)

    def mock_execute(method, op, path, **kwargs):
        if op == "LISTSTATUS":
            return FAKE_LISTSTATUS_TWO_DIRS
        if "alice" in path:
            raise _make_http_error(403)
        return FAKE_CS_BOB

    with patch.object(du_client, "execute", side_effect=mock_execute):
        df = du_cmd.execute("/data/users")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 2

    alice = df[df["name"] == "alice"].iloc[0]
    assert pd.isna(alice["size"])
    assert "permission denied" in alice["error"]
    assert "403" in alice["error"]

    bob = df[df["name"] == "bob"].iloc[0]
    assert bob["size"] == 524_288
    assert pd.isna(bob["error"])


def test_du_command_all_permission_denied(du_client):
    """When all children return 403, DataFrame has all error rows."""
    du_cmd = DuCommand(du_client)

    def mock_execute(method, op, path, **kwargs):
        if op == "LISTSTATUS":
            return FAKE_LISTSTATUS_TWO_DIRS
        raise _make_http_error(403)

    with patch.object(du_client, "execute", side_effect=mock_execute):
        df = du_cmd.execute("/data/users")

    assert len(df) == 2
    assert df["error"].notna().all()
    assert df["size"].isna().all()


def test_du_command_unauthorized_401(du_client):
    """HTTP 401 is also caught and reported gracefully."""
    du_cmd = DuCommand(du_client)

    def mock_execute(method, op, path, **kwargs):
        if op == "LISTSTATUS":
            return FAKE_LISTSTATUS_TWO_DIRS
        raise _make_http_error(401)

    with patch.object(du_client, "execute", side_effect=mock_execute):
        df = du_cmd.execute("/data/users")

    assert df["error"].str.contains("401").all()


def test_du_command_non_permission_http_error(du_client):
    """Non-403/401 HTTP errors (e.g. 500) are also caught and reported."""
    du_cmd = DuCommand(du_client)

    def mock_execute(method, op, path, **kwargs):
        if op == "LISTSTATUS":
            return FAKE_LISTSTATUS_TWO_DIRS
        raise _make_http_error(500)

    with patch.object(du_client, "execute", side_effect=mock_execute):
        df = du_cmd.execute("/data/users")

    assert df["error"].str.contains("500").all()
    assert df["size"].isna().all()


def test_du_command_accessible_column_present_on_success(du_client):
    """Successful rows always have error=None (column is always present)."""
    du_cmd = DuCommand(du_client)

    def mock_execute(method, op, path, **kwargs):
        if op == "LISTSTATUS":
            return FAKE_LISTSTATUS_TWO_DIRS
        return FAKE_CS_BOB

    with patch.object(du_client, "execute", side_effect=mock_execute):
        df = du_cmd.execute("/data/users")

    assert "error" in df.columns
    assert df["error"].isna().all()


# ---------------------------------------------------------------------------
# StatCommand tests
# ---------------------------------------------------------------------------

FAKE_FILE_STATUS_FILE = {
    "FileStatus": {
        "pathSuffix": "",
        "type": "FILE",
        "length": 10240,
        "owner": "hadoop",
        "group": "supergroup",
        "permission": "644",
        "modificationTime": 1700000000000,
        "blockSize": 134217728,
        "replication": 3,
    }
}

FAKE_FILE_STATUS_DIR = {
    "FileStatus": {
        "pathSuffix": "",
        "type": "DIRECTORY",
        "length": 0,
        "owner": "hadoop",
        "group": "supergroup",
        "permission": "755",
        "modificationTime": 1700000000000,
        "blockSize": 0,
        "replication": 0,
    }
}


@pytest.fixture
def stat_client():
    return MagicMock(spec=WebHDFSClient)


def test_stat_command_file(stat_client):
    """stat on a file returns a single-row DataFrame with correct metadata."""
    stat_client.execute.return_value = FAKE_FILE_STATUS_FILE
    cmd = StatCommand(stat_client)
    df = cmd.execute("/data/events.parquet")

    assert isinstance(df, pd.DataFrame)
    assert len(df) == 1
    assert list(df.columns) == [
        "name",
        "type",
        "size",
        "owner",
        "group",
        "permissions",
        "block_size",
        "modified",
        "replication",
    ]
    row = df.iloc[0]
    assert row["name"] == "events.parquet"
    assert row["type"] == "FILE"
    assert row["size"] == 10240
    assert row["owner"] == "hadoop"
    assert row["permissions"] == "rw-r--r--"
    assert row["replication"] == 3


def test_stat_command_directory(stat_client):
    """stat on a directory returns type=DIR and uses path basename as name."""
    stat_client.execute.return_value = FAKE_FILE_STATUS_DIR
    cmd = StatCommand(stat_client)
    df = cmd.execute("/data/users")

    row = df.iloc[0]
    assert row["name"] == "users"
    assert row["type"] == "DIR"
    assert row["permissions"] == "rwxr-xr-x"


def test_stat_command_trailing_slash(stat_client):
    """Trailing slash on path is stripped for the name."""
    stat_client.execute.return_value = FAKE_FILE_STATUS_DIR
    cmd = StatCommand(stat_client)
    df = cmd.execute("/data/users/")

    assert df.iloc[0]["name"] == "users"


def test_stat_command_root(stat_client):
    """stat on root path uses full path as name when basename is empty."""
    status = dict(FAKE_FILE_STATUS_DIR)
    status["FileStatus"] = dict(FAKE_FILE_STATUS_DIR["FileStatus"])
    stat_client.execute.return_value = status
    cmd = StatCommand(stat_client)
    df = cmd.execute("/")

    assert df.iloc[0]["name"] == "/"


def test_stat_command_calls_getfilestatus(stat_client):
    """StatCommand always calls GETFILESTATUS (not LISTSTATUS)."""
    stat_client.execute.return_value = FAKE_FILE_STATUS_FILE
    cmd = StatCommand(stat_client)
    cmd.execute("/data/events.parquet")

    stat_client.execute.assert_called_once_with("GET", "GETFILESTATUS", "/data/events.parquet")


def test_hdfs_stat_magic_command(monkeypatch, magics_instance):
    """%%hdfs stat dispatches correctly and returns a DataFrame."""
    fake_response = MagicMock()
    fake_data = FAKE_FILE_STATUS_FILE
    fake_response.content = json.dumps(fake_data).encode("utf-8")
    fake_response.status_code = 200
    fake_response.json.return_value = fake_data
    fake_response.raise_for_status = MagicMock()

    monkeypatch.setattr(requests, "request", lambda *a, **kw: fake_response)
    result = magics_instance.hdfs("stat /data/events.parquet")

    assert isinstance(result, pd.DataFrame)
    assert result.iloc[0]["name"] == "events.parquet"


def test_hdfs_stat_magic_no_path(magics_instance):
    """%%hdfs stat with no path returns usage message."""
    result = magics_instance.hdfs("stat")
    assert "Usage" in result


def test_hdfs_stat_magic_not_found(monkeypatch, magics_instance):
    """%%hdfs stat on a non-existent path returns a 404 message."""
    fake_response = MagicMock()
    fake_response.status_code = 404
    http_error = requests.exceptions.HTTPError(response=fake_response)
    fake_response.raise_for_status.side_effect = http_error

    monkeypatch.setattr(
        requests,
        "request",
        lambda *a, **kw: (_ for _ in ()).throw(http_error),
    )

    with patch.object(magics_instance.stat_cmd, "execute", side_effect=http_error):
        result = magics_instance.hdfs("stat /data/missing.csv")

    assert "not found" in result.lower() or "404" in result


# ---------------------------------------------------------------------------
# MvCommand tests
# ---------------------------------------------------------------------------


@pytest.fixture
def mv_client():
    return MagicMock(spec=WebHDFSClient)


def test_mv_command_success(mv_client):
    """mv returns success message when RENAME returns boolean=True."""
    mv_client.execute.return_value = {"boolean": True}
    cmd = MvCommand(mv_client)
    result = cmd.execute("/data/old.csv", "/data/new.csv")

    assert result == "/data/old.csv moved to /data/new.csv"


def test_mv_command_failure(mv_client):
    """mv returns error message when RENAME returns boolean=False."""
    mv_client.execute.return_value = {"boolean": False}
    cmd = MvCommand(mv_client)
    result = cmd.execute("/data/old.csv", "/data/existing.csv")

    assert "Error" in result
    assert "/data/old.csv" in result


def test_mv_command_calls_rename(mv_client):
    """MvCommand calls RENAME with correct src and destination param."""
    mv_client.execute.return_value = {"boolean": True}
    cmd = MvCommand(mv_client)
    cmd.execute("/data/old.csv", "/data/new.csv")

    mv_client.execute.assert_called_once_with(
        "PUT", "RENAME", "/data/old.csv", destination="/data/new.csv"
    )


def test_mv_command_directory(mv_client):
    """mv works on directories too."""
    mv_client.execute.return_value = {"boolean": True}
    cmd = MvCommand(mv_client)
    result = cmd.execute("/data/tmp", "/data/archive/tmp")

    assert result == "/data/tmp moved to /data/archive/tmp"


def test_hdfs_mv_magic_command(monkeypatch, magics_instance):
    """%hdfs mv dispatches correctly and returns success message."""
    fake_response = MagicMock()
    fake_data = {"boolean": True}
    fake_response.content = json.dumps(fake_data).encode("utf-8")
    fake_response.status_code = 200
    fake_response.json.return_value = fake_data
    fake_response.raise_for_status = MagicMock()

    monkeypatch.setattr(requests, "request", lambda *a, **kw: fake_response)
    result = magics_instance.hdfs("mv /data/old.csv /data/new.csv")

    assert "moved" in result
    assert "/data/old.csv" in result


def test_hdfs_mv_magic_no_args(magics_instance):
    """%hdfs mv with no args returns usage message."""
    result = magics_instance.hdfs("mv")
    assert "Usage" in result


def test_hdfs_mv_magic_one_arg(magics_instance):
    """%hdfs mv with only one arg returns usage message."""
    result = magics_instance.hdfs("mv /data/old.csv")
    assert "Usage" in result


def test_mv_command_missing_boolean_key(mv_client):
    """If RENAME response has no 'boolean' key, mv returns an error message."""
    mv_client.execute.return_value = {}
    cmd = MvCommand(mv_client)
    result = cmd.execute("/data/old.csv", "/data/new.csv")

    assert "Error" in result


def test_hdfs_mv_magic_not_found(magics_instance):
    """%hdfs mv on a non-existent source returns a friendly 404 message."""
    fake_response = MagicMock()
    fake_response.status_code = 404
    http_error = requests.exceptions.HTTPError(response=fake_response)

    with patch.object(magics_instance.mv_cmd, "execute", side_effect=http_error):
        result = magics_instance.hdfs("mv /data/missing.csv /data/new.csv")

    assert "not found" in result.lower() or "404" in result


def test_hdfs_stat_magic_permission_denied(magics_instance):
    """%hdfs stat on a 403 path surfaces the error (via general handler)."""
    fake_response = MagicMock()
    fake_response.status_code = 403
    http_error = requests.exceptions.HTTPError(response=fake_response)

    with patch.object(magics_instance.stat_cmd, "execute", side_effect=http_error):
        result = magics_instance.hdfs("stat /data/private")

    # 403 is not handled by _handle_http_error (only 404 is) so it re-raises
    # and the general except returns an Error traceback string
    assert result is not None and ("Error" in result or "403" in result)
