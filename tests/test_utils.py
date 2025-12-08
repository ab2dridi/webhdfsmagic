"""Tests unitaires pour utils.py - Utility functions."""

from datetime import datetime

from webhdfsmagic.utils import (
    format_file_entry,
    format_full_permissions,
    format_permissions,
    format_size,
    format_timestamp,
    normalize_hdfs_path,
    parse_hdfs_path,
)


def test_format_permissions_read_write_execute():
    """Test format_permissions with read-write-execute (7)."""
    assert format_permissions(7) == "rwx"


def test_format_permissions_no_permissions():
    """Test format_permissions with no permissions (0)."""
    assert format_permissions(0) == "---"


def test_format_permissions_read_only():
    """Test format_permissions with read-only (4)."""
    assert format_permissions(4) == "r--"


def test_format_permissions_read_execute():
    """Test format_permissions with read-execute (5)."""
    assert format_permissions(5) == "r-x"


def test_format_permissions_read_write():
    """Test format_permissions with read-write (6)."""
    assert format_permissions(6) == "rw-"


def test_format_full_permissions_755():
    """Test format_full_permissions with 755."""
    assert format_full_permissions(0o755) == "rwxr-xr-x"


def test_format_full_permissions_644():
    """Test format_full_permissions with 644."""
    assert format_full_permissions(0o644) == "rw-r--r--"


def test_format_full_permissions_777():
    """Test format_full_permissions with 777."""
    assert format_full_permissions(0o777) == "rwxrwxrwx"


def test_format_full_permissions_000():
    """Test format_full_permissions with 000."""
    assert format_full_permissions(0o000) == "---------"


def test_format_file_entry_file():
    """Test format_file_entry with a file."""
    file_status = {
        "pathSuffix": "test.csv",
        "type": "FILE",
        "permission": "644",
        "owner": "hadoop",
        "group": "supergroup",
        "modificationTime": 1609459200000,
        "length": 1024,
        "blockSize": 134217728,
        "replication": 3
    }

    result = format_file_entry(file_status)

    assert result["name"] == "test.csv"
    assert result["type"] == "FILE"
    assert result["owner"] == "hadoop"
    assert result["group"] == "supergroup"
    assert result["size"] == 1024
    assert result["replication"] == 3
    assert "modified" in result


def test_format_file_entry_directory():
    """Test format_file_entry with a directory."""
    dir_status = {
        "pathSuffix": "data",
        "type": "DIRECTORY",
        "permission": "755",
        "owner": "hadoop",
        "group": "supergroup",
        "modificationTime": 1609459200000,
        "length": 0,
        "blockSize": 0,
        "replication": 0
    }

    result = format_file_entry(dir_status)

    assert result["name"] == "data"
    assert result["type"] == "DIR"  # Note: DIRECTORY -> DIR
    assert result["size"] == 0
    assert result["replication"] == 0


def test_format_size_not_human_readable():
    """Test format_size with human_readable=False."""
    assert format_size(1048576, human_readable=False) == "1048576"
    assert format_size(1024, human_readable=False) == "1024"
    assert format_size(0, human_readable=False) == "0"


def test_format_size_human_readable():
    """Test format_size with human_readable=True."""
    # Just verify the format contains number and unit
    result = format_size(1048576, human_readable=True)
    assert "MB" in result or "KB" in result or "B" in result

    result = format_size(2048, human_readable=True)
    assert any(unit in result for unit in ["B", "KB", "MB", "GB"])


def test_format_size_above_petabytes():
    """Test size above petabytes (covers line 84 - return after loop)."""
    # 10 PB - forces the loop to complete and hit the final return statement
    result = format_size(10 * 1024 * 1024 * 1024 * 1024 * 1024, human_readable=True)
    assert result == "10.0 PB"


def test_format_timestamp():
    """Test format_timestamp conversion."""
    # 2021-01-01 00:00:00 UTC (approximate depending on timezone)
    timestamp_ms = 1609459200000
    result = format_timestamp(timestamp_ms)

    assert isinstance(result, datetime)
    assert result.year == 2021
    assert result.month == 1
    assert result.day == 1


def test_format_timestamp_different_dates():
    """Test format_timestamp with different dates."""
    # 2020-06-15
    timestamp_ms = 1592226645000
    result = format_timestamp(timestamp_ms)

    assert result.year == 2020
    assert result.month == 6
    assert result.day == 15


def test_parse_hdfs_path_with_file():
    """Test parse_hdfs_path with a file path."""
    directory, filename, extension = parse_hdfs_path("/user/hadoop/data/file.csv")

    assert directory == "/user/hadoop/data"
    assert filename == "file.csv"
    assert extension == ".csv"


def test_parse_hdfs_path_with_directory():
    """Test parse_hdfs_path with a directory path."""
    directory, filename, extension = parse_hdfs_path("/user/hadoop/data/")

    assert directory == "/user/hadoop/data"
    assert filename == ""
    assert extension == ""


def test_parse_hdfs_path_root():
    """Test parse_hdfs_path with root path."""
    directory, filename, extension = parse_hdfs_path("/")

    # Root path results in empty directory and filename
    assert directory in ["", "/"]
    assert filename == ""
    assert extension == ""


def test_parse_hdfs_path_no_extension():
    """Test parse_hdfs_path with file without extension."""
    directory, filename, extension = parse_hdfs_path("/user/hadoop/README")

    assert directory == "/user/hadoop"
    assert filename == "README"
    assert extension == ""


def test_parse_hdfs_path_multiple_extensions():
    """Test parse_hdfs_path with multiple dots."""
    directory, filename, extension = parse_hdfs_path("/data/backup.tar.gz")

    assert directory == "/data"
    assert filename == "backup.tar.gz"
    assert extension == ".gz"


def test_normalize_hdfs_path_trailing_slash():
    """Test normalize_hdfs_path removes trailing slashes."""
    assert normalize_hdfs_path("/user/hadoop/data/") == "/user/hadoop/data"
    assert normalize_hdfs_path("/user/hadoop/") == "/user/hadoop"


def test_normalize_hdfs_path_root():
    """Test normalize_hdfs_path preserves root slash."""
    assert normalize_hdfs_path("/") == "/"


def test_normalize_hdfs_path_double_slashes():
    """Test normalize_hdfs_path removes double slashes."""
    assert normalize_hdfs_path("/user//hadoop///data") == "/user/hadoop/data"
    assert normalize_hdfs_path("//user/hadoop") == "/user/hadoop"


def test_normalize_hdfs_path_already_normalized():
    """Test normalize_hdfs_path with already normalized path."""
    assert normalize_hdfs_path("/user/hadoop/data") == "/user/hadoop/data"


def test_normalize_hdfs_path_complex():
    """Test normalize_hdfs_path with complex path."""
    assert normalize_hdfs_path("/user//hadoop///data//file.csv/") == "/user/hadoop/data/file.csv"
