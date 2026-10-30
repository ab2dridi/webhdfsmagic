"""Tests for WebHDFS URL normalization in direct HTTP calls (cat, get, put)."""

from unittest.mock import MagicMock, Mock, patch

import pytest

from webhdfsmagic.client import WebHDFSClient, build_webhdfs_url
from webhdfsmagic.commands.file_ops import CatCommand, GetCommand, PutCommand

BASE = "https://knox.example.com/gateway/default/webhdfs/v1"


@pytest.mark.parametrize(
    "knox_url,webhdfs_api,path,expected",
    [
        ("https://knox.example.com/gateway/default", "/webhdfs/v1", "/", f"{BASE}/"),
        ("https://knox.example.com/gateway/default/", "/webhdfs/v1/", "/", f"{BASE}/"),
        ("https://knox.example.com/gateway/default/", "webhdfs/v1", "/a/b.csv", f"{BASE}/a/b.csv"),
        ("https://knox.example.com/gateway/default", "/webhdfs/v1/", "/data/", f"{BASE}/data/"),
    ],
)
def test_build_webhdfs_url(knox_url, webhdfs_api, path, expected):
    """Test that separators are normalized at component boundaries."""
    assert build_webhdfs_url(knox_url, webhdfs_api, path) == expected


@pytest.fixture
def client():
    """Mock client configured with trailing slashes."""
    client = MagicMock(spec=WebHDFSClient)
    client.knox_url = "https://knox.example.com/gateway/default/"
    client.webhdfs_api = "/webhdfs/v1/"
    client.auth_user = "testuser"
    client.auth_password = "testpass"
    client.verify_ssl = False
    return client


def test_cat_file_size_url_normalized(client):
    """Test GETFILESTATUS URL used by cat."""
    with patch("requests.get") as mock_get:
        mock_get.return_value.json.return_value = {"FileStatus": {"length": 3}}
        CatCommand(client)._get_file_size("/file.txt")
        assert mock_get.call_args.args[0].startswith(f"{BASE}/file.txt?op=GETFILESTATUS")


def test_cat_fetch_content_url_normalized(client):
    """Test OPEN URL used by cat."""
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.content = b"abc"
        CatCommand(client)._fetch_content("/file.txt", allow_redirects=True)
        assert mock_get.call_args.args[0] == f"{BASE}/file.txt?op=OPEN"


def test_get_download_url_normalized(client, tmp_path):
    """Test OPEN URL used by get."""
    with patch("requests.get") as mock_get:
        mock_get.return_value.status_code = 200
        mock_get.return_value.iter_content = lambda chunk_size: [b"abc"]
        GetCommand(client)._download_file("/file.txt", str(tmp_path / "file.txt"))
        assert mock_get.call_args.args[0] == f"{BASE}/file.txt?op=OPEN"


@pytest.mark.parametrize(
    "hdfs_dest,expected",
    [
        ("/data/file.txt", f"{BASE}/data/file.txt"),
        ("/", f"{BASE}/local.txt"),
        ("/data/", f"{BASE}/data/local.txt"),
    ],
)
def test_put_create_url_normalized(client, tmp_path, hdfs_dest, expected):
    """Test CREATE URL used by put, including uploads to the HDFS root."""
    local_file = tmp_path / "local.txt"
    local_file.write_text("abc")
    with patch("requests.put") as mock_put:
        mock_put.return_value = Mock(status_code=201)
        PutCommand(client)._upload_single_file(str(local_file), hdfs_dest)
        assert mock_put.call_args_list[0].args[0] == expected


@pytest.mark.parametrize(
    "livy_url,expected",
    [
        (
            "https://knox:8443/gateway/default/livy/v1",
            "https://knox:8443/gateway/default/webhdfs/v1/",
        ),
        (
            "https://knox:8443/gateway/default/livy_for_spark3",
            "https://knox:8443/gateway/default/webhdfs/v1/",
        ),
        (
            "https://knox:8443/gateway/default/livy/",
            "https://knox:8443/gateway/default/webhdfs/v1/",
        ),
    ],
)
def test_sparkmagic_config_builds_root_url(tmp_path, monkeypatch, livy_url, expected):
    """Test that a sparkmagic Livy URL leads to a valid WebHDFS root URL."""
    import json

    from webhdfsmagic.config import ConfigurationManager

    monkeypatch.setenv("HOME", str(tmp_path))
    (tmp_path / ".sparkmagic").mkdir()
    (tmp_path / ".sparkmagic" / "config.json").write_text(
        json.dumps({"kernel_python_credentials": {"url": livy_url}})
    )

    config = ConfigurationManager().load()

    assert build_webhdfs_url(config["knox_url"], config["webhdfs_api"], "/") == expected
