import json
from unittest.mock import MagicMock

import pytest
import requests
from IPython.core.interactiveshell import InteractiveShell

from webhdfsmagic.magics import WebHDFSMagics


@pytest.fixture
def magics_instance():
    """
    Fixture to create a WebHDFSMagics instance with test configuration.
    """
    shell = InteractiveShell.instance()
    magics = WebHDFSMagics(shell)
    magics.knox_url = "http://fake-knox"
    magics.webhdfs_api = "/fake-webhdfs"
    magics.auth_user = "user"
    magics.auth_password = "pass"
    magics.verify_ssl = False
    return magics


def test_ls(monkeypatch, magics_instance):
    """
    Test the ls command by mocking the LISTSTATUS response.
    """
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
    fake_response.json.return_value = fake_data  # <-- Set json() return value
    monkeypatch.setattr(
        requests, "request", lambda method, url, params, auth, verify: fake_response
    )
    df = magics_instance._format_ls("/fake-dir")
    assert len(df) == 1


def test_cat_default(monkeypatch, magics_instance):
    """
    Test the cat command with the default 100 lines.
    """
    fake_content = "\n".join([f"line {i}" for i in range(150)])
    fake_response = MagicMock()
    fake_response.content = fake_content.encode("utf-8")
    fake_response.status_code = 200
    monkeypatch.setattr(requests, "get", lambda url, auth, verify: fake_response)
    result = magics_instance.hdfs("cat /fake-file")
    lines = result.splitlines()
    assert len(lines) == 100


def test_cat_full(monkeypatch, magics_instance):
    """
    Test the cat command with '-n -1' to display the full file.
    """
    fake_content = "\n".join([f"line {i}" for i in range(50)])
    fake_response = MagicMock()
    fake_response.content = fake_content.encode("utf-8")
    fake_response.status_code = 200
    monkeypatch.setattr(requests, "get", lambda url, auth, verify: fake_response)
    result = magics_instance.hdfs("cat /fake-file -n -1")
    lines = result.splitlines()
    assert len(lines) == 50
