"""Tests pour le support multi-threading des commandes get/put."""

from unittest.mock import MagicMock

import pandas as pd
import requests


def test_get_wildcard_parallel(monkeypatch, magics_instance, tmp_path):
    """Test get avec wildcard et -t 2 (multi-threading)."""
    monkeypatch.chdir(tmp_path)
    fake_ls_data = pd.DataFrame(
        [
            {
                "name": "file1.csv",
                "type": "FILE",
                "permission": "644",
                "owner": "user",
                "group": "group",
                "size": 100,
                "modified": "2025-01-01",
                "replication": 3,
            },
            {
                "name": "file2.csv",
                "type": "FILE",
                "permission": "644",
                "owner": "user",
                "group": "group",
                "size": 200,
                "modified": "2025-01-01",
                "replication": 3,
            },
        ]
    )
    fake_content = b"test content"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    monkeypatch.setattr(magics_instance, "_format_ls", lambda path: fake_ls_data)
    monkeypatch.setattr(requests, "get", mock_get)
    import io
    import sys

    captured = io.StringIO()
    sys_stdout = sys.stdout
    sys.stdout = captured
    try:
        result = magics_instance.hdfs("get -t 2 /hdfs/*.csv .")
    finally:
        sys.stdout = sys_stdout
    output = captured.getvalue()
    assert "downloaded to" in output
    assert "file1.csv" in output and "file2.csv" in output


def test_put_wildcard_parallel(monkeypatch, magics_instance, tmp_path):
    """Test put avec wildcard et --threads 2 (multi-threading)."""
    # Crée deux fichiers temporaires
    f1 = tmp_path / "file1.csv"
    f2 = tmp_path / "file2.csv"
    f1.write_text("data1")
    f2.write_text("data2")

    def mock_put(url, params=None, auth=None, verify=None, allow_redirects=None, data=None):
        class Resp:
            status_code = 307 if params and params.get("op") == "CREATE" else 201
            headers = {"Location": url} if params and params.get("op") == "CREATE" else {}

        return Resp()

    monkeypatch.setattr(requests, "put", mock_put)
    magics_instance.put_cmd._fix_docker_hostname = lambda url: url
    import io
    import sys

    captured = io.StringIO()
    sys_stdout = sys.stdout
    sys.stdout = captured
    try:
        result = magics_instance.hdfs(f"put --threads 2 {tmp_path}/*.csv /hdfs/input/")
    finally:
        sys.stdout = sys_stdout
    output = captured.getvalue()
    assert "uploaded to" in output
    assert "file1.csv" in output and "file2.csv" in output


def test_get_wildcard_threads_edge(monkeypatch, magics_instance, tmp_path):
    """Test get avec threads=1 et threads=0 (doit forcer à 1)."""
    monkeypatch.chdir(tmp_path)
    fake_ls_data = pd.DataFrame(
        [
            {
                "name": "file1.csv",
                "type": "FILE",
                "permission": "644",
                "owner": "user",
                "group": "group",
                "size": 100,
                "modified": "2025-01-01",
                "replication": 3,
            },
        ]
    )
    fake_content = b"test content"
    fake_response = MagicMock()
    fake_response.content = fake_content
    fake_response.status_code = 200
    fake_response.iter_content = MagicMock(return_value=[fake_content])

    def mock_get(url, auth, verify, stream=False, allow_redirects=True):
        return fake_response

    monkeypatch.setattr(magics_instance, "_format_ls", lambda path: fake_ls_data)
    monkeypatch.setattr(requests, "get", mock_get)
    import io
    import sys

    captured1 = io.StringIO()
    sys_stdout = sys.stdout
    sys.stdout = captured1
    try:
        result1 = magics_instance.hdfs("get -t 1 /hdfs/*.csv .")
    finally:
        sys.stdout = sys_stdout
    output1 = captured1.getvalue()
    captured2 = io.StringIO()
    sys.stdout = captured2
    try:
        result2 = magics_instance.hdfs("get -t 0 /hdfs/*.csv .")
    finally:
        sys.stdout = sys_stdout
    output2 = captured2.getvalue()
    assert "downloaded to" in output1
    assert "downloaded to" in output2


def test_put_wildcard_threads_edge(monkeypatch, magics_instance, tmp_path):
    """Test put avec threads=1 et threads non entier (doit forcer à 1)."""
    f1 = tmp_path / "file1.csv"
    f1.write_text("data1")

    def mock_put(url, params=None, auth=None, verify=None, allow_redirects=None, data=None):
        class Resp:
            status_code = 307 if params and params.get("op") == "CREATE" else 201
            headers = {"Location": url} if params and params.get("op") == "CREATE" else {}

        return Resp()

    monkeypatch.setattr(requests, "put", mock_put)
    magics_instance.put_cmd._fix_docker_hostname = lambda url: url
    import io
    import sys

    captured1 = io.StringIO()
    sys_stdout = sys.stdout
    sys.stdout = captured1
    try:
        result1 = magics_instance.hdfs(f"put -t 1 {tmp_path}/file1.csv /hdfs/input/")
    finally:
        sys.stdout = sys_stdout
    output1 = captured1.getvalue()
    captured2 = io.StringIO()
    sys.stdout = captured2
    try:
        result2 = magics_instance.hdfs(f"put --threads abc {tmp_path}/file1.csv /hdfs/input/")
    finally:
        sys.stdout = sys_stdout
    output2 = captured2.getvalue()
    assert "uploaded to" in output1
    assert "uploaded to" in output2
