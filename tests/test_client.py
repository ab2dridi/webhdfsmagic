"""Tests unitaires pour client.py - WebHDFSClient."""

import json
from unittest.mock import MagicMock, patch

import pytest
import requests

from webhdfsmagic.client import WebHDFSClient


@pytest.fixture
def client():
    """Create a test client."""
    return WebHDFSClient(
        knox_url="http://test-knox:8443/gateway/default",
        webhdfs_api="/webhdfs/v1",
        auth_user="test_user",
        auth_password="test_pass",
        verify_ssl=False
    )


def test_client_init():
    """Test WebHDFSClient initialization."""
    client = WebHDFSClient(
        knox_url="http://knox:8443",
        webhdfs_api="/api/v1",
        auth_user="user",
        auth_password="pass",
        verify_ssl=True
    )

    assert client.knox_url == "http://knox:8443"
    assert client.webhdfs_api == "/api/v1"
    assert client.auth_user == "user"
    assert client.auth_password == "pass"
    assert client.verify_ssl is True


def test_client_init_with_cert_path():
    """Test WebHDFSClient with certificate path."""
    client = WebHDFSClient(
        knox_url="http://knox:8443",
        webhdfs_api="/api/v1",
        auth_user="user",
        auth_password="pass",
        verify_ssl="/path/to/cert.pem"
    )

    assert client.verify_ssl == "/path/to/cert.pem"


@patch('requests.request')
def test_execute_get_request(mock_request, client):
    """Test execute with GET request."""
    mock_response = MagicMock()
    mock_response.content = json.dumps({"result": "success"}).encode()
    mock_response.json.return_value = {"result": "success"}
    mock_request.return_value = mock_response

    result = client.execute("GET", "LISTSTATUS", "/test")

    assert result == {"result": "success"}
    mock_request.assert_called_once()
    call_kwargs = mock_request.call_args.kwargs
    assert call_kwargs["method"] == "GET"
    assert "/test" in call_kwargs["url"]
    assert call_kwargs["params"]["op"] == "LISTSTATUS"


@patch('requests.request')
def test_execute_with_stream(mock_request, client):
    """Test execute with streaming response."""
    mock_response = MagicMock()
    mock_request.return_value = mock_response

    result = client.execute("GET", "OPEN", "/file.txt", stream=True)

    assert result == mock_response
    call_kwargs = mock_request.call_args.kwargs
    assert call_kwargs["stream"] is True


@patch('requests.request')
def test_execute_without_redirects(mock_request, client):
    """Test execute with allow_redirects=False."""
    mock_response = MagicMock()
    mock_response.content = b""
    mock_request.return_value = mock_response

    client.execute("PUT", "CREATE", "/file.txt", allow_redirects=False)

    call_kwargs = mock_request.call_args.kwargs
    assert call_kwargs["allow_redirects"] is False


@patch('requests.request')
def test_execute_adds_username_param(mock_request, client):
    """Test execute adds user.name parameter."""
    mock_response = MagicMock()
    mock_response.content = json.dumps({}).encode()
    mock_response.json.return_value = {}
    mock_request.return_value = mock_response

    client.execute("GET", "LISTSTATUS", "/test")

    call_kwargs = mock_request.call_args.kwargs
    assert call_kwargs["params"]["user.name"] == "test_user"


@patch('requests.request')
def test_execute_empty_response(mock_request, client):
    """Test execute with empty response content."""
    mock_response = MagicMock()
    mock_response.content = b""
    mock_request.return_value = mock_response

    result = client.execute("DELETE", "DELETE", "/file.txt")

    assert result == {}


@patch('requests.request')
def test_execute_raises_http_error(mock_request, client):
    """Test execute raises HTTP errors."""
    mock_response = MagicMock()
    mock_response.raise_for_status.side_effect = requests.HTTPError("404 Not Found")
    mock_request.return_value = mock_response

    with pytest.raises(requests.HTTPError):
        client.execute("GET", "LISTSTATUS", "/nonexistent")


def test_get_method(client):
    """Test get convenience method."""
    with patch.object(client, 'execute') as mock_execute:
        mock_execute.return_value = {"files": []}

        result = client.get("LISTSTATUS", "/test", limit=10)

        mock_execute.assert_called_once_with(
            "GET", "LISTSTATUS", "/test", stream=False, limit=10
        )
        assert result == {"files": []}


def test_get_method_with_stream(client):
    """Test get method with streaming."""
    with patch.object(client, 'execute') as mock_execute:
        mock_response = MagicMock()
        mock_execute.return_value = mock_response

        result = client.get("OPEN", "/file.txt", stream=True)

        mock_execute.assert_called_once_with(
            "GET", "OPEN", "/file.txt", stream=True
        )
        assert result == mock_response


@patch('requests.put')
def test_put_method(mock_put, client):
    """Test put convenience method."""
    mock_response = MagicMock()
    mock_response.content = json.dumps({"success": True}).encode()
    mock_response.json.return_value = {"success": True}
    mock_put.return_value = mock_response

    result = client.put("CREATE", "/file.txt", data=b"test data", overwrite="true")

    assert result == {"success": True}
    mock_put.assert_called_once()
    call_kwargs = mock_put.call_args.kwargs
    assert call_kwargs["params"]["op"] == "CREATE"
    assert call_kwargs["data"] == b"test data"


@patch('requests.put')
def test_put_method_empty_response(mock_put, client):
    """Test put method with empty response."""
    mock_response = MagicMock()
    mock_response.content = b""
    mock_put.return_value = mock_response

    result = client.put("MKDIRS", "/new_dir")

    assert result == {}


@patch('requests.post')
def test_post_method(mock_post, client):
    """Test post convenience method."""
    mock_response = MagicMock()
    mock_response.content = json.dumps({"appended": True}).encode()
    mock_response.json.return_value = {"appended": True}
    mock_post.return_value = mock_response

    result = client.post("APPEND", "/file.txt", data=b"more data")

    assert result == {"appended": True}
    mock_post.assert_called_once()
    call_kwargs = mock_post.call_args.kwargs
    assert call_kwargs["params"]["op"] == "APPEND"
    assert call_kwargs["data"] == b"more data"


def test_delete_method(client):
    """Test delete convenience method."""
    with patch.object(client, 'execute') as mock_execute:
        mock_execute.return_value = {"boolean": True}

        result = client.delete("DELETE", "/file.txt", recursive="true")

        mock_execute.assert_called_once_with(
            "DELETE", "DELETE", "/file.txt", recursive="true"
        )
        assert result == {"boolean": True}


@patch('requests.request')
def test_execute_with_additional_params(mock_request, client):
    """Test execute with additional query parameters."""
    mock_response = MagicMock()
    mock_response.content = json.dumps({}).encode()
    mock_response.json.return_value = {}
    mock_request.return_value = mock_response

    client.execute(
        "PUT",
        "SETPERMISSION",
        "/file.txt",
        permission="755",
        custom_param="value"
    )

    call_kwargs = mock_request.call_args.kwargs
    assert call_kwargs["params"]["permission"] == "755"
    assert call_kwargs["params"]["custom_param"] == "value"
