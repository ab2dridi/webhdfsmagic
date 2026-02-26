"""
Shared test fixtures for webhdfsmagic tests.
"""

import json
from unittest.mock import MagicMock

import pytest

from webhdfsmagic.magics import WebHDFSMagics


@pytest.fixture
def magics_instance():
    """Create a WebHDFSMagics instance for testing."""
    from IPython.core.interactiveshell import InteractiveShell

    shell = InteractiveShell.instance()
    magics = WebHDFSMagics(shell=shell)
    magics.knox_url = "http://fake-knox"
    magics.webhdfs_api = "/fake-webhdfs"
    magics.auth_user = "user"
    magics.auth_password = "pass"
    magics.verify_ssl = False
    return magics


@pytest.fixture
def mock_requests_get():
    """Create a mock for requests.get with flexible argument handling."""

    def create_mock_response(data, status_code=200):
        fake_response = MagicMock()
        fake_response.content = json.dumps(data).encode("utf-8")
        fake_response.status_code = status_code
        fake_response.json.return_value = data
        fake_response.raise_for_status = MagicMock()
        return fake_response

    return create_mock_response


@pytest.fixture
def mock_requests_request():
    """Create a flexible mock for requests.request that accepts all kwargs."""

    def create_mock(data, status_code=200):
        fake_response = MagicMock()
        fake_response.content = json.dumps(data).encode("utf-8")
        fake_response.status_code = status_code
        fake_response.json.return_value = data
        fake_response.raise_for_status = MagicMock()

        def flexible_request(*args, **kwargs):
            # Accept any arguments
            return fake_response

        return flexible_request

    return create_mock
