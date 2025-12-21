"""
Test script for webhdfsmagic without a real HDFS server.
Uses mocks to simulate HDFS responses.
"""

import json
from unittest.mock import MagicMock, patch

import pytest
from IPython.core.interactiveshell import InteractiveShell

from webhdfsmagic.magics import WebHDFSMagics


def mock_request(method, url, **kwargs):
    """Mock HTTP requests to HDFS."""
    response = MagicMock()
    response.status_code = 200

    if "LISTSTATUS" in url:
        json_data = {
            "FileStatuses": {
                "FileStatus": [
                    {
                        "pathSuffix": "test_file.txt",
                        "type": "FILE",
                        "length": 2048,
                        "owner": "testuser",
                        "group": "hadoop",
                        "permission": "644",
                        "modificationTime": 1638360000000,
                        "blockSize": 134217728,
                        "replication": 3,
                    },
                    {
                        "pathSuffix": "test_dir",
                        "type": "DIRECTORY",
                        "length": 0,
                        "owner": "testuser",
                        "group": "hadoop",
                        "permission": "755",
                        "modificationTime": 1638360000000,
                        "blockSize": 0,
                        "replication": 0,
                    },
                ]
            }
        }
        response.json.return_value = json_data
        response.content = json.dumps(json_data).encode()
    elif "MKDIRS" in url:
        json_data = {"boolean": True}
        response.json.return_value = json_data
        response.content = json.dumps(json_data).encode()
    elif "OPEN" in url:
        response.content = b"Mock file content\nLine 2\nLine 3"
    else:
        response.json.return_value = {}
        response.content = b"{}"

    return response


@pytest.fixture
def magics():
    """Fixture to create a configured WebHDFSMagics instance."""
    shell = InteractiveShell.instance()
    magics_instance = WebHDFSMagics(shell)

    # Configuration
    magics_instance.knox_url = "http://fake-hdfs:8443/gateway/default"
    magics_instance.webhdfs_api = "/webhdfs/v1"
    magics_instance.auth_user = "testuser"
    magics_instance.auth_password = "testpass"  # pragma: allowlist secret
    magics_instance.verify_ssl = False

    return magics_instance


def test_autoload():
    """Test that the extension can be loaded."""
    print("Test 1: Loading the extension")
    shell = InteractiveShell.instance()
    magics_instance = WebHDFSMagics(shell)

    # Configuration
    magics_instance.knox_url = "http://fake-hdfs:8443/gateway/default"
    magics_instance.webhdfs_api = "/webhdfs/v1"
    magics_instance.auth_user = "testuser"
    magics_instance.auth_password = "testpass"  # pragma: allowlist secret
    magics_instance.verify_ssl = False

    print("✓ Extension loaded successfully\n")
    assert magics_instance is not None


def test_ls(magics):
    """Test the ls command."""
    print("Test 2: Command %hdfs ls")

    # Patch the client's execute method
    def mock_execute(method, operation, path, **params):
        if operation == "LISTSTATUS":
            return {
                "FileStatuses": {
                    "FileStatus": [
                        {
                            "pathSuffix": "test_file.txt",
                            "type": "FILE",
                            "length": 2048,
                            "owner": "testuser",
                            "group": "hadoop",
                            "permission": "644",
                            "modificationTime": 1638360000000,
                            "blockSize": 134217728,
                            "replication": 3,
                        },
                        {
                            "pathSuffix": "test_dir",
                            "type": "DIRECTORY",
                            "length": 0,
                            "owner": "testuser",
                            "group": "hadoop",
                            "permission": "755",
                            "modificationTime": 1638360000000,
                            "blockSize": 0,
                            "replication": 0,
                        },
                    ]
                }
            }
        return {}

    with patch.object(magics.client, "execute", side_effect=mock_execute):
        result = magics.list_cmd.execute("/user/test")
        print(f"✓ Files found: {len(result)}")
        print(result)
        print()
        assert result is not None
        assert len(result) == 2


def test_cat(magics):
    """Test the cat command."""
    print("Test 3: Command %hdfs cat")

    def mock_get(url, **kwargs):
        response = MagicMock()
        response.status_code = 200
        response.content = b"Mock file content\nLine 2\nLine 3"
        return response

    with patch("requests.get", side_effect=mock_get):
        # The cat command prints output, doesn't return it
        magics.hdfs("cat /user/test/file.txt -n 10")
        print("✓ Cat command executed successfully")
        print()


def test_mkdir(magics):
    """Test the mkdir command."""
    print("Test 4: Command %hdfs mkdir")

    def mock_execute(method, operation, path, **params):
        if operation == "MKDIRS":
            return {"boolean": True}
        return {}

    with patch.object(magics, "_execute", side_effect=mock_execute):
        result = magics.hdfs("mkdir /user/test/newdir")
        print(f"✓ Result: {result}")
        print()


def main():
    """Main function."""
    print("=" * 60)
    print("Testing webhdfsmagic without HDFS server")
    print("=" * 60)
    print()

    magics = test_autoload()
    test_ls(magics)
    test_cat(magics)
    test_mkdir(magics)

    print("=" * 60)
    print("✓ All tests passed successfully!")
    print("=" * 60)


if __name__ == "__main__":
    main()
