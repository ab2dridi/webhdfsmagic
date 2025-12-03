"""
Integration test: Load configuration from JSON files and verify SSL handling.
"""

import json
import os
import tempfile
from pathlib import Path

from IPython.core.interactiveshell import InteractiveShell

from webhdfsmagic.magics import WebHDFSMagics


def test_load_config_no_ssl():
    """Test loading config with verify_ssl: false."""
    print("\n" + "=" * 60)
    print("Integration Test 1: Load config with verify_ssl = false")
    print("=" * 60)

    config_dir = Path.home() / ".webhdfsmagic"
    config_file = config_dir / "config.json"
    backup_file = config_dir / "config.json.backup"

    # Backup existing config if present
    if config_file.exists():
        config_file.rename(backup_file)

    try:
        # Create test config
        config_dir.mkdir(exist_ok=True)
        test_config = {
            "knox_url": "https://test-knox:8443/gateway/default",
            "webhdfs_api": "/webhdfs/v1",
            "username": "testuser",
            "password": "testpass",
            "verify_ssl": False,
        }

        with open(config_file, "w") as f:
            json.dump(test_config, f, indent=2)

        # Load extension
        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Verify configuration
        assert magics.knox_url == "https://test-knox:8443/gateway/default"
        assert magics.verify_ssl is False
        print(f"✅ Config loaded: knox_url={magics.knox_url}")
        print(f"✅ SSL verification: {magics.verify_ssl}")

    finally:
        # Cleanup
        if config_file.exists():
            config_file.unlink()
        if backup_file.exists():
            backup_file.rename(config_file)


def test_load_config_with_ssl():
    """Test loading config with verify_ssl: true."""
    print("\n" + "=" * 60)
    print("Integration Test 2: Load config with verify_ssl = true")
    print("=" * 60)

    config_dir = Path.home() / ".webhdfsmagic"
    config_file = config_dir / "config.json"
    backup_file = config_dir / "config.json.backup"

    # Backup existing config if present
    if config_file.exists():
        config_file.rename(backup_file)

    try:
        # Create test config
        config_dir.mkdir(exist_ok=True)
        test_config = {
            "knox_url": "https://prod-knox:8443/gateway/default",
            "webhdfs_api": "/webhdfs/v1",
            "username": "produser",
            "password": "prodpass",
            "verify_ssl": True,
        }

        with open(config_file, "w") as f:
            json.dump(test_config, f, indent=2)

        # Load extension
        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Verify configuration
        assert magics.knox_url == "https://prod-knox:8443/gateway/default"
        assert magics.verify_ssl is True
        print(f"✅ Config loaded: knox_url={magics.knox_url}")
        print(f"✅ SSL verification: {magics.verify_ssl}")

    finally:
        # Cleanup
        if config_file.exists():
            config_file.unlink()
        if backup_file.exists():
            backup_file.rename(config_file)


def test_load_config_with_cert():
    """Test loading config with verify_ssl pointing to certificate file."""
    print("\n" + "=" * 60)
    print("Integration Test 3: Load config with certificate path")
    print("=" * 60)

    config_dir = Path.home() / ".webhdfsmagic"
    config_file = config_dir / "config.json"
    backup_file = config_dir / "config.json.backup"

    # Create temporary certificate
    with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as f:
        f.write("-----BEGIN CERTIFICATE-----\n")
        f.write("Test certificate content\n")
        f.write("-----END CERTIFICATE-----\n")
        cert_path = f.name

    # Backup existing config if present
    if config_file.exists():
        config_file.rename(backup_file)

    try:
        # Create test config
        config_dir.mkdir(exist_ok=True)
        test_config = {
            "knox_url": "https://secure-knox:8443/gateway/default",
            "webhdfs_api": "/webhdfs/v1",
            "username": "secureuser",
            "password": "securepass",
            "verify_ssl": cert_path,
        }

        with open(config_file, "w") as f:
            json.dump(test_config, f, indent=2)

        # Load extension
        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Verify configuration
        assert magics.knox_url == "https://secure-knox:8443/gateway/default"
        assert magics.verify_ssl == cert_path
        print(f"✅ Config loaded: knox_url={magics.knox_url}")
        print(f"✅ SSL certificate: {magics.verify_ssl}")

    finally:
        # Cleanup
        if os.path.exists(cert_path):
            os.unlink(cert_path)
        if config_file.exists():
            config_file.unlink()
        if backup_file.exists():
            backup_file.rename(config_file)


def main():
    """Run all integration tests."""
    print("\n" + "=" * 60)
    print("Integration Tests: Configuration Loading")
    print("=" * 60)

    test_load_config_no_ssl()
    test_load_config_with_ssl()
    test_load_config_with_cert()

    print("\n" + "=" * 60)
    print("✅ All integration tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
