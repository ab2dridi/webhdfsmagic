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


def test_config_priority_webhdfsmagic_over_sparkmagic():
    """Test that .webhdfsmagic/config.json has priority over .sparkmagic/config.json."""
    print("\n" + "=" * 60)
    print("Integration Test 4: Config priority - webhdfsmagic over sparkmagic")
    print("=" * 60)

    webhdfs_config_dir = Path.home() / ".webhdfsmagic"
    webhdfs_config_file = webhdfs_config_dir / "config.json"
    webhdfs_backup_file = webhdfs_config_dir / "config.json.backup"

    sparkmagic_config_dir = Path.home() / ".sparkmagic"
    sparkmagic_config_file = sparkmagic_config_dir / "config.json"
    sparkmagic_backup_file = sparkmagic_config_dir / "config.json.backup"

    # Backup existing configs if present
    if webhdfs_config_file.exists():
        webhdfs_config_file.rename(webhdfs_backup_file)
    if sparkmagic_config_file.exists():
        sparkmagic_config_file.rename(sparkmagic_backup_file)

    try:
        # Create sparkmagic config
        sparkmagic_config_dir.mkdir(exist_ok=True)
        sparkmagic_config = {
            "kernel_python_credentials": {
                "url": "https://sparkmagic-knox:8443/gateway/default/livy/v1"
            },
            "verify_ssl": True,
        }
        with open(sparkmagic_config_file, "w") as f:
            json.dump(sparkmagic_config, f, indent=2)

        # Create webhdfsmagic config (should have priority)
        webhdfs_config_dir.mkdir(exist_ok=True)
        webhdfs_config = {
            "knox_url": "https://webhdfs-knox:9443/gateway/production",
            "webhdfs_api": "/webhdfs/v1",
            "username": "webhdfsuser",
            "password": "webhdfspass",
            "verify_ssl": False,
        }
        with open(webhdfs_config_file, "w") as f:
            json.dump(webhdfs_config, f, indent=2)

        # Load extension
        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Verify that webhdfsmagic config was loaded (not sparkmagic)
        assert magics.knox_url == "https://webhdfs-knox:9443/gateway/production"
        assert magics.auth_user == "webhdfsuser"
        assert magics.verify_ssl is False
        print(f"✅ webhdfsmagic config took priority: knox_url={magics.knox_url}")
        print(f"✅ User from webhdfsmagic: {magics.auth_user}")
        print(f"✅ SSL from webhdfsmagic: {magics.verify_ssl}")

    finally:
        # Cleanup
        if webhdfs_config_file.exists():
            webhdfs_config_file.unlink()
        if sparkmagic_config_file.exists():
            sparkmagic_config_file.unlink()
        if webhdfs_backup_file.exists():
            webhdfs_backup_file.rename(webhdfs_config_file)
        if sparkmagic_backup_file.exists():
            sparkmagic_backup_file.rename(sparkmagic_config_file)


def test_fallback_to_sparkmagic_config():
    """Test that .sparkmagic/config.json is used when .webhdfsmagic/config.json doesn't exist."""
    print("\n" + "=" * 60)
    print("Integration Test 5: Fallback to sparkmagic config")
    print("=" * 60)

    webhdfs_config_dir = Path.home() / ".webhdfsmagic"
    webhdfs_config_file = webhdfs_config_dir / "config.json"
    webhdfs_backup_file = webhdfs_config_dir / "config.json.backup"

    sparkmagic_config_dir = Path.home() / ".sparkmagic"
    sparkmagic_config_file = sparkmagic_config_dir / "config.json"
    sparkmagic_backup_file = sparkmagic_config_dir / "config.json.backup"

    # Backup existing configs if present
    if webhdfs_config_file.exists():
        webhdfs_config_file.rename(webhdfs_backup_file)
    if sparkmagic_config_file.exists():
        sparkmagic_config_file.rename(sparkmagic_backup_file)

    try:
        # Create ONLY sparkmagic config (no webhdfsmagic config)
        sparkmagic_config_dir.mkdir(exist_ok=True)
        sparkmagic_config = {
            "kernel_python_credentials": {
                "url": "https://sparkmagic-host:8443/gateway/cluster/livy/v1",
                "username": "sparkuser",
                "password": "sparkpass",
            },
            "verify_ssl": False,
        }
        with open(sparkmagic_config_file, "w") as f:
            json.dump(sparkmagic_config, f, indent=2)

        # Load extension
        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Verify that sparkmagic config was used and transformed correctly
        # URL transformation: removes last segment (/v1) and appends /webhdfs/v1
        # So: .../gateway/cluster/livy/v1 -> .../gateway/cluster/livy + /webhdfs/v1
        assert magics.knox_url == "https://sparkmagic-host:8443/gateway/cluster/livy/webhdfs/v1"
        assert magics.webhdfs_api == "/webhdfs/v1"
        assert magics.auth_user == "sparkuser"
        assert magics.auth_password == "sparkpass"
        assert magics.verify_ssl is False
        print(f"✅ Sparkmagic config used: knox_url={magics.knox_url}")
        print(f"✅ User from sparkmagic: {magics.auth_user}")
        print(f"✅ WebHDFS API: {magics.webhdfs_api}")

    finally:
        # Cleanup
        if sparkmagic_config_file.exists():
            sparkmagic_config_file.unlink()
        if webhdfs_backup_file.exists():
            webhdfs_backup_file.rename(webhdfs_config_file)
        if sparkmagic_backup_file.exists():
            sparkmagic_backup_file.rename(sparkmagic_config_file)


def test_no_config_files_uses_defaults():
    """Test that default values are used when no config files exist."""
    print("\n" + "=" * 60)
    print("Integration Test 6: No config files - use defaults")
    print("=" * 60)

    webhdfs_config_dir = Path.home() / ".webhdfsmagic"
    webhdfs_config_file = webhdfs_config_dir / "config.json"
    webhdfs_backup_file = webhdfs_config_dir / "config.json.backup"

    sparkmagic_config_dir = Path.home() / ".sparkmagic"
    sparkmagic_config_file = sparkmagic_config_dir / "config.json"
    sparkmagic_backup_file = sparkmagic_config_dir / "config.json.backup"

    # Backup existing configs if present
    if webhdfs_config_file.exists():
        webhdfs_config_file.rename(webhdfs_backup_file)
    if sparkmagic_config_file.exists():
        sparkmagic_config_file.rename(sparkmagic_backup_file)

    try:
        # Load extension without any config files
        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Verify that default values are used
        assert magics.knox_url == "https://localhost:8443/gateway/default"
        assert magics.webhdfs_api == "/webhdfs/v1"
        assert magics.verify_ssl is False
        print(f"✅ Default knox_url: {magics.knox_url}")
        print(f"✅ Default webhdfs_api: {magics.webhdfs_api}")
        print(f"✅ Default verify_ssl: {magics.verify_ssl}")

    finally:
        # Restore backups if they exist
        if webhdfs_backup_file.exists():
            webhdfs_backup_file.rename(webhdfs_config_file)
        if sparkmagic_backup_file.exists():
            sparkmagic_backup_file.rename(sparkmagic_config_file)


def test_sparkmagic_config_with_custom_livy_endpoint():
    """Test sparkmagic config with custom livy endpoint like /livy_for_spark3."""
    print("\n" + "=" * 60)
    print("Integration Test 7: Sparkmagic with custom livy endpoint")
    print("=" * 60)

    webhdfs_config_dir = Path.home() / ".webhdfsmagic"
    webhdfs_config_file = webhdfs_config_dir / "config.json"
    webhdfs_backup_file = webhdfs_config_dir / "config.json.backup"

    sparkmagic_config_dir = Path.home() / ".sparkmagic"
    sparkmagic_config_file = sparkmagic_config_dir / "config.json"
    sparkmagic_backup_file = sparkmagic_config_dir / "config.json.backup"

    # Backup existing configs if present
    if webhdfs_config_file.exists():
        webhdfs_config_file.rename(webhdfs_backup_file)
    if sparkmagic_config_file.exists():
        sparkmagic_config_file.rename(sparkmagic_backup_file)

    try:
        # Create sparkmagic config with custom livy endpoint
        sparkmagic_config_dir.mkdir(exist_ok=True)
        sparkmagic_config = {
            "kernel_python_credentials": {
                "username": "user",
                "password": "password",
                "url": "https://hostname:port/gateway/default/livy_for_spark3",
                "auth": "Basic_Access",
            },
            "verify_ssl": False,
        }
        with open(sparkmagic_config_file, "w") as f:
            json.dump(sparkmagic_config, f, indent=2)

        # Load extension
        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Verify that URL was correctly transformed
        # Should remove /livy_for_spark3 and append /webhdfs/v1
        assert magics.knox_url == "https://hostname:port/gateway/default/webhdfs/v1"
        assert magics.webhdfs_api == "/webhdfs/v1"
        assert magics.auth_user == "user"
        assert magics.auth_password == "password"
        assert magics.verify_ssl is False
        print(f"✅ URL transformed correctly: {magics.knox_url}")
        print(f"✅ User: {magics.auth_user}")
        print("✅ Custom livy endpoint handled properly")

    finally:
        # Cleanup
        if sparkmagic_config_file.exists():
            sparkmagic_config_file.unlink()
        if webhdfs_backup_file.exists():
            webhdfs_backup_file.rename(webhdfs_config_file)
        if sparkmagic_backup_file.exists():
            sparkmagic_backup_file.rename(sparkmagic_config_file)


def test_sparkmagic_config_variations():
    """Test sparkmagic config with various URL patterns."""
    print("\n" + "=" * 60)
    print("Integration Test 8: Sparkmagic URL variations")
    print("=" * 60)

    webhdfs_config_dir = Path.home() / ".webhdfsmagic"
    webhdfs_config_file = webhdfs_config_dir / "config.json"
    webhdfs_backup_file = webhdfs_config_dir / "config.json.backup"

    sparkmagic_config_dir = Path.home() / ".sparkmagic"
    sparkmagic_config_file = sparkmagic_config_dir / "config.json"
    sparkmagic_backup_file = sparkmagic_config_dir / "config.json.backup"

    # Backup existing configs if present
    if webhdfs_config_file.exists():
        webhdfs_config_file.rename(webhdfs_backup_file)
    if sparkmagic_config_file.exists():
        sparkmagic_config_file.rename(sparkmagic_backup_file)

    test_cases = [
        {
            "name": "livy/v1 endpoint",
            "url": "https://host1:8443/gateway/cluster/livy/v1",
            "expected": "https://host1:8443/gateway/cluster/livy/webhdfs/v1",
        },
        {
            "name": "livy_for_spark3 endpoint",
            "url": "https://host2:8443/gateway/default/livy_for_spark3",
            "expected": "https://host2:8443/gateway/default/webhdfs/v1",
        },
        {
            "name": "livy endpoint without version",
            "url": "https://host3:8443/gateway/production/livy",
            "expected": "https://host3:8443/gateway/production/webhdfs/v1",
        },
    ]

    try:
        sparkmagic_config_dir.mkdir(exist_ok=True)

        for test_case in test_cases:
            print(f"\n  Testing: {test_case['name']}")

            # Create sparkmagic config
            sparkmagic_config = {
                "kernel_python_credentials": {
                    "username": "testuser",
                    "password": "testpass",
                    "url": test_case["url"],
                }
            }
            with open(sparkmagic_config_file, "w") as f:
                json.dump(sparkmagic_config, f, indent=2)

            # Load extension
            shell = InteractiveShell.instance()
            magics = WebHDFSMagics(shell)

            # Verify transformation
            assert magics.knox_url == test_case["expected"], (
                f"Expected {test_case['expected']}, got {magics.knox_url}"
            )
            print(f"    ✅ {test_case['url']} → {magics.knox_url}")

    finally:
        # Cleanup
        if sparkmagic_config_file.exists():
            sparkmagic_config_file.unlink()
        if webhdfs_backup_file.exists():
            webhdfs_backup_file.rename(webhdfs_config_file)
        if sparkmagic_backup_file.exists():
            sparkmagic_backup_file.rename(sparkmagic_config_file)


def main():
    """Run all integration tests."""
    print("\n" + "=" * 60)
    print("Integration Tests: Configuration Loading")
    print("=" * 60)

    test_load_config_no_ssl()
    test_load_config_with_ssl()
    test_load_config_with_cert()
    test_config_priority_webhdfsmagic_over_sparkmagic()
    test_fallback_to_sparkmagic_config()
    test_no_config_files_uses_defaults()
    test_sparkmagic_config_with_custom_livy_endpoint()
    test_sparkmagic_config_variations()
    test_load_webhdfsmagic_config_exception()
    test_load_sparkmagic_config_exception()
    test_verify_ssl_nonexistent_cert()
    test_verify_ssl_unexpected_type()
    test_transform_url_without_final_segment()

    print("\n" + "=" * 60)
    print("✅ All integration tests passed!")
    print("=" * 60)


def test_load_webhdfsmagic_config_exception():
    """Test exception handling in _load_webhdfsmagic_config (lines 65-67)."""
    print("\n" + "=" * 60)
    print("Test: Exception handling in webhdfsmagic config loading")
    print("=" * 60)

    config_dir = Path.home() / ".webhdfsmagic"
    config_file = config_dir / "config.json"
    backup_file = config_dir / "config.json.backup"

    if config_file.exists():
        config_file.rename(backup_file)

    try:
        config_dir.mkdir(parents=True, exist_ok=True)
        # Write invalid JSON
        with open(config_file, "w") as f:
            f.write("{ invalid json }")

        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Should fall back to defaults (can be knox-gateway or localhost depending on env)
        assert "8443/gateway/default" in magics.knox_url
        print("✅ Handled invalid JSON gracefully, fell back to defaults")

    finally:
        if config_file.exists():
            config_file.unlink()
        if backup_file.exists():
            backup_file.rename(config_file)


def test_load_sparkmagic_config_exception():
    """Test exception handling in _load_sparkmagic_config (lines 94-96)."""
    print("\n" + "=" * 60)
    print("Test: Exception handling in sparkmagic config loading")
    print("=" * 60)

    config_dir_whdfs = Path.home() / ".webhdfsmagic"
    config_dir_spark = Path.home() / ".sparkmagic"
    config_file_spark = config_dir_spark / "config.json"
    backup_file = config_dir_spark / "config.json.backup"

    # Ensure no webhdfsmagic config exists
    whdfs_config = config_dir_whdfs / "config.json"
    whdfs_backup = config_dir_whdfs / "config.json.backup"
    if whdfs_config.exists():
        whdfs_config.rename(whdfs_backup)

    if config_file_spark.exists():
        config_file_spark.rename(backup_file)

    try:
        config_dir_spark.mkdir(parents=True, exist_ok=True)
        # Write invalid JSON
        with open(config_file_spark, "w") as f:
            f.write("{ invalid json }")

        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Should fall back to defaults (can be knox-gateway or localhost depending on env)
        assert "8443/gateway/default" in magics.knox_url
        print("✅ Handled invalid sparkmagic JSON gracefully")

    finally:
        if config_file_spark.exists():
            config_file_spark.unlink()
        if backup_file.exists():
            backup_file.rename(config_file_spark)
        if whdfs_backup.exists():
            whdfs_backup.rename(whdfs_config)


def test_verify_ssl_nonexistent_cert():
    """Test verify_ssl falls back to False for nonexistent cert (lines 138-142)."""
    print("\n" + "=" * 60)
    print("Test: verify_ssl with nonexistent certificate file")
    print("=" * 60)

    config_dir = Path.home() / ".webhdfsmagic"
    config_file = config_dir / "config.json"
    backup_file = config_dir / "config.json.backup"

    if config_file.exists():
        config_file.rename(backup_file)

    try:
        config_dir.mkdir(parents=True, exist_ok=True)
        test_config = {"knox_url": "https://test:8443", "verify_ssl": "/nonexistent/cert.pem"}
        with open(config_file, "w") as f:
            json.dump(test_config, f, indent=2)

        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Should fall back to False
        assert magics.client.verify_ssl is False
        print("✅ Nonexistent cert file falls back to False")

    finally:
        if config_file.exists():
            config_file.unlink()
        if backup_file.exists():
            backup_file.rename(config_file)


def test_verify_ssl_unexpected_type():
    """Test verify_ssl handles unexpected type (lines 143-145)."""
    print("\n" + "=" * 60)
    print("Test: verify_ssl with unexpected type")
    print("=" * 60)

    config_dir = Path.home() / ".webhdfsmagic"
    config_file = config_dir / "config.json"
    backup_file = config_dir / "config.json.backup"

    if config_file.exists():
        config_file.rename(backup_file)

    try:
        config_dir.mkdir(parents=True, exist_ok=True)
        test_config = {
            "knox_url": "https://test:8443",
            "verify_ssl": 123,  # Invalid type
        }
        with open(config_file, "w") as f:
            json.dump(test_config, f, indent=2)

        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Should fall back to False
        assert magics.client.verify_ssl is False
        print("✅ Unexpected verify_ssl type falls back to False")

    finally:
        if config_file.exists():
            config_file.unlink()
        if backup_file.exists():
            backup_file.rename(config_file)


def test_transform_url_without_final_segment():
    """Test URL transformation when path has no final segment (line 115 - else branch)."""
    print("\n" + "=" * 60)
    print("Test: URL transformation without final segment")
    print("=" * 60)

    from webhdfsmagic.config import ConfigurationManager

    config = ConfigurationManager()

    # Test normal case - removes last segment
    url1 = "https://knox:8443/gateway/default/livy/v1"
    result1 = config._transform_sparkmagic_url(url1)
    # Should keep base path and add /webhdfs/v1
    assert "gateway/default/livy/webhdfs/v1" in result1
    print(f"✅ {url1} → {result1}")

    # Test edge case that triggers line 115 (else branch)
    # When path_parts = ['', 'gateway'] and path_parts[-1] = 'gateway' (truthy)
    # len(path_parts) = 2, path_parts[-1] = 'gateway' → takes if branch
    # When path ends with / and becomes empty: line 115 (else)
    url2 = "https://knox:8443/"
    result2 = config._transform_sparkmagic_url(url2)
    assert "/webhdfs/v1" in result2
    print(f"✅ {url2} → {result2}")


if __name__ == "__main__":
    main()
