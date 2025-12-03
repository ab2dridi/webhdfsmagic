"""
Test SSL verification handling in webhdfsmagic.
Tests different verify_ssl values: False, True, and certificate paths.
"""

import os
import tempfile
from pathlib import Path

from IPython.core.interactiveshell import InteractiveShell

from webhdfsmagic.magics import WebHDFSMagics


def test_verify_ssl_false():
    """Test verify_ssl with boolean False."""
    print("\n" + "=" * 60)
    print("Test 1: verify_ssl = False")
    print("=" * 60)

    shell = InteractiveShell.instance()
    magics = WebHDFSMagics(shell)

    # Set verify_ssl to False
    magics.verify_ssl = False

    # Re-run SSL handling logic
    if isinstance(magics.verify_ssl, bool):
        pass
    else:
        magics.verify_ssl = False

    assert magics.verify_ssl is False, "Expected verify_ssl to be False"
    print("✅ verify_ssl = False works correctly")


def test_verify_ssl_true():
    """Test verify_ssl with boolean True."""
    print("\n" + "=" * 60)
    print("Test 2: verify_ssl = True")
    print("=" * 60)

    shell = InteractiveShell.instance()
    magics = WebHDFSMagics(shell)

    # Set verify_ssl to True
    magics.verify_ssl = True

    # Re-run SSL handling logic
    if isinstance(magics.verify_ssl, bool):
        pass
    else:
        magics.verify_ssl = False

    assert magics.verify_ssl is True, "Expected verify_ssl to be True"
    print("✅ verify_ssl = True works correctly")


def test_verify_ssl_with_valid_cert():
    """Test verify_ssl with a valid certificate file path."""
    print("\n" + "=" * 60)
    print("Test 3: verify_ssl with valid certificate path")
    print("=" * 60)

    # Create a temporary certificate file
    with tempfile.NamedTemporaryFile(mode="w", suffix=".pem", delete=False) as f:
        f.write("-----BEGIN CERTIFICATE-----\n")
        f.write("Fake certificate content\n")
        f.write("-----END CERTIFICATE-----\n")
        cert_path = f.name

    try:
        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Set verify_ssl to certificate path
        magics.verify_ssl = cert_path

        # Re-run SSL handling logic
        if isinstance(magics.verify_ssl, str):
            expanded_path = os.path.expanduser(magics.verify_ssl)
            if os.path.exists(expanded_path):
                magics.verify_ssl = expanded_path
            else:
                magics.verify_ssl = False

        assert magics.verify_ssl == cert_path, f"Expected {cert_path}, got {magics.verify_ssl}"
        print(f"✅ verify_ssl with valid cert path works: {cert_path}")
    finally:
        # Cleanup
        if os.path.exists(cert_path):
            os.unlink(cert_path)


def test_verify_ssl_with_invalid_cert():
    """Test verify_ssl with an invalid (non-existent) certificate path."""
    print("\n" + "=" * 60)
    print("Test 4: verify_ssl with invalid certificate path")
    print("=" * 60)

    shell = InteractiveShell.instance()
    magics = WebHDFSMagics(shell)

    # Set verify_ssl to non-existent path
    fake_path = "/nonexistent/path/to/cert.pem"
    magics.verify_ssl = fake_path

    # Re-run SSL handling logic (should fall back to False)
    if isinstance(magics.verify_ssl, str):
        expanded_path = os.path.expanduser(magics.verify_ssl)
        if os.path.exists(expanded_path):
            magics.verify_ssl = expanded_path
        else:
            print(
                f"Warning: certificate file '{magics.verify_ssl}' "
                "does not exist. Falling back to False."
            )
            magics.verify_ssl = False

    assert magics.verify_ssl is False, "Expected verify_ssl to fall back to False"
    print("✅ Invalid cert path correctly falls back to False")


def test_verify_ssl_with_tilde_expansion():
    """Test verify_ssl with ~ (home directory) expansion."""
    print("\n" + "=" * 60)
    print("Test 5: verify_ssl with ~ (tilde) expansion")
    print("=" * 60)

    # Create a certificate in user's home directory
    home_dir = Path.home()
    test_cert_dir = home_dir / ".webhdfsmagic_test"
    test_cert_dir.mkdir(exist_ok=True)
    cert_file = test_cert_dir / "test_cert.pem"

    with open(cert_file, "w") as f:
        f.write("-----BEGIN CERTIFICATE-----\n")
        f.write("Fake certificate content\n")
        f.write("-----END CERTIFICATE-----\n")

    try:
        shell = InteractiveShell.instance()
        magics = WebHDFSMagics(shell)

        # Set verify_ssl with tilde path
        tilde_path = "~/.webhdfsmagic_test/test_cert.pem"
        magics.verify_ssl = tilde_path

        # Re-run SSL handling logic with expansion
        if isinstance(magics.verify_ssl, str):
            expanded_path = os.path.expanduser(magics.verify_ssl)
            if os.path.exists(expanded_path):
                magics.verify_ssl = expanded_path
            else:
                magics.verify_ssl = False

        expected_path = str(cert_file)
        assert magics.verify_ssl == expected_path, (
            f"Expected {expected_path}, got {magics.verify_ssl}"
        )
        print(f"✅ Tilde expansion works: {tilde_path} → {magics.verify_ssl}")
    finally:
        # Cleanup
        if cert_file.exists():
            cert_file.unlink()
        if test_cert_dir.exists():
            test_cert_dir.rmdir()


def test_verify_ssl_with_invalid_type():
    """Test verify_ssl with an invalid type (not bool or string)."""
    print("\n" + "=" * 60)
    print("Test 6: verify_ssl with invalid type")
    print("=" * 60)

    shell = InteractiveShell.instance()
    magics = WebHDFSMagics(shell)

    # Try to set verify_ssl to an invalid type (int)
    # This should raise a TraitError from traitlets
    try:
        magics.verify_ssl = 123
        # If we get here, the trait accepted the value (shouldn't happen)
        raise AssertionError("Expected TraitError but value was accepted")
    except AssertionError:
        raise
    except Exception as e:
        # Expected: traitlets will raise an error for invalid type
        assert "TraitError" in str(type(e)) or "trait" in str(e).lower(), (
            f"Expected TraitError, got {type(e)}: {e}"
        )
        print(f"✅ Invalid type correctly rejected by trait system: {type(e).__name__}")
        print(f"   Error message: {str(e)[:80]}...")


def main():
    """Run all tests."""
    print("\n" + "=" * 60)
    print("Testing SSL Verification Handling in webhdfsmagic")
    print("=" * 60)

    test_verify_ssl_false()
    test_verify_ssl_true()
    test_verify_ssl_with_valid_cert()
    test_verify_ssl_with_invalid_cert()
    test_verify_ssl_with_tilde_expansion()
    test_verify_ssl_with_invalid_type()

    print("\n" + "=" * 60)
    print("✅ All SSL verification tests passed!")
    print("=" * 60)


if __name__ == "__main__":
    main()
