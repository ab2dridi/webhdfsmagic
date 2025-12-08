"""Tests for automatic loading functionality."""

from unittest.mock import patch

from webhdfsmagic.install import get_ipython_startup_dir, install_autoload


def test_get_ipython_startup_dir():
    """Test getting IPython startup directory path."""
    startup_dir = get_ipython_startup_dir()
    assert "ipython" in startup_dir.lower()
    assert "startup" in startup_dir


def test_install_autoload_creates_script(tmp_path, monkeypatch):
    """Test that install_autoload creates the startup script."""
    # Create temp startup directory
    startup_dir = tmp_path / ".ipython" / "profile_default" / "startup"

    # Mock get_ipython_startup_dir to return temp path
    monkeypatch.setattr(
        "webhdfsmagic.install.get_ipython_startup_dir",
        lambda: str(startup_dir)
    )

    # Install
    result = install_autoload()

    assert result is True

    # Check file was created
    script_file = startup_dir / "00-webhdfsmagic.py"
    assert script_file.exists()

    # Check content
    content = script_file.read_text()
    assert "webhdfsmagic" in content
    assert "load_extension" in content


def test_install_autoload_idempotent(tmp_path, monkeypatch):
    """Test that install_autoload is idempotent."""
    startup_dir = tmp_path / ".ipython" / "profile_default" / "startup"

    monkeypatch.setattr(
        "webhdfsmagic.install.get_ipython_startup_dir",
        lambda: str(startup_dir)
    )

    # Install twice
    result1 = install_autoload()
    result2 = install_autoload()

    assert result1 is True
    assert result2 is True

    # Should only create one file
    script_file = startup_dir / "00-webhdfsmagic.py"
    assert script_file.exists()


def test_install_autoload_handles_errors(monkeypatch):
    """Test that install_autoload handles errors gracefully."""
    # Mock to raise an exception
    monkeypatch.setattr(
        "webhdfsmagic.install.get_ipython_startup_dir",
        lambda: "/nonexistent/path/that/will/fail"
    )

    # Mock Path.mkdir to raise exception
    def mock_mkdir(*args, **kwargs):
        raise PermissionError("No permission")

    with patch('pathlib.Path.mkdir', side_effect=mock_mkdir):
        result = install_autoload()
        # Should return False but not raise
        assert result is False


def test_startup_script_content(tmp_path, monkeypatch):
    """Test the content of the generated startup script."""
    startup_dir = tmp_path / ".ipython" / "profile_default" / "startup"

    monkeypatch.setattr(
        "webhdfsmagic.install.get_ipython_startup_dir",
        lambda: str(startup_dir)
    )

    install_autoload()

    script_file = startup_dir / "00-webhdfsmagic.py"
    content = script_file.read_text()

    # Check for proper error handling
    assert "try:" in content
    assert "except Exception:" in content
    assert "pass" in content

    # Should not print errors
    assert "print" not in content
