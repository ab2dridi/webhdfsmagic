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


class TestInstallMain:
    """Test install.py main() function."""

    def test_main_success(self):
        """Test main() function with successful installation."""
        from unittest.mock import patch

        from webhdfsmagic.install import main

        with patch('webhdfsmagic.install.install_autoload', return_value=True):
            with patch('builtins.print') as mock_print:
                result = main()

                assert result == 0
                calls = [str(call) for call in mock_print.call_args_list]
                assert any("complete" in str(call).lower() for call in calls)

    def test_main_failure(self):
        """Test main() function with failed installation."""
        from unittest.mock import patch

        from webhdfsmagic.install import main

        with patch('webhdfsmagic.install.install_autoload', return_value=False):
            with patch('builtins.print') as mock_print:
                result = main()

                assert result == 1
                calls = [str(call) for call in mock_print.call_args_list]
                assert any("failed" in str(call).lower() for call in calls)

    def test_install_autoload_with_stderr_warning(self):
        """Test install_autoload prints warning to stderr on failure."""
        from unittest.mock import patch

        from webhdfsmagic.install import install_autoload

        with patch(
            'webhdfsmagic.install.get_ipython_startup_dir',
            side_effect=Exception("No IPython")
        ):
            with patch('sys.stderr'):
                result = install_autoload()

                assert result is False

    def test_install_autoload_without_stderr(self):
        """Test install_autoload when sys.stderr is not available."""
        import sys
        from unittest.mock import patch

        from webhdfsmagic.install import install_autoload

        with patch(
            'webhdfsmagic.install.get_ipython_startup_dir',
            side_effect=Exception("Test error")
        ):
            old_stderr = sys.stderr
            try:
                del sys.stderr
                result = install_autoload()
                assert result is False
            finally:
                sys.stderr = old_stderr


class TestInitAutoSetup:
    """Test __init__.py auto-setup logic."""

    def test_setup_autoload_success(self):
        """Test successful auto-setup on import."""
        from unittest.mock import patch

        with patch(
            'webhdfsmagic.install.install_autoload', return_value=True
        ) as mock_install:
            from webhdfsmagic import _setup_autoload

            _setup_autoload()

            mock_install.assert_called_once()

    def test_setup_autoload_exception(self):
        """Test auto-setup handles exceptions silently."""
        from unittest.mock import patch

        with patch(
            'webhdfsmagic.install.install_autoload', side_effect=Exception("Test error")
        ):
            from webhdfsmagic import _setup_autoload

            _setup_autoload()

    def test_auto_setup_checks_startup_script(self):
        """Test that auto-setup checks for startup script existence."""
        import importlib
        from unittest.mock import patch

        import webhdfsmagic

        with patch('pathlib.Path.exists', return_value=False):
            with patch('webhdfsmagic.install.install_autoload', return_value=True):
                with patch('pathlib.Path.touch'):
                    with patch('pathlib.Path.mkdir'):
                        importlib.reload(webhdfsmagic)

    def test_auto_setup_skips_if_already_installed(self):
        """Test that auto-setup is skipped if startup script exists."""
        import importlib
        from unittest.mock import patch

        import webhdfsmagic

        with patch('pathlib.Path.exists', return_value=True):
            with patch('webhdfsmagic.install.install_autoload'):
                importlib.reload(webhdfsmagic)
