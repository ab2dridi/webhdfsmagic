"""
Package webhdfsmagic

This package provides an IPython extension for interacting with HDFS via WebHDFS/Knox.
"""

__version__ = "0.0.2"

from .magics import WebHDFSMagics as WebHDFSMagics
from .magics import load_ipython_extension as load_ipython_extension

__all__ = ["WebHDFSMagics", "load_ipython_extension", "__version__"]

# Auto-configure on first import (only once per environment)
def _setup_autoload():
    """Set up automatic loading of webhdfsmagic in Jupyter/IPython."""
    try:
        from .install import install_autoload
        install_autoload()
    except Exception:
        pass  # Silently fail - don't break imports


# Run setup automatically on import
try:
    from pathlib import Path

    # Check if startup script exists (better indicator than marker file)
    ipython_startup = (
        Path.home() / ".ipython" / "profile_default" / "startup" / "00-webhdfsmagic.py"
    )

    if not ipython_startup.exists():
        # Startup script doesn't exist, try to create it
        _setup_autoload()

        # Create marker file to track that we attempted installation
        marker_file = Path.home() / ".webhdfsmagic" / ".installed"
        marker_file.parent.mkdir(parents=True, exist_ok=True)
        marker_file.touch()
except Exception:
    pass  # Don't break imports if setup fails

