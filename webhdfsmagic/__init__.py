"""
Package webhdfsmagic

This package provides an IPython extension for interacting with HDFS via WebHDFS/Knox.
"""

__version__ = "0.0.2"

from .magics import WebHDFSMagics as WebHDFSMagics
from .magics import load_ipython_extension as load_ipython_extension

__all__ = ["WebHDFSMagics", "load_ipython_extension", "__version__"]

# Auto-configure on first import (only once)
def _setup_autoload():
    """Set up automatic loading of webhdfsmagic in Jupyter/IPython."""
    try:
        from .install import install_autoload
        install_autoload()
    except Exception:
        pass  # Silently fail - don't break imports


# Run setup only once per installation
try:
    import os
    from pathlib import Path

    # Check if already set up by looking for the marker file
    marker_file = Path(os.path.expanduser("~/.webhdfsmagic/.installed"))
    if not marker_file.exists():
        _setup_autoload()
        # Create marker to avoid running again
        marker_file.parent.mkdir(parents=True, exist_ok=True)
        marker_file.touch()
except Exception:
    pass  # Don't break imports if setup fails
