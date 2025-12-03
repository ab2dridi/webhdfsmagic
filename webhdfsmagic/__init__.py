"""
Package webhdfsmagic

This package provides an IPython extension for interacting with HDFS via WebHDFS/Knox.
"""

from .magics import WebHDFSMagics as WebHDFSMagics
from .magics import load_ipython_extension as load_ipython_extension

__all__ = ["WebHDFSMagics", "load_ipython_extension"]
