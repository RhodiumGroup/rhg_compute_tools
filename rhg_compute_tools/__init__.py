# -*- coding: utf-8 -*-

"""Top-level package for RHG Compute Tools."""


try:
    # Python 3.8+
    from importlib.metadata import version
except ImportError:
    # Python 3.7 fallback
    from importlib_metadata import version

__author__ = """Michael Delgado"""
__email__ = "mdelgado@rhg.com"

try:
    __version__ = version("rhg_compute_tools")
except Exception:
    # Local copy or not installed with setuptools.
    # Disable minimum version checks on downstream libraries.
    __version__ = "999"
