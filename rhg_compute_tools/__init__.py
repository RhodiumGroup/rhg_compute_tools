# -*- coding: utf-8 -*-

"""Top-level package for RHG Compute Tools."""


import pkg_resources

__author__ = """Michael Delgado"""
__email__ = "mdelgado@rhg.com"

try:
    __version__ = pkg_resources.get_distribution("rhg_compute_tools").version    
except Exception:
    # Local copy or not installed with setuptools.
    # Disable minimum version checks on downstream libraries.
    __version__ = "999"
