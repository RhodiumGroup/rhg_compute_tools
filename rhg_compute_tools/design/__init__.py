
from __future__ import absolute_import

from rhg_compute_tools.design.colors import _load_colors
from rhg_compute_tools.design.plotting import (
    get_color_scheme,
    add_colorbar)

_load_colors()

__all__ = ['get_color_scheme', 'add_colorbar']
