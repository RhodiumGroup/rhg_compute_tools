import matplotlib.cm
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

try:
    _string_types = (str, unicode)
except NameError:
    _string_types = (str,)


def get_color_scheme(values, cmap=None, colors=None, levels=None, how=None):
    """
    Generate a norm and color scheme from data

    Parameters
    ----------
    values : array-like
        data to be plotted, from which to generate cmap and norm.
        This should be an array, DataArray, etc. that we can use
        to find the min/max and/or quantiles of the data.
    cmap : str, optional
        named matplotlib cmap (default inferred from data)
    colors : list-like, optional
        list of colors to use in a discrete colormap, or with which
        to create a custom color map
    levels : list-like, optional
        boundaries of discrete colormap, provide
    how : str, optional
        Optional setting form ``{'linear', 'log', 'symlog', None}``.
        Used to construct the returned ``norm`` object, which defines
        the way the colors map to values. By default, we the method is
        inferred from the ``values``.

    Returns
    -------
    cmap : object
        :py:class:`matplotlib.colors.cmap` color mapping
    norm : object
        :py:class:`matplotlib.colors.Normalize` instance using the provided
        values, levels, color specification, and "how" method

    """

    mini, maxi = float(values.min()), float(values.max())
    amax = max(abs(mini), abs(maxi))

    if (cmap is None) and (colors is not None):
        cmap = LinearSegmentedColormap.from_list("custom_cmap", colors)
    elif cmap is None:
        if (mini < 0) and (maxi > 0):
            cmap = matplotlib.cm.RdBu_r
        else:
            cmap = matplotlib.cm.viridis
    elif isinstance(cmap, _string_types):
        cmap = matplotlib.cm.get_cmap(cmap)

    if how is None and levels is not None:
        norm = matplotlib.colors.BoundaryNorm(levels, cmap.N)

    elif (how is None) or (how == "eq_hist"):
        if levels is None:
            levels = 11

        bin_edges = np.percentile(
            values[~np.isnan(values)], np.linspace(0, 100, levels)
        )

        norm = matplotlib.colors.BoundaryNorm(bin_edges, cmap.N)

    elif how == "log":
        norm = matplotlib.colors.LogNorm(vmin=mini, vmax=maxi)

    elif how == "symlog":
        norm = matplotlib.colors.SymLogNorm(
            vmin=-amax, vmax=amax, linthresh=(amax / 100)
        )

    elif how == "linear":
        norm = matplotlib.colors.Normalize(vmin=mini, vmax=maxi)

    else:
        raise ValueError(
            "color scheme `how` argument {} not recognized. "
            "choose from {eq_hist, log, symlog, linear} or "
            "provide `levels`".format(how)
        )

    return cmap, norm


def add_colorbar(ax, cmap="viridis", norm=None, orientation="vertical", **kwargs):
    """
    Add a colorbar to a plot, using a pre-defined cmap and norm

    Parameters
    ----------

    ax : object
        matplotlib axis object
    cmap : str or object, optional
        :py:class:`matplotlib.colors.cmap` instance or name of a
        registered cmap (default viridis)
    norm: object, optional
        :py:class:`matplotlib.colors.Normalize` instance. default is a linear
        norm between the min and max of the first plotted object.
    orientation : str, optional
        default 'vertical'
    **kwargs :
        passed to colorbar constructor
    """

    if norm is None:
        norm = matplotlib.colors.Normalize

    n_cmap = matplotlib.cm.ScalarMappable(norm=norm, cmap=cmap)
    n_cmap.set_array([])
    cbar = ax.get_figure().colorbar(n_cmap, ax=ax, orientation=orientation, **kwargs)

    return cbar
