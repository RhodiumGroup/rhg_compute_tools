import os

import pytest

import rhg_compute_tools

styles = [
    s.split(".")[0]
    for s in os.listdir(
        os.path.join(os.path.dirname(rhg_compute_tools.__file__), "design", "styles")
    )
]


def test_imports():
    import matplotlib.cm
    import matplotlib.colors

    import rhg_compute_tools.design

    assert isinstance(
        matplotlib.cm.get_cmap("rhg_Blues"), matplotlib.colors.LinearSegmentedColormap
    )


@pytest.mark.parametrize("style", styles)
def test_mplstyles(style):
    import matplotlib.style

    with matplotlib.style.context(style):
        pass
