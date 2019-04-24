
def test_imports():
    import rhg_compute_tools.design
    import matplotlib.cm
    import matplotlib.colors

    assert isinstance(
        matplotlib.cm.get_cmap('rhg_Blues'),
        matplotlib.colors.LinearSegmentedColormap)
