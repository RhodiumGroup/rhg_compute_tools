
def test_imports():
    import rhg_compute_tools.design
    import rhg_compute_tools.design.colors
    import matplotlib.cm

    assert isinstance(
        matplotlib.cm.get_cmap('rhg_Blues'),
        matplotlib.colors.LinearSegmentedColormap)
