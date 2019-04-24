
import matplotlib.cm
from matplotlib.colors import LinearSegmentedColormap, ListedColormap

_custom_continuous_cmaps = {
    'cil_RdBu': [
        "#e0603f", "#ed7453", "#f68f50", "#ffaa4d",
        "#ffcb58", "#ffea80", "#ffffad", "#d9ffd9",
        "#bbeae4", "#88d8da", "#55c7d2", "#41bad1",
        "#41a9c1", "#4197b0"],

    'cil_Reds': [
        "#FFDB6C", "#ffcb58", "#FFBB53", "#ffaa4d",
        "#FB9D4F", "#f68f50", "#F28252", "#ed7453",
        "#E76A49", "#e0603f", "#D45433", "#c84726",
        "#b53919"],

    'cil_Blues': [
        "#A2E1DF", "#88d8da", "#6FD0D6", "#55c7d2",
        "#4BC1D2", "#41bad1", "#41B2C9", "#41a9c1",
        "#41A0B9", "#4197b0", "#398AA1", "#307c92",
        "#065b74"],

    'rhg_Blues': ["#ACD8F1", "#63AAD6", "#0078AD", "#055A7F", "#023B56"],
    'rhg_Greens': ["#C1E399", "#A0D55F", "#77B530", "#59901B", "#366108"],
    'rhg_Yellows': ["#FDE8A5", "#FBD568", "#E7B731", "#C49C20", "#926E00"],
    'rhg_Oranges': ["#F9C7A2", "#FEA569", "#E97625", "#C45506", "#913F05"],
    'rhg_Reds': ["#ECA5AB", "#E66967", "#C32524", "#920605", "#6C0405"],
    'rhg_Purples': ["#D7BBE3", "#BE95CF", "#915FA4", "#633A76", "#6C0405"],
}

_custom_discrete_cmaps = {
    'rhg_standard': [
        "#0078AD", "#77B530", "#E7B731", "#E97625", "#C32524", "#915FA4"],
    'rhg_light': [
        "#63AAD6", "#A0D55F", "#FBD568", "#FEA569", "#E66967", "#BE95CF"],
}


def _load_colors():
    for cmap_name, cmap_colors in _custom_continuous_cmaps.items():
        cmap = LinearSegmentedColormap.from_list(cmap_name, cmap_colors)
        matplotlib.cm.register_cmap(cmap=cmap)
        cmap_r = cmap.reversed()
        matplotlib.cm.register_cmap(cmap=cmap_r)

    for cmap_name, cmap_colors in _custom_discrete_cmaps.items():
        cmap = ListedColormap(cmap_colors, name=cmap_name)
        matplotlib.cm.register_cmap(cmap=cmap)
        cmap_r = cmap.reversed()
        matplotlib.cm.register_cmap(cmap=cmap_r)
