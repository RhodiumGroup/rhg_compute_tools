import fsspec
import git
import xarray as xr
import pandas as pd

import dask
from pathlib import Path
import os
import zarr
from typing import Union

import rhg_compute_tools.xarray
from rhg_compute_tools.io_tools.readers import get_fs


def _xr_document_repo_state(ds: xr.Dataset, repo_root=".") -> xr.Dataset:

    repo = git.Repo(repo_root, search_parent_directories=True)
    repo_dir = os.path.dirname(repo.git_dir)

    rhg_compute_tools.xarray.document_dataset(ds, repo_dir)

    return ds


def document_dataset(ds: xr.Dataset, repo_root: str = ".") -> xr.Dataset:
    ds = _xr_document_repo_state(ds, repo_root=repo_root)
    ds.attrs["updated"] = pd.Timestamp.now(tz="UTC").strftime("%c (%Z)")

    return ds


def get_maximal_chunks_encoding(ds: xr.Dataset, **var_chunks) -> dict:
    encoding_kwargs = {"encoding": {}}
    for c in ds.coords.keys():
        if ds.coords[c].chunks is None:
            encoding_kwargs["encoding"][c] = {
                "chunks": tuple([-1 for _ in ds.coords[c].dims])
            }
        else:
            encoding_kwargs["encoding"][c] = {
                "chunks": tuple([max(v) for v in ds.coords[c].chunks])
            }
    for v in ds.data_vars.keys():
        if ds[v].chunks is None:
            encoding_kwargs["encoding"][v] = {
                "chunks": tuple([var_chunks.get(d, -1) for d in ds[v].dims])
            }
        else:
            encoding_kwargs["encoding"][v] = {
                "chunks": tuple([max(v) for v in ds[v].chunks])
            }

    return encoding_kwargs


def write_zarr(
    out_ds: xr.Dataset,
    out_fp: str,
    fs: Union[None, fsspec.filesystem] = None,
    set_maximal_chunks: bool = True,
    writer_kwargs: Union[dict, None] = None,
    encoding_kwargs: Union[dict, None] = None,
) -> None:
    if fs is None:
        fs = get_fs(out_fp)

    if writer_kwargs is None:
        writer_kwargs = {}

    assert isinstance(out_ds, xr.Dataset), (
        "Do not write a DataArray. Instead use da.to_dataset(name='variable_name') "
        "to convert to a Dataset, and then assign metadata prior to writing"
    )

    mapper = fs.get_mapper(out_fp)

    if encoding_kwargs is None:
        if set_maximal_chunks:
            encoding_kwargs = get_maximal_chunks_encoding(out_ds)
        else:
            encoding_kwargs = {}

    for v in list(out_ds.coords.keys()):
        if out_ds.coords[v].dtype == object and v != "time":
            out_ds.coords[v] = out_ds.coords[v].astype("unicode")

    for v in list(out_ds.variables.keys()):
        if out_ds[v].dtype == object and v != "time":
            out_ds[v] = out_ds[v].astype("unicode")

    try:
        futures = out_ds.to_zarr(
            mapper, compute=False, **writer_kwargs, **encoding_kwargs
        )
        dask.compute(futures, retries=3)
    except zarr.errors.ContainsGroupError:
        raise zarr.errors.ContainsGroupError(out_fp)


def write_netcdf(
    out_ds: xr.Dataset,
    out_fp: str,
    fs: Union[None, fsspec.filesystem] = None,
) -> None:

    assert isinstance(out_ds, xr.Dataset), (
        "for consistency, do not write a DataArray. Instead use da.to_dataset(name='variable_name') "
        "to convert to a Dataset, and then assign metadata prior to writing"
    )

    # netcdf requires the parent directory to be created
    parent_dir = "/".join(out_fp.split("/")[:-1]).replace("gs://", "/gcs/")
    os.makedirs(parent_dir, exist_ok=True)
    futures = out_ds.to_netcdf(out_fp.replace("gs://", "/gcs/"), compute=False)
    dask.compute(futures, retries=3)
