import fsspec
import os
import re
import yaml
import tempfile
import rasterio
import numpy as np
import pandas as pd
import xarray as xr
import geopandas as gpd
from zarr.errors import GroupNotFoundError
from contextlib import contextmanager
import contextlib

try:
    from gcsfs.retry import HttpError as GCSFSHttpError
except ImportError:
    from gcsfs.utils import HttpError as GCSFSHttpError


def get_fs(fp) -> fsspec.AbstractFileSystem:

    filesystem = "" if "://" not in fp else fp.split("://")[0]

    fs = fsspec.filesystem(
        filesystem,
        cache_timeout=-1,
        timeout=120,
        requests_timeout=120,
        read_timeout=120,
        conn_timeout=120,
    )

    return fs


@contextmanager
def report_if_not_found(fp):
    try:
        yield

    except (FileNotFoundError, GroupNotFoundError):
        raise FileNotFoundError(fp)

    except KeyError as e:
        if ".zmetadata" not in str(e.args[0]):
            raise

        raise FileNotFoundError(fp)


def read_zarr(
    fp,
    fs=None,
    mapper_kwargs=None,
    isel_dict: dict = None,
    load_data: bool = False,
    **kwargs,
) -> xr.Dataset:

    if fs is None:
        fs = get_fs(fp)

    with report_if_not_found(fp):
        mapper = fs.get_mapper(fp)
        ds = xr.open_zarr(mapper, **kwargs)
        if isel_dict is not None:
            assert isinstance(isel_dict, dict), "`sel_dict_method` should be a dict"
            assert all(
                [x in ds.dims for x in isel_dict]
            ), f"all keys of `isel_dict` are not in ds.dims	{isel_dict.keys(), list(ds.dims)}"
            ds = ds.isel(isel_dict)
        if load_data:
            ds = ds.load()
        return ds


def load_netcdf(blob, fsspec_kwargs=None, *args, retries=5, **kwargs) -> xr.Dataset:
    """Read a geotiff or raster file from a local or gs:// location"""

    if fsspec_kwargs is None:
        fsspec_kwargs = {}

    for i in range(retries + 1):
        try:
            if os.path.exists(blob):
                with xr.open_dataset(blob, *args, **kwargs) as ds:
                    #                     yield ds
                    ds = ds.load()
                    return ds
                break

            elif "://" in str(blob):
                with tempfile.NamedTemporaryFile(suffix=".nc") as tmp_fp:
                    fp = str(tmp_fp.name)
                    protocol = re.match(r"(\w+)://", blob, re.I).group(1)
                    fs = fsspec.filesystem(protocol, **fsspec_kwargs)
                    fs.get(blob, fp)
                    with xr.open_dataset(fp, *args, **kwargs) as ds:
                        #                         yield ds
                        ds = ds.load()
                        return ds
                    break

        except (IOError, GCSFSHttpError) as e:
            if i >= retries:
                raise OSError(f"read aborted after {i} retry attempts: {e}")


def read_dataset(fp, engine=None, **kwargs) -> xr.Dataset:
    if engine is None:
        if fp.endswith(".zarr"):
            engine = "zarr"
        elif fp.endswith(".nc") or fp.endswith(".nc4"):
            engine = "netcdf4"
        else:
            raise IOError(f"engine could not be auto-determined from fp: {fp}")

    if engine == "zarr":
        return read_zarr(fp, **kwargs)
    elif engine == "netcdf4":
        return load_netcdf(fp, **kwargs)
    else:
        raise IOError(
            f"engine not recognized: {engine}. Choose one of {{'zarr', 'netcdf4'}}."
        )


def read_shapefile(fp):
    with fsspec.open(fp) as f:
        return gpd.read_file(f)


@contextlib.contextmanager
def read_rasterio(blob, fsspec_kwargs=None, *args, retries=5, **kwargs) -> xr.DataArray:
    """Read a NETCDF file from a local or gs:// location"""

    if fsspec_kwargs is None:
        fsspec_kwargs = {}

    for i in range(retries + 1):
        try:
            if os.path.exists(blob):
                with xr.open_rasterio(blob) as ds:
                    yield ds
                break

            elif "://" in str(blob):
                with tempfile.TemporaryDirectory() as tmpdir:
                    f = os.path.join(tmpdir, os.path.basename(blob))
                    fs = fsspec.filesystem(blob.split("://")[0])
                    fs.get(blob.replace("/gcs/", "gs://"), f)
                    with rasterio.open(f) as data:
                        with xr.open_rasterio(data) as ds:
                            yield ds
                        break
            else:
                raise ValueError("file protocol not recognized: {blob}")

        except (IOError, GCSFSHttpError) as e:
            if i >= retries:
                raise OSError(f"read aborted after {i} retry attempts: {e}")


@contextlib.contextmanager
def read_netcdf(blob, fsspec_kwargs=None, *args, retries=5, **kwargs) -> xr.Dataset:
    """Read a geotiff or raster file from a local or gs:// location. Very similar to load_netcdf, but without the load."""

    if fsspec_kwargs is None:
        fsspec_kwargs = {}

    for i in range(retries + 1):
        try:
            if os.path.exists(blob):
                with xr.open_dataset(blob, *args, **kwargs) as ds:
                    yield ds
                break

            elif "://" in str(blob):
                with tempfile.NamedTemporaryFile(suffix=".nc") as tmp_fp:
                    fp = str(tmp_fp.name)
                    protocol = re.match(r"(\w+)://", blob, re.I).group(1)
                    fs = fsspec.filesystem(protocol, **fsspec_kwargs)
                    fs.get(blob, fp)
                    with xr.open_dataset(fp, *args, **kwargs) as ds:
                        yield ds
                    break

        except (IOError, GCSFSHttpError) as e:
            if i >= retries:
                raise OSError(f"read aborted after {i} retry attempts: {e}")


def read_csv(blob, **fsspec_kwargs) -> pd.DataFrame:
    """
    Read a csv file from a local or gs:// location
    """
    if not "://" in str(blob):
        fsspec_kwargs = {}

    return pd.read_csv(blob, **fsspec_kwargs)


def read_parquet(blob, **fsspec_kwargs) -> pd.DataFrame:
    """
    Read a parquet file from a local or gs:// location
    """
    if not "://" in str(blob):
        fsspec_kwargs = {}

    return pd.read_parquet(blob, **fsspec_kwargs)


def read_dataframe(blob, **fsspec_kwargs) -> pd.DataFrame:
    """
    Read a CSV or parquet file from a local or gs:// location
    """

    if blob.endswith(".csv") or blob.endswith(".txt"):
        return read_csv(blob, **fsspec_kwargs)
    elif blob.endswith(".parquet"):
        return read_parquet(blob, **fsspec_kwargs)
    else:
        parts = os.path.basename(blob).split(".")
        if len(parts) == 1:
            raise ValueError("No extension could be inferred for file: {}".format(blob))

        ext = ".".join(parts[1:])

        raise ValueError(
            "File type could not be inferred from extension: {}".format(ext)
        )


def read_csvv_response(fp) -> xr.Dataset:
    with fsspec.open(fp, "r") as f:
        firstline = f.readline().strip()
        assert firstline == "---", firstline
        header, data = f.read().split("...\n")
        header = yaml.safe_load(header)

        fields = [
            "observations",
            "prednames",
            "covarnames",
            "obsnames",
            "gamma",
            "gammavcv",
            "residvcv",
        ]

        parsed_data = {}

        current_field = None
        current = []
        for line in data.split("\n"):
            if line.strip() in fields:
                if current_field is not None:
                    if "names" in current_field:
                        parsed_data[current_field] = np.array(
                            [c.strip().rstrip(",").split(",") for c in current]
                        )
                    else:
                        parsed_data[current_field] = np.loadtxt(current, delimiter=",")

                current = []
                current_field = line.strip()

            else:
                current.append(line.strip().rstrip(",").replace(", ", ","))

    if "names" in current_field:
        parsed_data[current_field] = np.array(
            [c.strip().rstrip(",").split(",") for c in current]
        )
    else:
        parsed_data[current_field] = np.loadtxt(current, delimiter=",")

    if "obsnames" not in parsed_data:
        parsed_data["obsnames"] = np.array(
            ["outcome"] * len(parsed_data["prednames"].flat)
        )

    X = pd.MultiIndex.from_arrays(
        [
            parsed_data["prednames"].flat,
            parsed_data["covarnames"].flat,
            parsed_data["obsnames"].flat,
        ],
        names=["predictor", "covariate", "outcome"],
    )

    Y = pd.MultiIndex.from_arrays(
        [
            parsed_data["prednames"].flat,
            parsed_data["covarnames"].flat,
            parsed_data["obsnames"].flat,
        ],
        names=["predictor_y", "covariate_y", "outcome_y"],
    )

    ds = xr.Dataset(
        {
            "gamma": xr.DataArray(parsed_data["gamma"], [X], ["X"]),
            "vcv": xr.DataArray(parsed_data["gammavcv"], [X, Y], ["X", "Y"]),
            "residvcv": xr.DataArray(parsed_data["residvcv"], [], []),
        },
        attrs={k: v for k, v in header.items() if not isinstance(v, dict)},
    )

    dict_keys = [k for k, v in header.items() if isinstance(v, dict)]
    for dk in dict_keys:
        ds.attrs.update({f"{dk}_{k}": v for k, v in header[dk].items()})

    return ds
