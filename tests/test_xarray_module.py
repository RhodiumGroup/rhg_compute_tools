
import pytest
import xarray as xr
import numpy as np
import dask.distributed as dd

import rhg_compute_tools.xarray as rhgxr

@pytest.fixture(scope='session')
def client():
    client = dd.Client()
    return client

def gen_dataarray(i):
    return xr.DataArray(
        np.arange(100 * i, 100 * (i + 1)).reshape(10, 10, 1, order='F'),
        dims=list('xyz'),
        coords={'z': [i]},
    )

def gen_dataset(i):
    return xr.Dataset({'a': gen_dataarray(i)})

def test_dataarrays_from_delayed(client):
    futures = client.map(gen_dataarray, range(10))

    res = xr.concat(rhgxr.dataarrays_from_delayed(futures), dim='z').compute()
    expected = xr.DataArray(
        np.arange(1000).reshape(10, 10, 10, order='F'),
        dims=list('xyz'),
        coords={'z': range(10)},
    )

    xr.testing.assert_equal(res, expected)

def test_dataset_from_delayed(client):
    futures = client.map(gen_dataset, range(10))

    res = xr.concat(rhgxr.datasets_from_delayed(futures), dim='z').compute()
    expected = xr.Dataset(
        {
            'a': xr.DataArray(
                np.arange(1000).reshape(10, 10, 10, order='F'),
                dims=list('xyz'),
                coords={'z': range(10)},
            ),
        },
    )

    xr.testing.assert_equal(res, expected)
