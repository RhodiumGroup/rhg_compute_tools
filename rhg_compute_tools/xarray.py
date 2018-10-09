import xarray as xr
import dask.array
import dask.distributed as dd


def dataarrays_from_delayed(futures, client=None):
    '''
    Returns a list of xarray dataarrays from a list of futures of dataarrays

    Parameters
    ----------
    futures : list
        list of :py:class:`dask.delayed.Future` objects holding
        :py:class:`xarray.DataArray` objects.
    client : object, optional
        :py:class:`dask.distributed.Client` to use in gathering
        metadata on futures. If not provided, client is inferred
        from context.

    Returns
    -------
    arrays : list
        list of :py:class:`xarray.DataArray` objects with
        :py:class:`dask.array.Array` backends.

    Examples
    --------

    Given a mapped xarray DataArray, pull the metadata into memory while
    leaving the data on the workers:

    .. code-block:: python

        >>> import numpy as np

        >>> def build_arr(multiplier):
        ...     return multiplier * xr.DataArray(
        ...         np.arange(2), dims=['x'], coords=[['a', 'b']])
        ...

        >>> client = dd.Client()
        >>> fut = client.map(build_arr, range(3))
        >>> arrs = dataarrays_from_delayed(fut)
        >>> arrs[-1]  # doctest: +ELLIPSIS
        <xarray.DataArray 'from-value-...' (x: 2)>
        dask.array<shape=(2,), dtype=int64, chunksize=(2,)>
        Coordinates:
          * x        (x) <U1 'a' 'b'

    This list of arrays can now be manipulated using normal xarray tools:

    .. code-block:: python

        >>> xr.concat(arrs, dim='simulation') # doctest: +ELLIPSIS
        <xarray.DataArray 'from-value-...' (simulation: 3, x: 2)>
        dask.array<shape=(3, 2), dtype=int64, chunksize=(1, 2)>
        Coordinates:
          * x        (x) <U1 'a' 'b'
        Dimensions without coordinates: simulation

    '''

    if client is None:
        client = dd.get_client()

    delayed_arrays = client.map(lambda x: x.data, futures)

    dask_array_metadata = client.gather(
        client.map(lambda x: (x.data.shape, x.data.dtype), futures))

    dask_arrays = [
        dask.array.from_delayed(delayed_arrays[i], *dask_array_metadata[i])
        for i in range(len(futures))]

    array_metadata = client.gather(
        client.map(
            lambda x: {'dims': x.dims, 'coords': x.coords, 'attrs': x.attrs},
            futures))

    data_arrays = [
        xr.DataArray(dask_arrays[i], **array_metadata[i])
        for i in range(len(futures))]

    return data_arrays


def dataarray_from_delayed(futures, dim=None, client=None):
    '''
    Returns a DataArray from a list of futures

    Parameters
    ----------
    futures : list
        list of :py:class:`dask.delayed.Future` objects holding
        :py:class:`xarray.DataArray` objects.
    dim : str, optional
        dimension along which to concat :py:class:`xarray.DataArray`.
        Inferred by default.
    client : object, optional
        :py:class:`dask.distributed.Client` to use in gathering
        metadata on futures. If not provided, client is inferred
        from context.

    Returns
    -------
    array : object
        :py:class:`xarray.DataArray` concatenated along ``dim`` with
        a :py:class:`dask.array.Array` backend.

    Examples
    --------

    Given a mapped xarray DataArray, pull the metadata into memory while
    leaving the data on the workers:

    .. code-block:: python

        >>> import numpy as np, pandas as pd

        >>> def build_arr(multiplier):
        ...     return multiplier * xr.DataArray(
        ...         np.arange(2), dims=['x'], coords=[['a', 'b']])
        ...

        >>> client = dd.Client()
        >>> fut = client.map(build_arr, range(3))
        >>> da = dataarray_from_delayed(
        ...     fut,
        ...     dim=pd.Index(range(3), name='simulation'))
        ...

        >>> da  # doctest: +ELLIPSIS
        <xarray.DataArray 'from-value-...' (simulation: 3, x: 2)>
        dask.array<shape=(3, 2), dtype=int64, chunksize=(1, 2)>
        Coordinates:
          * x           (x) <U1 'a' 'b'
          * simulation  (simulation) int64 0 1 2
    '''

    data_arrays = dataarrays_from_delayed(futures, client=client)
    da = xr.concat(data_arrays, dim=dim)

    return da


def datasets_from_delayed(futures, client=None):
    '''
    Returns a list of xarray datasets from a list of futures of datasets

    Parameters
    ----------
    futures : list
        list of :py:class:`dask.delayed.Future` objects holding
        :py:class:`xarray.Dataset` objects.
    client : object, optional
        :py:class:`dask.distributed.Client` to use in gathering
        metadata on futures. If not provided, client is inferred
        from context.

    Returns
    -------
    datasets : list
        list of :py:class:`xarray.Dataset` objects with
        :py:class:`dask.array.Array` backends for each variable.

    Examples
    --------

    Given a mapped :py:class:`xarray.Dataset`, pull the metadata into memory
    while leaving the data on the workers:

    .. code-block:: python

        >>> import numpy as np

        >>> def build_ds(multiplier):
        ...     return multiplier * xr.Dataset({
        ...         'var1': xr.DataArray(
        ...             np.arange(2), dims=['x'], coords=[['a', 'b']])})
        ...

        >>> client = dd.Client()
        >>> fut = client.map(build_ds, range(3))
        >>> arrs = datasets_from_delayed(fut)
        >>> arrs[-1]  # doctest: +ELLIPSIS
        <xarray.Dataset>
        Dimensions:  (x: 2)
        Coordinates:
          * x        (x) <U1 'a' 'b'
        Data variables:
            var1     (x) int64 dask.array<shape=(2,), chunksize=(2,)>

    This list of arrays can now be manipulated using normal xarray tools:

    .. code-block:: python

        >>> xr.concat(arrs, dim='y') # doctest: +ELLIPSIS
        <xarray.Dataset>
        Dimensions:  (x: 2, y: 3)
        Coordinates:
          * x        (x) <U1 'a' 'b'
        Dimensions without coordinates: y
        Data variables:
            var1     (y, x) int64 dask.array<shape=(3, 2), chunksize=(1, 2)>
    '''

    if client is None:
        client = dd.get_client()

    data_var_keys = client.gather(
        client.map(lambda x: list(x.data_vars.keys()), futures))

    delayed_arrays = [
        {
            k: (client.submit(lambda x: x[k].data, futures[i]))
            for k in data_var_keys[i]}
        for i in range(len(futures))]

    dask_array_metadata = [
        {
            k: (
                client.submit(
                    lambda x: (x[k].data.shape, x[k].data.dtype),
                    futures[i])
                .result())
            for k in data_var_keys[i]}
        for i in range(len(futures))]

    dask_data_arrays = [
        {
            k: (
                dask.array.from_delayed(
                    delayed_arrays[i][k], *dask_array_metadata[i][k]))
            for k in data_var_keys[i]}
        for i in range(len(futures))]

    array_metadata = [
        {
            k: client.submit(
                lambda x: {
                    'dims': x[k].dims,
                    'coords': x[k].coords,
                    'attrs': x[k].attrs},
                futures[i]).result()
            for k in data_var_keys[i]}
        for i in range(len(futures))]

    data_arrays = [
        {
            k: (
                xr.DataArray(dask_data_arrays[i][k], **array_metadata[i][k]))
            for k in data_var_keys[i]}
        for i in range(len(futures))]

    datasets = [xr.Dataset(arr) for arr in data_arrays]

    dataset_metadata = client.gather(
        client.map(lambda x: x.attrs, futures))

    for i in range(len(futures)):
        datasets[i].attrs.update(dataset_metadata[i])

    return datasets


def dataset_from_delayed(futures, dim=None, client=None):
    '''
    Returns an :py:class:`xarray.Dataset` from a list of futures

    Parameters
    ----------
    futures : list
        list of :py:class:`dask.delayed.Future` objects holding
        :py:class:`xarray.Dataset` objects.
    dim : str, optional
        dimension along which to concat :py:class:`xarray.Dataset`.
        Inferred by default.
    client : object, optional
        :py:class:`dask.distributed.Client` to use in gathering
        metadata on futures. If not provided, client is inferred
        from context.

    Returns
    -------
    dataset : object
        :py:class:`xarray.Dataset` concatenated along ``dim`` with
        :py:class:`dask.array.Array` backends for each variable.

    Examples
    --------

    Given a mapped :py:class:`xarray.Dataset`, pull the metadata into memory
    while leaving the data on the workers:

    .. code-block:: python

        >>> import numpy as np, pandas as pd

        >>> def build_ds(multiplier):
        ...     return multiplier * xr.Dataset({
        ...         'var1': xr.DataArray(
        ...             np.arange(2), dims=['x'], coords=[['a', 'b']])})
        ...

        >>> client = dd.Client()
        >>> fut = client.map(build_ds, range(3))
        >>> ds = dataset_from_delayed(fut, dim=pd.Index(range(3), name='y'))
        >>> ds
        <xarray.Dataset>
        Dimensions:  (x: 2, y: 3)
        Coordinates:
          * x        (x) <U1 'a' 'b'
          * y        (y) int64 0 1 2
        Data variables:
            var1     (y, x) int64 dask.array<shape=(3, 2), chunksize=(1, 2)>
    '''

    datasets = datasets_from_delayed(futures, client=client)
    ds = xr.concat(datasets, dim=dim)

    return ds
