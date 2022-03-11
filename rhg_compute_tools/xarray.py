import functools

import dask.array
import numpy as np
import xarray as xr
from dask import distributed as dd


def dataarrays_from_delayed(futures, client=None, **client_kwargs):
    """
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
    client_kwargs : optional
        kwargs to pass to ``client.map`` and ``client.gather`` commands (e.g.
        ``priority``)

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
        >>> arrs = dataarrays_from_delayed(fut, priority=1)
        >>> arrs[-1]  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        <xarray.DataArray ...(x: 2)>
        dask.array<...shape=(2,), dtype=int64, chunksize=(2,), chunktype=numpy.ndarray>
        Coordinates:
          * x        (x) <U1 'a' 'b'

    This list of arrays can now be manipulated using normal xarray tools:

    .. code-block:: python

        >>> xr.concat(arrs, dim='simulation') # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        <xarray.DataArray ...(simulation: 3, x: 2)>
        dask.array<...shape=(3, 2), dtype=int64, chunksize=(1, 2), chunktype=numpy.ndarray>
        Coordinates:
          * x        (x) <U1 'a' 'b'
        Dimensions without coordinates: simulation

        >>> client.close()
    """

    if client is None:
        client = dd.get_client()

    delayed_arrays = client.map(lambda x: x.data, futures, **client_kwargs)

    dask_array_metadata = client.gather(
        client.map(lambda x: (x.data.shape, x.data.dtype), futures, **client_kwargs)
    )

    dask_arrays = [
        dask.array.from_delayed(delayed_arrays[i], *dask_array_metadata[i])
        for i in range(len(futures))
    ]

    # using dict(x.coords) b/c gathering coords can blow up memory for some reason
    array_metadata = client.gather(
        client.map(
            lambda x: {
                "dims": x.dims,
                "coords": dict(x.coords),
                "attrs": x.attrs,
                "name": x.name,
            },
            futures,
            **client_kwargs
        )
    )

    data_arrays = [
        xr.DataArray(dask_arrays[i], **array_metadata[i]) for i in range(len(futures))
    ]

    return data_arrays


def dataarray_from_delayed(futures, dim=None, client=None, **client_kwargs):
    """
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
    client_kwargs : optional
        kwargs to pass to ``client.map`` and ``client.gather`` commands (e.g.
        ``priority``)

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
        ...     dim=pd.Index(range(3), name='simulation'),
        ...     priority=1
        ... )
        ...

        >>> da  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        <xarray.DataArray ...(simulation: 3, x: 2)>
        dask.array<...shape=(3, 2), dtype=int64, chunksize=(1, 2), chunktype=numpy.ndarray>
        Coordinates:
          * x           (x) <U1 'a' 'b'
          * simulation  (simulation) int64 0 1 2

        >>> client.close()
    """

    data_arrays = dataarrays_from_delayed(futures, client=client, **client_kwargs)
    da = xr.concat(data_arrays, dim=dim)

    return da


def datasets_from_delayed(futures, client=None, **client_kwargs):
    """
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
    client_kwargs : optional
        kwargs to pass to ``client.map`` and ``client.gather`` commands (e.g.
        ``priority``)

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
        >>> arrs = datasets_from_delayed(fut, priority=1)
        >>> arrs[-1]  # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        <xarray.Dataset>
        Dimensions:  (x: 2)
        Coordinates:
          * x        (x) <U1 'a' 'b'
        Data variables:
            var1     (x) int64 dask.array<chunksize=(2,), meta=np.ndarray>

    This list of arrays can now be manipulated using normal xarray tools:

    .. code-block:: python

        >>> xr.concat(arrs, dim='y') # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        <xarray.Dataset>
        Dimensions:  (x: 2, y: 3)
        Coordinates:
          * x        (x) <U1 'a' 'b'
        Dimensions without coordinates: y
        Data variables:
            var1     (y, x) int64 dask.array<chunksize=(1, 2), meta=np.ndarray>

        >>> client.close()
    """

    if client is None:
        client = dd.get_client()

    data_var_keys = client.gather(
        client.map(lambda x: list(x.data_vars.keys()), futures, **client_kwargs)
    )

    delayed_arrays = [
        {
            k: client.submit(lambda x: x[k].data, futures[i], client_kwargs)
            for k in data_var_keys[i]
        }
        for i in range(len(futures))
    ]

    dask_array_metadata = client.gather(
        [
            {
                k: (
                    client.submit(
                        lambda x: (x[k].data.shape, x[k].data.dtype),
                        futures[i],
                        **client_kwargs
                    )
                )
                for k in data_var_keys[i]
            }
            for i in range(len(futures))
        ]
    )

    dask_data_arrays = [
        {
            k: (
                dask.array.from_delayed(
                    delayed_arrays[i][k], *dask_array_metadata[i][k]
                )
            )
            for k in data_var_keys[i]
        }
        for i in range(len(futures))
    ]

    # using dict(x.coords) b/c gathering coords can blow up memory for some reason
    array_metadata = client.gather(
        [
            {
                k: client.submit(
                    lambda x: {
                        "dims": x[k].dims,
                        "coords": dict(x[k].coords),
                        "attrs": x[k].attrs,
                    },
                    futures[i],
                    **client_kwargs
                )
                for k in data_var_keys[i]
            }
            for i in range(len(futures))
        ]
    )

    data_arrays = [
        {
            k: (xr.DataArray(dask_data_arrays[i][k], **array_metadata[i][k]))
            for k in data_var_keys[i]
        }
        for i in range(len(futures))
    ]

    datasets = [xr.Dataset(arr) for arr in data_arrays]

    dataset_metadata = client.gather(
        client.map(lambda x: x.attrs, futures, **client_kwargs)
    )

    for i in range(len(futures)):
        datasets[i].attrs.update(dataset_metadata[i])

    return datasets


def dataset_from_delayed(futures, dim=None, client=None, **client_kwargs):
    """
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
    client_kwargs : optional
        kwargs to pass to ``client.map`` and ``client.gather`` commands (e.g.
        ``priority``)

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
        >>> ds = dataset_from_delayed(fut, dim=pd.Index(range(3), name='y'), priority=1)
        >>> ds # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        <xarray.Dataset>
        Dimensions:  (x: 2, y: 3)
        Coordinates:
          * x        (x) <U1 'a' 'b'
          * y        (y) int64 0 1 2
        Data variables:
            var1     (y, x) int64 dask.array<chunksize=(1, 2), meta=np.ndarray>

        >>> client.close()
    """

    datasets = datasets_from_delayed(futures, client=client, **client_kwargs)
    ds = xr.concat(datasets, dim=dim)

    return ds


def choose_along_axis(arr, axis=-1, replace=True, nchoices=1, p=None):
    """
    Wrapper on np.random.choice, but along a single dimension within a larger array

    Parameters
    ----------
    arr : np.array
        Array with more than one dimension. Choices will be drawn from along the
        ``axis`` dimension.
    axis : integer, optional
        Dimension along which to draw samples
    replace : bool, optional
        Whether to sample with replacement. Passed to :py:func:`np.random.choice`.
        Default 1.
    nchoices : int, optional
        Number of samples to draw. Must be less than or equal to the number of
        valid options if replace is False. Default 1.
    p : np.array
        Array with the same shape as ``arr`` with weights for each choice. Each
        dimension is sampled independently, so weights will be normalized to 1
        along the ``axis`` dimension.

    Returns
    -------
    sampled : np.array
        Array with the same shape as ``arr`` but with length ``nchoices`` along axis
        ``axis`` and with values chosen from the values of ``arr`` along dimension
        ``axis`` with weights ``p``.

    Examples
    --------

    Let's say we have an array with NaNs in it:

    .. code-block:: python

        >>> arr = np.arange(40).reshape(4, 2, 5).astype(float)
        >>> for i in range(4):
        ...     arr[i, :, i+1:] = np.nan
        >>> arr  # doctest: +NORMALIZE_WHITESPACE
        array([[[ 0., nan, nan, nan, nan],
                [ 5., nan, nan, nan, nan]],
               [[10., 11., nan, nan, nan],
                [15., 16., nan, nan, nan]],
               [[20., 21., 22., nan, nan],
                [25., 26., 27., nan, nan]],
               [[30., 31., 32., 33., nan],
                [35., 36., 37., 38., nan]]])


    We can set weights such that we only select from non-nan values

    .. code-block:: python

        >>> p = (~np.isnan(arr))
        >>> p = p / p.sum(axis=2).reshape(4, 2, 1)

    Now, sampling from this along the second dimension will draw from
    these values:

    .. code-block:: python

        >>> np.random.seed(1)
        >>> choose_along_axis(arr, 2, p=p, nchoices=10)  # doctest: +NORMALIZE_WHITESPACE
        array([[[ 0.,  0.,  0.,  0.,  0.,  0.,  0.,  0.,  0.,  0.],
                [ 5.,  5.,  5.,  5.,  5.,  5.,  5.,  5.,  5.,  5.]],
               [[11., 11., 10., 11., 11., 11., 10., 10., 10., 11.],
                [15., 15., 16., 16., 16., 15., 16., 16., 15., 16.]],
               [[22., 22., 20., 22., 20., 21., 22., 20., 20., 20.],
                [25., 27., 25., 25., 26., 25., 26., 25., 26., 27.]],
               [[30., 31., 32., 31., 30., 32., 32., 32., 33., 32.],
                [38., 35., 35., 38., 36., 35., 38., 36., 38., 37.]]])

    See Also
    --------
    :py:func:`np.random.choice` : 1-d version of this function
    """
    if p is None:
        p = np.ones_like(arr).astype(float) / arr.shape[axis]

    axis = axis % len(arr.shape)
    new_shape = tuple(list(arr.shape[:axis]) + [nchoices] + list(arr.shape[axis + 1 :]))
    result = np.ndarray(shape=new_shape, dtype=arr.dtype)

    for ind in np.ndindex(tuple([l for i, l in enumerate(arr.shape) if i != axis])):
        indexer = tuple(list(ind[:axis]) + [slice(None)] + list(ind[axis:]))
        result[indexer] = np.random.choice(
            arr[indexer],
            size=nchoices,
            replace=replace,
            p=p[indexer],
        )

    return result


def choose_along_dim(da, dim, samples=1, expand=None, new_dim_name=None):
    """
    Sample values from a DataArray along a dimension

    Wraps :py:func:`np.random.choice` to sample a different random index
    (or set of indices) from along dimension ``dim`` for each combination of
    elements along the other dimensions. This is very different from block
    resampling - to block resample along a dimension simply choose a set
    of indices and draw these from the array using :py:meth:`xr.DataArray.sel`.

    Parameters
    ----------
    da : xr.DataArray
        DataArray from which to sample values.
    dim: str
        Dimension along which to sample. Sampling will draw from elements along
        this dimension for all combinations of other dimensions.
    samples : int, optional
        Number of samples to take from the dimension ``dim``. If greater than 1,
        ``expand`` is ignored (and set to True).
    expand : bool, optional
        Whether to expand the array along the sampled dimension.
    new_dim_name : str, optoinal
        Name for the new dimension. If not provided, will use ``dim``.

    Returns
    -------
    sampled : xr.DataArray
        DataArray with sampled values chosen along dimension ``dim``

    Examples
    --------

    .. code-block:: python

        >>> da = xr.DataArray(
        ...     np.arange(40).reshape(4, 2, 5),
        ...     dims=['x', 'y', 'z'],
        ...     coords=[np.arange(4), np.arange(2), np.arange(5)],
        ... )

        >>> da # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        <xarray.DataArray (x: 4, y: 2, z: 5)>
        array([[[ 0,  1,  2,  3,  4],
                [ 5,  6,  7,  8,  9]],
               [[10, 11, 12, 13, 14],
                [15, 16, 17, 18, 19]],
               [[20, 21, 22, 23, 24],
                [25, 26, 27, 28, 29]],
               [[30, 31, 32, 33, 34],
                [35, 36, 37, 38, 39]]])
        Coordinates:
          * x        (x) int64 0 1 2 3
          * y        (y) int64 0 1
          * z        (z) int64 0 1 2 3 4

    We can take a random value along the ``'z'`` dimension:

    .. code-block:: python

        >>> np.random.seed(1)
        >>> choose_along_dim(da, 'z')
        <xarray.DataArray (x: 4, y: 2)>
        array([[ 2,  8],
               [10, 16],
               [20, 25],
               [30, 36]])
        Coordinates:
          * x        (x) int64 0 1 2 3
          * y        (y) int64 0 1


    If you provide a ``sample`` argument greater than one (or
    set expand=True) the array will be expanded to a new
    dimension:

    .. code-block:: python

        >>> np.random.seed(1)
        >>> choose_along_dim(da, 'z', samples=3) # doctest: +ELLIPSIS +NORMALIZE_WHITESPACE
        <xarray.DataArray (x: 4, y: 2, z: 3)>
        array([[[ 2,  3,  0],
                [ 6,  5,  5]],
               [[10, 11, 11],
                [17, 17, 18]],
               [[21, 24, 20],
                [28, 27, 27]],
               [[30, 30, 34],
                [39, 36, 38]]])
        Coordinates:
          * x        (x) int64 0 1 2 3
          * y        (y) int64 0 1
          * z        (z) int64 0 1 2
    """
    sampled = choose_along_axis(da.values, axis=da.get_axis_num(dim), nchoices=samples)

    if samples > 1:
        expand = True

    if not expand:
        sampled = np.take(sampled, 0, axis=da.get_axis_num(dim))
        return xr.DataArray(
            sampled,
            dims=[d for d in da.dims if d != dim],
            coords=[da.coords[d] for d in da.dims if d != dim],
        )

    else:
        if new_dim_name is None:
            new_dim_name = dim

        return xr.DataArray(
            sampled,
            dims=[d if d != dim else new_dim_name for d in da.dims],
            coords=[da.coords[d] if d != dim else np.arange(samples) for d in da.dims],
        )


@xr.register_dataarray_accessor("random")
class random:
    def __init__(self, xarray_obj):
        self._xarray_obj = xarray_obj

    @functools.wraps(choose_along_dim)
    def choice(self, *args, **kwargs):
        return choose_along_dim(self._xarray_obj, *args, **kwargs)
