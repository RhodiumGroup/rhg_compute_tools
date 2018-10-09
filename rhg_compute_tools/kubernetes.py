# -*- coding: utf-8 -*-

"""Tools for interacting with kubernetes."""
import dask_kubernetes as dk
import dask.distributed as dd
import yaml as yml
import traceback as tb
import os
import numpy as np


def traceback(ftr):
    """Return a full stacktrace of an exception that occured on a worker

    Parameters
    __________
    ftr : :py:class:`dask.distributed.Future`

    Returns
    _______
    str : Traceback
    """
    return tb.print_exception(
        type(ftr.exception()),
        ftr.exception(),
        ftr.traceback())


def _append_docstring(func_with_docstring):
    def decorator(func):
        if func.__doc__ is None:
            func.__doc__ = ''
        if func_with_docstring.__doc__ is None:
            func.__doc__ = ''

        func.__doc__ = (
            func.__doc__
            + '\n'.join(func_with_docstring.__doc__.lstrip().split('\n')[1:]))

        return func
    return decorator


def get_cluster(
        name=None,
        extra_pip_packages=None,
        extra_conda_packages=None,
        memory_gb=None,
        nthreads=None,
        cpus=None,
        cred_path=None,
        env_items=None,
        scaling_factor=1,
        template_path='~/worker-template.yml'):
    """
    Start dask.kubernetes cluster and dask.distributed client

    All arguments are optional. If not provided, arguments will default to
    values provided in ``template_path``.

    Parameters
    ----------
    name : str, optional
        Name of worker image to use. If None, default to worker specified in
        ``template_path``.
    extra_pip_packages : str, optional
        Extra pip packages to install on worker. Packages are installed
        using ``pip install extra_pip_packages``.
    extra_conda_packages :str, optional
        Extra conda packages to install on worker. Default channel is
        ``conda-forge``. Packages are installed using
        ``conda install -y -c conda-forge ${EXTRA_CONDA_PACKAGES}``.
    memory_gb : float, optional
        Memory to assign per 'group of workers', where a group consists of
        nthreads independent workers.
    nthreads : int, optional
        Number of independent threads per group of workers. Not sure if this
        should ever be set to something other than 1.
    cpus : float, optional
        Number of virtual CPUs to assign per 'group of workers'
    cred_path : str, optional
        Path to Google Cloud credentials file to use.
    env_items : list of dict, optional
        A list of env variable 'name'-'value' pairs to append to the env
        variables included in ``template_path``, e.g.

        .. code-block:: python

            [{
                'name': 'GCLOUD_DEFAULT_TOKEN_FILE',
                'value': '/opt/gcsfuse_tokens/rhg-data.json'}])

    scaling_factor: float, optional
        scale the worker memory & CPU size using a constant multiplier of the
        specified worker. No constraints in terms of performance or cluster
        size are enforced - if you request too little the dask worker will not
        perform; if you request too much you may see an ``InsufficientMemory``
        or ``InsufficientCPU`` error on the google cloud Kubernetes console.

        Recommended scaling factors given our default ``~/worker-template.yml``
        specs are [0.5, 1, 2, 4].
    template_path : str, optional
        Path to worker template file. Default ``~/worker-template.yml``.

    Returns
    -------
    client : object
        :py:class:`dask.distributed.Client` connected to cluster
    cluster : object
        Pre-configured :py:class:`dask_kubernetes.KubeCluster`

    See Also
    --------
    :py:func:`get_micro_cluster`, :py:func:`get_standard_cluster`,
    :py:func:`get_big_cluster`, :py:func:`get_giant_cluster`
    """

    template_path = os.path.expanduser(template_path)

    with open(template_path, 'r') as f:
        template = yml.load(f)

    container = template['spec']['containers'][0]

    # replace the defualt image with the new one
    if name is not None:
        container['image'] = name

    if extra_pip_packages is not None:
        container['env'].append({
            'name': 'EXTRA_PIP_PACKAGES',
            'value': extra_pip_packages})

    if extra_conda_packages is not None:
        container['env'].append({
            'name': 'EXTRA_CONDA_PACKAGES',
            'value': extra_conda_packages})

    if cred_path is not None:
        container['env'].append({
            'name': 'GCLOUD_DEFAULT_TOKEN_FILE',
            'value': cred_path})

    if env_items is not None:
        container['env'] = container['env'] + env_items

    # adjust worker creation args
    args = container['args']

    # set nthreads if provided
    nthreads_ix = args.index('--nthreads') + 1

    if nthreads is not None:
        args[nthreads_ix] = str(nthreads)

    # then in resources
    resources = container['resources']
    limits = resources['limits']
    requests = resources['requests']

    msg = '{} limits and requests do not match'

    if memory_gb is None:
        memory_gb = float(limits['memory'].strip('G'))
        mem_request = float(requests['memory'].strip('G'))
        assert memory_gb == mem_request, msg.format('memory')

    if cpus is None:
        cpus = float(limits['cpu'])
        cpu_request = float(requests['cpu'])
        assert cpus == cpu_request, msg.format('cpu')

    format_request = lambda x: '{:04.2f}'.format(np.floor(x * 100) / 100)

    # set memory-limit if provided
    mem_ix = args.index('--memory-limit') + 1
    args[mem_ix] = (
        format_request(float(memory_gb) * scaling_factor) + 'GB')

    limits['memory'] = (
        format_request(float(memory_gb) * scaling_factor) + 'GB')

    requests['memory'] = (
        format_request(float(memory_gb) * scaling_factor) + 'GB')

    limits['cpu'] = format_request(float(cpus) * scaling_factor)
    requests['cpu'] = format_request(float(cpus) * scaling_factor)

    # start cluster and client and return
    cluster = dk.KubeCluster.from_dict(template)

    client = dd.Client(cluster)

    return client, cluster


@_append_docstring(get_cluster)
def get_giant_cluster(*args, **kwargs):
    """
    Start a cluster with 4x the memory and CPU per worker relative to default
    """

    return get_cluster(*args, scaling_factor=4, **kwargs)


@_append_docstring(get_cluster)
def get_big_cluster(*args, **kwargs):
    """
    Start a cluster with 2x the memory and CPU per worker relative to default
    """

    return get_cluster(*args, scaling_factor=2, **kwargs)


@_append_docstring(get_cluster)
def get_standard_cluster(*args, **kwargs):
    """
    Start a cluster with 1x the memory and CPU per worker relative to default
    """

    return get_cluster(*args, scaling_factor=1, **kwargs)


@_append_docstring(get_cluster)
def get_micro_cluster(*args, **kwargs):
    """
    Start a cluster with a single CPU per worker
    """

    return get_cluster(*args, scaling_factor=(4 / 7), **kwargs)
