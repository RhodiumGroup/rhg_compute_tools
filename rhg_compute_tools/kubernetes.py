# -*- coding: utf-8 -*-

"""Tools for interacting with kubernetes."""
import dask_kubernetes as dk
import dask.distributed as dd
import yaml as yml
import traceback as tb


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

def get_worker(
        name=None,
        extra_pip_packages=None,
        extra_conda_packages=None,
        memory_gb=None,
        nthreads=None,
        cpus=None,
        cred_path=None,
        env_items=None):
    """
    Start dask.kubernetes cluster and dask.distributed client

    Parameters
    ----------
    name : str, optional
        Name of worker image to use. If None, default to worker specified in
        `/home/jovyan/worker-template.yml`.
    extra_pip_packages : str, optional
        Extra pip packages to install on worker. Syntax to install is
        "pip install `extra_pip_packages`"
    extra_conda_packages :str, optional
        Same as above except for conda
    memory_gb : float, optional
        Memory to assign per 'group of workers', where a group consists of
        nthreads independent workers.
    nthreads : int, optional
        Number of independent threads per group of workers. Not sure if this
        should ever be set to something other than 1.
    cpus : float, optional
        Number of virtual CPUs to assign per 'group of workers'
    cred_path : str or None, optional
        Path to Google Cloud credentials file to use.
    env_items : list of dict, optional
        A list of env variable 'name'-'value' pairs to append to the env variables
        included in worker-template.yml. (e.g. [{'name': 'GCLOUD_DEFAULT_TOKEN_FILE',
        'value': '/opt/gcsfuse_tokens/rhg-data.json'}])

    Returns
    -------
    client : :py:class:dask.distributed.Client
    cluster : :py:class:dask_kubernetes.KubeCluster
    """

    with open('/home/jovyan/worker-template.yml', 'r') as f:
        template = yml.load(f)

    container = template['spec']['containers'][0]

    # replace the defualt image with the new one
    if name is not None:
        container['image'] = name

    if extra_pip_packages is not None:
        container['env'].append({'name':'EXTRA_PIP_PACKAGES',
                                        'value':extra_pip_packages})
    if extra_conda_packages is not None:
        container['env'].append({'name':'EXTRA_CONDA_PACKAGES',
                                        'value':extra_conda_packages})
    if cred_path is not None:
        container['env'].append({'name': 'GCLOUD_DEFAULT_TOKEN_FILE',
                                 'value': cred_path})
    if env_items is not None:
        container['env'] = container['env'] + env_items

    # adjust worker creation args
    args = container['args']

    # set nthreads if provided
    nthreads_ix = args.index('--nthreads') + 1
    if nthreads is not None:
        args[nthreads_ix] = str(nthreads)

    # set memory-limit if provided
    mem_ix = args.index('--memory-limit') + 1
    if memory_gb is not None:
        args[mem_ix] = '{}GB'.format(memory_gb)

    # then in resources
    resources = container['resources']
    limits = resources['limits']
    requests = resources['requests']

    if memory_gb is not None:
        limits['memory'] = '{}G'.format(memory_gb)
        requests['memory'] = '{}G'.format(memory_gb)

    if cpus is not None:
        limits['cpu'] = str(cpus)
        requests['cpu'] = str(cpus)

    # start cluster and client and return
    cluster = dk.KubeCluster.from_dict(template)

    client = dd.Client(cluster)

    return client, cluster
