# -*- coding: utf-8 -*-
"""Tools for interacting with kubernetes."""

from dask_kubernetes import KubeCluster
import dask
import dask.distributed as dd
import yaml as yml
import traceback as tb
import os
import socket
import numpy as np
from collections import Sequence


def traceback(ftr):
    return tb.print_exception(type(ftr.exception()), ftr.exception(), ftr.traceback())


def _append_docstring(func_with_docstring):
    def decorator(func):
        if func.__doc__ is None:
            func.__doc__ = ""
        if func_with_docstring.__doc__ is None:
            func.__doc__ = ""

        func.__doc__ = func.__doc__ + "\n".join(
            func_with_docstring.__doc__.lstrip().split("\n")[1:]
        )

        return func

    return decorator


def get_cluster(
    name=None,
    extra_pip_packages=None,
    extra_conda_packages=None,
    memory_gb=None,
    nthreads=None,
    cpus=None,
    cred_name=None,
    cred_path=None,
    env_items=None,
    scaling_factor=1,
    dask_config_dict={},
    deploy_mode="local",
    idle_timeout=None,
    template_path="~/worker-template.yml",
    extra_worker_labels=None,
    extra_pod_tolerations=None,
    keep_default_tolerations=True,
    **kwargs
):
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
    cred_name : str, optional
        Name of Google Cloud credentials file to use, equivalent to providing
        ``cred_path='/opt/gcsfuse_tokens/{}.json'.format(cred_name)``
    cred_path : str, optional
        Path to Google Cloud credentials file to use.
    env_items : list of dict, optional
        A list of env variable 'name'-'value' pairs to append to the env
        variables included in ``template_path``, e.g.

        .. code-block:: python

            [{
                'name': 'GOOGLE_APPLICATION_CREDENTIALS',
                'value': '/opt/gcsfuse_tokens/rhg-data.json'}])

    scaling_factor : float, optional
        scale the worker memory & CPU size using a constant multiplier of the
        specified worker. No constraints in terms of performance or cluster
        size are enforced - if you request too little the dask worker will not
        perform; if you request too much you may see an ``InsufficientMemory``
        or ``InsufficientCPU`` error on the google cloud Kubernetes console.
        Recommended scaling factors given our default ``~/worker-template.yml``
        specs are [0.5, 1, 2, 4].
    dask_config_dict : dict, optional
        Dask config parameters to modify from their defaults. A '.' is used
        to access progressive levels of the yaml structure. For instance, the
        dict could look like ``{'distributed.worker.profile.interval': '100ms'}``
    deploy_mode : str, optional
        Where to deploy the scheduler (on the same pod or a different pod)
    idle_timeout : str, optional
        Number of seconds without active ommunication with the client before the 
        remote scheduler shuts down (ignored if ``deploy_mode=='local'``).
        Default is to not shut down for this reason.
    template_path : str, optional
        Path to worker template file. Default ``~/worker-template.yml``.
    extra_worker_labels : dict, optional
        Dictionary of kubernetes labels to apply to pods. None (default) results
        in no additional labels besides those in the template, as well as
        ``jupyter_user``, which is inferred from the ``JUPYTERHUB_USER``, or, if
        not set, the server's hostname.
    extra_pod_tolerations : list of dict, optional
        List of pod toleration dictionaries. For example, to match a node pool
        NoSchedule toleration, you might provide:
        
        .. code-block:: python

            extra_pod_tolerations=[
                {"effect": "NoSchedule", "key": "k8s.dask.org_dedicated", "operator": "Equal", "value": "worker-highcpu"},
                {"effect": "NoSchedule", "key": "k8s.dask.org/dedicated", "operator": "Equal", "value": "worker-highcpu"}]

    keep_default_tolerations : bool, optional
        Whether to append (default) or replace the default tolerations. Ignored if
        ``extra_pod_tolerations`` is ``None`` or has length 0.

    Returns
    -------
    client : object
        :py:class:`dask.distributed.Client` connected to cluster
    cluster : object
        Pre-configured :py:class:`dask_kubernetes.KubeCluster`


    See Also
    --------
    :py:func:`get_micro_cluster` :
        A cluster with one-CPU workers
    :py:func:`get_standard_cluster` :
        The default cluster specification
    :py:func:`get_big_cluster` :
        A cluster with workers twice the size of the default
    :py:func:`get_giant_cluster` :
        A cluster with workers four times the size of the default

    """

    # update dask settings
    dask.config.set(dask_config_dict)

    template_path = os.path.expanduser(template_path)

    with open(template_path, "r") as f:
        template = yml.load(f, Loader=yml.SafeLoader)
    
    # update labels with default and user-provided labels
    if ('metadata' not in template) or (template.get('metadata', {}) is None):
        template['metadata'] = {}

    if ('labels' not in template['metadata']) or (template['metadata']['labels'] is None):
        template['metadata']['labels'] = {}

    labels = template['metadata']['labels']
    
    if extra_worker_labels is not None:
        labels.update(extra_worker_labels)

    labels.update({
        'jupyter_user': os.environ.get('JUPYTERHUB_USER', socket.gethostname())})
        
    template['metadata']['labels'] = labels
    
    if 'tolerations' not in template['spec']:
        template['spec']['tolerations'] = []
    
    if (extra_pod_tolerations is not None) and (len(extra_pod_tolerations) > 0):
        if keep_default_tolerations:
            template['spec']['tolerations'].extend(extra_pod_tolerations)
        else:
            template['spec']['tolerations'] = extra_pod_tolerations

    container = template["spec"]["containers"][0]

    # replace the defualt image with the new one
    if name is not None:
        container["image"] = name

    if extra_pip_packages is not None:
        container["env"].append(
            {"name": "EXTRA_PIP_PACKAGES", "value": extra_pip_packages}
        )

    if extra_conda_packages is not None:
        container["env"].append(
            {"name": "EXTRA_CONDA_PACKAGES", "value": extra_conda_packages}
        )

    if cred_path is not None:
        container["env"].append(
            {"name": "GOOGLE_APPLICATION_CREDENTIALS", "value": cred_path}
        )

    elif cred_name is not None:
        container["env"].append(
            {
                "name": "GOOGLE_APPLICATION_CREDENTIALS",
                "value": "/opt/gcsfuse_tokens/{}.json".format(cred_name),
            }
        )

    if env_items is not None:
        if isinstance(env_items, dict):
            [
                container["env"].append({"name": k, "value": v})
                for k, v in env_items.values()
            ]
        # allow deprecated passing of list of name/value pairs
        elif isintance(env_items, Sequence):
            warnings.warn(
                "Passing of list of name/value pairs deprecated. "
                "Please pass a dictionary instead."
            )
            container["env"] = container["env"] + env_items
        else:
            raise ValueError("Expected `env_items` of type dict or sequence.")

    # adjust worker creation args
    args = container["args"]

    # set nthreads if provided
    nthreads_ix = args.index("--nthreads") + 1
    if nthreads is not None:
        args[nthreads_ix] = str(nthreads)
    nthreads = int(args[nthreads_ix])

    # then in resources
    resources = container["resources"]
    limits = resources["limits"]
    requests = resources["requests"]

    msg = "{} limits and requests do not match"

    if memory_gb is None:
        memory_gb = float(limits["memory"].strip("G"))
        mem_request = float(requests["memory"].strip("G"))
        assert memory_gb == mem_request, msg.format("memory")

    if cpus is None:
        cpus = float(limits["cpu"])
        cpu_request = float(requests["cpu"])
        assert cpus == cpu_request, msg.format("cpu")

    # now properly set the threads accessible by multi-threaded libraries
    # so that there's no competition between dask threads and the threads of
    # these libraries
    cpus_rounded = np.round(cpus)
    lib_threads = int(cpus_rounded / nthreads)
    for lib in ["OMP_NUM_THREADS", "MKL_NUM_THREADS", "OPENBLAS_NUM_THREADS"]:
        container["env"].append({"name": lib, "value": lib_threads})
    format_request = lambda x: "{:04.2f}".format(np.floor(x * 100) / 100)

    # set memory-limit if provided
    mem_ix = args.index("--memory-limit") + 1
    args[mem_ix] = format_request(float(memory_gb) * scaling_factor) + "GB"

    limits["memory"] = format_request(float(memory_gb) * scaling_factor) + "G"

    requests["memory"] = format_request(float(memory_gb) * scaling_factor) + "G"

    limits["cpu"] = format_request(float(cpus) * scaling_factor)
    requests["cpu"] = format_request(float(cpus) * scaling_factor)

    # start cluster and client and return
    # need more time to connect to remote scheduler
    if deploy_mode == "remote":
        dask.config.set({"kubernetes.idle-timeout": idle_timeout})
    cluster = KubeCluster.from_dict(
        template, deploy_mode=deploy_mode, idle_timeout=None
    )

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

    return get_cluster(*args, scaling_factor=(0.97 / 1.75), **kwargs)
