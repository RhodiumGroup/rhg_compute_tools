# -*- coding: utf-8 -*-
"""Tools for interacting with kubernetes."""

import os
import socket
import traceback as tb
import warnings
from collections.abc import Sequence
from pathlib import Path

import dask
import numpy as np
import yaml as yml
from dask import distributed as dd

# is dask-gateway available
GATEWAY = False
try:
    import dask_gateway

    GATEWAY = True
except ModuleNotFoundError:
    from dask_kubernetes import KubeCluster


def traceback(ftr):
    return tb.print_exception(type(ftr.exception()), ftr.exception(), ftr.traceback())


def _append_docstring():
    func_with_docstring = _get_cluster_dask_kubernetes
    if GATEWAY:
        func_with_docstring = _get_cluster_dask_gateway

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


def _get_cluster_dask_gateway(**kwargs):
    """
    Start dask.kubernetes cluster and dask.distributed client

    All arguments are optional. If not provided, defaults will be used. To view
    defaults, instantiate a :class:`dask_gateway.Gateway` object and call
    `gateway.cluster_options()`.

    Parameters
    ----------
    name : str, optional
        Name of worker image to use (e.g. ``rhodium/worker:latest``). If ``None``
        (default), default to worker specified in ``template_path``.
    tag : str, optional
        Tag of the worker image to use. Cannot be used in combination with
        ``name``, which should include a tag. If provided, overrides the
        tag of the image specified in ``template_path``. If ``None``
        (default), the full image specified in ``name`` or ``template_path``
        is used.
    extra_pip_packages : str, optional
        Extra pip packages to install on worker. Packages are installed
        using ``pip install extra_pip_packages``.
    profile : One of ["micro", "standard", "big", "giant"]
        Determines size of worker. CPUs assigned are slightly under 1, 2, 4, and 8,
        respectively. Memory assigned is slightly over 6, 12, 24, and 48 GB,
        respectively.
    cpus : float, optional
        Set the CPUs requested for your workers as defined by ``profile``. Will
        raise error if >7.5, because our 8-CPU nodes need ~.5 vCPU for kubernetes pods.
        (NOTE 12/15/20: This is currently set to 1 by default to allow for mapping
        big workflows across inputs, see
        https://github.com/dask/dask-gateway/issues/364).
    cred_name : str, optional
        Name of Google Cloud credentials file to use, equivalent to providing
        ``cred_path='/opt/gcsfuse_tokens/{}.json'.format(cred_name)``. May not use
        if ``cred_path`` is specified.
    cred_path : str, optional
        Path to Google Cloud credentials file to use. May not use if ``cred_name`` is
        specified.
    env_items : dict, optional
        A dictionary of env variable 'name'-'value' pairs to append to the env
        variables included in ``template_path``, e.g.

        .. code-block:: python

            {
                'MY_ENV_VAR': 'some string',
            }

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
                {
                    "effect": "NoSchedule",
                    "key": "k8s.dask.org_dedicated",
                    "operator": "Equal",
                    "value": "worker-highcpu"
                },
                {
                    "effect": "NoSchedule",
                    "key": "k8s.dask.org/dedicated",
                    "operator": "Equal",
                    "value": "worker-highcpu"
                }
            ]

    keep_default_tolerations : bool, optional
        Whether to append (default) or replace the default tolerations. Ignored if
        ``extra_pod_tolerations`` is ``None`` or has length 0.

    Returns
    -------
    client : object
        :py:class:`dask.distributed.Client` connected to cluster
    cluster : object
        Pre-configured :py:class:`dask_gateway.GatewayCluster`


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

    gateway = dask_gateway.Gateway()
    default_options = gateway.cluster_options()

    new_kwargs = kwargs.copy()

    if new_kwargs.get("cpus", 0) > 7.25:
        raise ValueError("Must specify ``cpus`` <= 7.25")

    # handle naming changes
    for k, v in kwargs.items():
        if k == "name":
            new_kwargs["worker_image"] = kwargs["name"]
            del new_kwargs["name"]
        elif k == "cred_path":
            if "cred_name" not in kwargs.keys():
                new_kwargs["cred_name"] = Path(v).stem
            del new_kwargs["cred_path"]
        elif k == "extra_pod_tolerations":
            if (
                "keep_default_tolerations" in kwargs.keys()
                and kwargs["keep_default_tolerations"] == False
            ):
                base_tols = {}
            else:
                base_tols = default_options.worker_tolerations
            new_kwargs.pop("keep_default_tolerations", None)
            new_kwargs["worker_tolerations"] = {
                **base_tols,
                **{
                    f"user_{key}": val
                    for key, val in enumerate(new_kwargs.pop("extra_pod_tolerations"))
                },
            }
        elif k not in list(default_options.keys()) + ["tag"]:
            raise KeyError(f"{k} not allowed as a kwarg when using dask-gateway")

    if "worker_image" in new_kwargs and "tag" in new_kwargs:
        raise ValueError("provide either `name` or `tag`, not both")

    if "tag" in new_kwargs:
        img, _ = default_options.worker_image.split(":")
        new_kwargs["worker_image"] = ":".join(img, new_kwargs["tag"])

    cluster = gateway.new_cluster(**new_kwargs)
    client = cluster.get_client()

    return client, cluster


def _get_cluster_dask_kubernetes(
    name=None,
    tag=None,
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
    **kwargs,
):
    """

    **DEPRECATED (12/15/2020) **: Since we no longer maintain clusters using
    dask-kubernetes schedulers. Only dask-gateway is now supported.

    Start dask.kubernetes cluster and dask.distributed client

    All arguments are optional. If not provided, arguments will default to
    values provided in ``template_path``.

    Parameters
    ----------
    name : str, optional
        Name of worker image to use (e.g. ``rhodium/worker:latest``). If ``None``
        (default), default to worker specified in ``template_path``.
    tag : str, optional
        Tag of the worker image to use. Cannot be used in combination with
        ``name``, which should include a tag. If provided, overrides the
        tag of the image specified in ``template_path``. If ``None``
        (default), the full image specified in ``name`` or ``template_path``
        is used.
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
    env_items : dict, optional
        A dictionary of env variable 'name'-'value' pairs to append to the env
        variables included in ``template_path``, e.g.

        .. code-block:: python

            {
                'GOOGLE_APPLICATION_CREDENTIALS': '/opt/gcsfuse_tokens/rhg-data.json',
            }

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
        Number of seconds without active communication with the client before the
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
                {
                    "effect": "NoSchedule",
                    "key": "k8s.dask.org_dedicated",
                    "operator": "Equal",
                    "value": "worker-highcpu"
                },
                {
                    "effect": "NoSchedule",
                    "key": "k8s.dask.org/dedicated",
                    "operator": "Equal",
                    "value": "worker-highcpu"
                }
            ]

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

    if (name is not None) and (tag is not None):
        raise ValueError("provide either `name` or `tag`, not both")

    # update dask settings
    dask.config.set(dask_config_dict)

    template_path = os.path.expanduser(template_path)

    with open(template_path, "r") as f:
        template = yml.load(f, Loader=yml.SafeLoader)

    # update labels with default and user-provided labels
    if ("metadata" not in template) or (template.get("metadata", {}) is None):
        template["metadata"] = {}

    if ("labels" not in template["metadata"]) or (
        template["metadata"]["labels"] is None
    ):
        template["metadata"]["labels"] = {}

    labels = template["metadata"]["labels"]

    if extra_worker_labels is not None:
        labels.update(extra_worker_labels)

    labels.update(
        {"jupyter_user": os.environ.get("JUPYTERHUB_USER", socket.gethostname())}
    )

    template["metadata"]["labels"] = labels

    if "tolerations" not in template["spec"]:
        template["spec"]["tolerations"] = []

    if (extra_pod_tolerations is not None) and (len(extra_pod_tolerations) > 0):
        if keep_default_tolerations:
            template["spec"]["tolerations"].extend(extra_pod_tolerations)
        else:
            template["spec"]["tolerations"] = extra_pod_tolerations

    container = template["spec"]["containers"][0]

    # replace the defualt image with the new one
    if name is not None:
        container["image"] = name
    if tag is not None:
        img, _ = container["image"].split(":")
        container["image"] = ":".join(img, tag)

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
                for k, v in env_items.items()
            ]
        # allow deprecated passing of list of name/value pairs
        elif isinstance(env_items, Sequence):
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

    def format_request(x):
        return "{:04.2f}".format(np.floor(x * 100) / 100)

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


@_append_docstring()
def get_cluster(*args, **kwargs):
    if GATEWAY:
        if len(args) > 0:
            raise ValueError(
                "Positional args not allowed when using dask-gateway. If you are "
                "trying to pass worker name, use the ``name`` kwarg."
            )
        return _get_cluster_dask_gateway(**kwargs)
    return _get_cluster_dask_kubernetes(*args, **kwargs)


@_append_docstring()
def get_giant_cluster(*args, **kwargs):
    """
    Start a cluster with 4x the memory and CPU per worker relative to default
    """

    if GATEWAY:
        if len(args) > 0:
            raise ValueError(
                "Positional args not allowed when using dask-gateway. If you are "
                "trying to pass worker name, use the ``name`` kwarg."
            )
        return _get_cluster_dask_gateway(profile="giant", **kwargs)
    return _get_cluster_dask_kubernetes(*args, scaling_factor=4, **kwargs)


@_append_docstring()
def get_big_cluster(*args, **kwargs):
    """
    Start a cluster with 2x the memory and CPU per worker relative to default
    """
    if GATEWAY:
        if len(args) > 0:
            raise ValueError(
                "Positional args not allowed when using dask-gateway. If you are "
                "trying to pass worker name, use the ``name`` kwarg."
            )
        return _get_cluster_dask_gateway(profile="big", **kwargs)
    return _get_cluster_dask_kubernetes(*args, scaling_factor=2, **kwargs)


@_append_docstring()
def get_standard_cluster(*args, **kwargs):
    """
    Start a cluster with 1x the memory and CPU per worker relative to default
    """
    if GATEWAY:
        if len(args) > 0:
            raise ValueError(
                "Positional args not allowed when using dask-gateway. If you are "
                "trying to pass worker name, use the ``name`` kwarg."
            )
        return _get_cluster_dask_gateway(profile="standard", **kwargs)
    return _get_cluster_dask_kubernetes(*args, scaling_factor=1, **kwargs)


@_append_docstring()
def get_micro_cluster(*args, **kwargs):
    """
    Start a cluster with a single CPU per worker
    """
    if GATEWAY:
        if len(args) > 0:
            raise ValueError(
                "Positional args not allowed when using dask-gateway. If you are "
                "trying to pass worker name, use the ``name`` kwarg."
            )
        return _get_cluster_dask_gateway(profile="micro", **kwargs)
    return _get_cluster_dask_kubernetes(*args, scaling_factor=(0.97 / 1.75), **kwargs)
