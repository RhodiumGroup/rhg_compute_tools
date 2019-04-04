# -*- coding: utf-8 -*-

import dask
import dask.distributed

from distributed.deploy.adaptive import Adaptive
from distributed.deploy.cluster import Cluster
from distributed.deploy.local import LocalCluster
from dask_kubernetes import KubeCluster
from toolz import valmap, second, compose, groupby
from distributed.utils import log_errors, PeriodicCallback, parse_timedelta

import atexit
from datetime import timedelta
import logging
import math
from time import sleep
import weakref
import toolz

from tornado import gen

from distributed.core import CommClosedError
from distributed.utils import (sync, ignoring, All, silence_logging, LoopRunner,
        log_errors, thread_state)
from distributed.nanny import Nanny
from distributed.scheduler import Scheduler
from distributed.worker import Worker, _ncores

import logging


logger = logging.getLogger('distributed.deploy.adaptive')
logger.setLevel(logging.DEBUG)


class MyAdaptive(Adaptive):
    def needs_cpu(self):
        total_occupancy = self.scheduler.total_occupancy
        total_cores = sum([ws.ncores for ws in self.scheduler.workers.values()])

        if total_occupancy / (total_cores + 1e-9) > self.startup_cost * 2:
            logger.info("CPU limit exceeded [%d occupancy / %d cores]",
                        total_occupancy, total_cores)

            num_workers = len(self.scheduler.workers)

            tasks_processing = 0

            for w in self.scheduler.workers.values():
                tasks_processing += len(w.processing)

                if tasks_processing > num_workers:
                    logger.info(
                        "pending tasks exceed number of workers "
                        "[%d tasks / %d workers]",
                        tasks_processing, num_workers)

                    return True

        return False


class MyCluster(Cluster):
    def adapt(self, **kwargs):
        with ignoring(AttributeError):
            self._adaptive.stop()
        if not hasattr(self, '_adaptive_options'):
            self._adaptive_options = {}
        self._adaptive_options.update(kwargs)
        self._adaptive = MyAdaptive(self.scheduler, self, **self._adaptive_options)
        return self._adaptive


class MyLocalCluster(MyCluster, LocalCluster):
    pass


'''
subclass dask_kubernetes.KubeCluster
'''

import getpass
import logging
import os
import socket
import string
import time
from urllib.parse import urlparse
import uuid
from weakref import finalize

try:
    import yaml
except ImportError:
    yaml = False

import dask
from distributed.deploy import LocalCluster, Cluster
from distributed.comm.utils import offload
import kubernetes
from tornado import gen

from dask_kubernetes.objects import make_pod_from_dict, clean_pod_template


class MyKubeCluster(MyCluster, KubeCluster):
    def __init__(
            self,
            pod_template=None,
            name=None,
            namespace=None,
            n_workers=None,
            host=None,
            port=None,
            env=None,
            **kwargs
    ):
        name = name or dask.config.get('kubernetes.name')
        namespace = namespace or dask.config.get('kubernetes.namespace')
        n_workers = n_workers if n_workers is not None else dask.config.get('kubernetes.count.start')
        host = host or dask.config.get('kubernetes.host')
        port = port if port is not None else dask.config.get('kubernetes.port')
        env = env if env is not None else dask.config.get('kubernetes.env')

        if not pod_template and dask.config.get('kubernetes.worker-template', None):
            d = dask.config.get('kubernetes.worker-template')
            d = dask.config.expand_environment_variables(d)
            pod_template = make_pod_from_dict(d)

        if not pod_template and dask.config.get('kubernetes.worker-template-path', None):
            import yaml
            fn = dask.config.get('kubernetes.worker-template-path')
            fn = fn.format(**os.environ)
            with open(fn) as f:
                d = yaml.safe_load(f)
            d = dask.config.expand_environment_variables(d)
            pod_template = make_pod_from_dict(d)

        if not pod_template:
            msg = ("Worker pod specification not provided. See KubeCluster "
                   "docstring for ways to specify workers")
            raise ValueError(msg)

        self.cluster = MyLocalCluster(ip=host or socket.gethostname(),
                                    scheduler_port=port,
                                    n_workers=0, **kwargs)
        try:
            kubernetes.config.load_incluster_config()
        except kubernetes.config.ConfigException:
            kubernetes.config.load_kube_config()

        self.core_api = kubernetes.client.CoreV1Api()

        if namespace is None:
            namespace = _namespace_default()

        name = name.format(user=getpass.getuser(),
                           uuid=str(uuid.uuid4())[:10],
                           **os.environ)
        name = escape(name)

        self.pod_template = clean_pod_template(pod_template)
        # Default labels that can't be overwritten
        self.pod_template.metadata.labels['dask.org/cluster-name'] = name
        self.pod_template.metadata.labels['user'] = escape(getpass.getuser())
        self.pod_template.metadata.labels['app'] = 'dask'
        self.pod_template.metadata.labels['component'] = 'dask-worker'
        self.pod_template.metadata.namespace = namespace

        self.pod_template.spec.containers[0].env.append(
            kubernetes.client.V1EnvVar(name='DASK_SCHEDULER_ADDRESS',
                                       value=self.scheduler_address)
        )
        if env:
            self.pod_template.spec.containers[0].env.extend([
                kubernetes.client.V1EnvVar(name=k, value=str(v))
                for k, v in env.items()
            ])
        self.pod_template.metadata.generate_name = name

        finalize(self, _cleanup_pods, self.namespace, self.pod_template.metadata.labels)

        if n_workers:
            self.scale(n_workers)

def _cleanup_pods(namespace, labels):
    """ Remove all pods with these labels in this namespace """
    api = kubernetes.client.CoreV1Api()
    pods = api.list_namespaced_pod(namespace, label_selector=format_labels(labels))
    for pod in pods.items:
        try:
            api.delete_namespaced_pod(pod.metadata.name, namespace,
                                      kubernetes.client.V1DeleteOptions())
            logger.info('Deleted pod: %s', pod.metadata.name)
        except kubernetes.client.rest.ApiException as e:
            # ignore error if pod is already removed
            if e.status != 404:
                raise

def _namespace_default():
    ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().strip()
    return 'default'


valid_characters = string.ascii_letters + string.digits + '_-.'


def escape(s):
    return ''.join(c for c in s if c in valid_characters)



"""Tools for interacting with kubernetes."""
import dask.distributed as dd
import yaml as yml
import traceback as tb
import os
import numpy as np


def traceback(ftr):
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
        dask_config_dict={},
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
    dask_config_dict: dict, optional
        Dask config parameters to modify from their defaults. A '.' is used
        to access progressive levels of the yaml structure. For instance, the
        dict could look like {'distributed.worker.profile.interval':'100ms'}
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
    :py:func:`get_micro_cluster` :
        A cluster with one-CPU workers
    :py:func:`get_standard_cluster` :
        The default cluster specification
    :py:func:`get_big_cluster` :
        A cluster with workers twice the size of the default
    :py:func:`get_giant_cluster` :
        A cluster with workers four times the size of the default

    """

    ## update dask settings
    dask.config.set(dask_config_dict)


    template_path = os.path.expanduser(template_path)

    with open(template_path, 'r') as f:
        template = yml.load(f, Loader=yml.SafeLoader)

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
        format_request(float(memory_gb) * scaling_factor) + 'G')

    requests['memory'] = (
        format_request(float(memory_gb) * scaling_factor) + 'G')

    limits['cpu'] = format_request(float(cpus) * scaling_factor)
    requests['cpu'] = format_request(float(cpus) * scaling_factor)

    # start cluster and client and return
    cluster = MyKubeCluster.from_dict(template)

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
