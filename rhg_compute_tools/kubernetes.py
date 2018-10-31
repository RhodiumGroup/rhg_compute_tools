# -*- coding: utf-8 -*-

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
        """
        Check if the cluster is CPU constrained (too many tasks per core)
        Notes
        -----
        Returns ``True`` if the occupancy per core is some factor larger
        than ``startup_cost``.
        """
        total_occupancy = self.scheduler.total_occupancy
        total_cores = sum([ws.ncores for ws in self.scheduler.workers.values()])

        if total_occupancy / (total_cores + 1e-9) > self.startup_cost * 2:
            logger.info("CPU limit exceeded [%d occupancy / %d cores]",
                        total_occupancy, total_cores)


            tasks_processing = sum((len(w.processing) for w in self.scheduler.workers.values()))
            num_workers = len(self.scheduler.workers)

            if tasks_processing > num_workers:
                return True

        return False

    def should_scale_up(self):
        """
        Determine whether additional workers should be added to the cluster
        Returns
        -------
        scale_up : bool
        Notes
        ----
        Additional workers are added whenever
        1. There are unrunnable tasks and no workers
        2. The cluster is CPU constrained
        3. The cluster is RAM constrained
        4. There are fewer workers than our minimum
        See Also
        --------
        needs_cpu
        needs_memory
        """
        with log_errors():
            if len(self.scheduler.workers) < self.minimum:
                return True

            if self.maximum is not None and len(self.scheduler.workers) >= self.maximum:
                return False

            if self.scheduler.unrunnable and not self.scheduler.workers:
                return True

            needs_cpu = self.needs_cpu()
            needs_memory = self.needs_memory()

            if needs_cpu or needs_memory:
                return True

            return False

class MyCluster(Cluster):
    def adapt(self, **kwargs):
        """ Turn on adaptivity
        For keyword arguments see dask.distributed.Adaptive
        Examples
        --------
        >>> cluster.adapt(minimum=0, maximum=10, interval='500ms')
        """
        with ignoring(AttributeError):
            self._adaptive.stop()
        if not hasattr(self, '_adaptive_options'):
            self._adaptive_options = {}
        self._adaptive_options.update(kwargs)
        self._adaptive = MyAdaptive(self.scheduler, self, **self._adaptive_options)
        return self._adaptive
    
class MyLocalCluster(MyCluster):
    """ Create local Scheduler and Workers
    This creates a "cluster" of a scheduler and workers running on the local
    machine.
    Parameters
    ----------
    n_workers: int
        Number of workers to start
    processes: bool
        Whether to use processes (True) or threads (False).  Defaults to True
    threads_per_worker: int
        Number of threads per each worker
    scheduler_port: int
        Port of the scheduler.  8786 by default, use 0 to choose a random port
    silence_logs: logging level
        Level of logs to print out to stdout.  ``logging.WARN`` by default.
        Use a falsey value like False or None for no change.
    ip: string
        IP address on which the scheduler will listen, defaults to only localhost
    diagnostics_port: int
        Port on which the :doc:`web` will be provided.  8787 by default, use 0
        to choose a random port, ``None`` to disable it, or an
        :samp:`({ip}:{port})` tuple to listen on a different IP address than
        the scheduler.
    asynchronous: bool (False by default)
        Set to True if using this cluster within async/await functions or within
        Tornado gen.coroutines.  This should remain False for normal use.
    kwargs: dict
        Extra worker arguments, will be passed to the Worker constructor.
    service_kwargs: Dict[str, Dict]
        Extra keywords to hand to the running services
    security : Security
    Examples
    --------
    >>> c = LocalCluster()  # Create a local cluster with as many workers as cores  # doctest: +SKIP
    >>> c  # doctest: +SKIP
    LocalCluster("127.0.0.1:8786", workers=8, ncores=8)
    >>> c = Client(c)  # connect to local cluster  # doctest: +SKIP
    Add a new worker to the cluster
    >>> w = c.start_worker(ncores=2)  # doctest: +SKIP
    Shut down the extra worker
    >>> c.stop_worker(w)  # doctest: +SKIP
    Pass extra keyword arguments to Bokeh
    >>> LocalCluster(service_kwargs={'bokeh': {'prefix': '/foo'}})  # doctest: +SKIP
    """
    def __init__(self, n_workers=None, threads_per_worker=None, processes=True,
                 loop=None, start=None, ip=None, scheduler_port=0,
                 silence_logs=logging.WARN, diagnostics_port=8787,
                 services=None, worker_services=None, service_kwargs=None,
                 asynchronous=False, security=None, **worker_kwargs):
        if start is not None:
            msg = ("The start= parameter is deprecated. "
                   "LocalCluster always starts. "
                   "For asynchronous operation use the following: \n\n"
                   "  cluster = yield LocalCluster(asynchronous=True)")
            raise ValueError(msg)

        self.status = None
        self.processes = processes
        self.silence_logs = silence_logs
        self._asynchronous = asynchronous
        self.security = security
        services = services or {}
        worker_services = worker_services or {}
        if silence_logs:
            self._old_logging_level = silence_logging(level=silence_logs)
        if n_workers is None and threads_per_worker is None:
            if processes:
                n_workers = _ncores
                threads_per_worker = 1
            else:
                n_workers = 1
                threads_per_worker = _ncores
        if n_workers is None and threads_per_worker is not None:
            n_workers = max(1, _ncores // threads_per_worker)
        if n_workers and threads_per_worker is None:
            # Overcommit threads per worker, rather than undercommit
            threads_per_worker = max(1, int(math.ceil(_ncores / n_workers)))

        worker_kwargs.update({
            'ncores': threads_per_worker,
            'services': worker_services,
        })

        self._loop_runner = LoopRunner(loop=loop, asynchronous=asynchronous)
        self.loop = self._loop_runner.loop

        if diagnostics_port is not False and diagnostics_port is not None:
            try:
                from distributed.bokeh.scheduler import BokehScheduler
                from distributed.bokeh.worker import BokehWorker
            except ImportError:
                logger.debug("To start diagnostics web server please install Bokeh")
            else:
                services[('bokeh', diagnostics_port)] = (BokehScheduler, (service_kwargs or {}).get('bokeh', {}))
                worker_services[('bokeh', 0)] = BokehWorker

        self.scheduler = Scheduler(loop=self.loop,
                                   services=services,
                                   security=security)
        self.scheduler_port = scheduler_port

        self.workers = []
        self.worker_kwargs = worker_kwargs
        if security:
            self.worker_kwargs['security'] = security

        self.start(ip=ip, n_workers=n_workers)

        clusters_to_close.add(self)

    def __repr__(self):
        return ('LocalCluster(%r, workers=%d, ncores=%d)' %
                (self.scheduler_address, len(self.workers),
                 sum(w.ncores for w in self.workers))
                )

    def __await__(self):
        return self._started.__await__()

    def sync(self, func, *args, **kwargs):
        asynchronous = kwargs.pop('asynchronous', None)
        if asynchronous or self._asynchronous or getattr(thread_state, 'asynchronous', False):
            callback_timeout = kwargs.pop('callback_timeout', None)
            future = func(*args, **kwargs)
            if callback_timeout is not None:
                future = gen.with_timeout(timedelta(seconds=callback_timeout),
                                          future)
            return future
        else:
            return sync(self.loop, func, *args, **kwargs)

    def start(self, **kwargs):
        self._loop_runner.start()
        if self._asynchronous:
            self._started = self._start(**kwargs)
        else:
            self.sync(self._start, **kwargs)

    @gen.coroutine
    def _start(self, ip=None, n_workers=0):
        """
        Start all cluster services.
        """
        if self.status == 'running':
            return
        if (ip is None) and (not self.scheduler_port) and (not self.processes):
            # Use inproc transport for optimization
            scheduler_address = 'inproc://'
        elif ip is not None and ip.startswith('tls://'):
            scheduler_address = ('%s:%d' % (ip, self.scheduler_port))
        else:
            if ip is None:
                ip = '127.0.0.1'
            scheduler_address = (ip, self.scheduler_port)
        self.scheduler.start(scheduler_address)

        yield [self._start_worker(**self.worker_kwargs) for i in range(n_workers)]

        self.status = 'running'

        raise gen.Return(self)

    @gen.coroutine
    def _start_worker(self, death_timeout=60, **kwargs):
        if self.processes:
            W = Nanny
            kwargs['quiet'] = True
        else:
            W = Worker

        w = W(self.scheduler.address, loop=self.loop,
              death_timeout=death_timeout,
              silence_logs=self.silence_logs, **kwargs)
        yield w._start()

        self.workers.append(w)

        while w.status != 'closed' and w.worker_address not in self.scheduler.workers:
            yield gen.sleep(0.01)

        if w.status == 'closed' and self.scheduler.status == 'running':
            self.workers.remove(w)
            raise gen.TimeoutError("Worker failed to start")

        raise gen.Return(w)

    def start_worker(self, **kwargs):
        """ Add a new worker to the running cluster
        Parameters
        ----------
        port: int (optional)
            Port on which to serve the worker, defaults to 0 or random
        ncores: int (optional)
            Number of threads to use.  Defaults to number of logical cores
        Examples
        --------
        >>> c = LocalCluster()  # doctest: +SKIP
        >>> c.start_worker(ncores=2)  # doctest: +SKIP
        Returns
        -------
        The created Worker or Nanny object.  Can be discarded.
        """
        return self.sync(self._start_worker, **kwargs)

    @gen.coroutine
    def _stop_worker(self, w):
        yield w._close()
        if w in self.workers:
            self.workers.remove(w)

    def stop_worker(self, w):
        """ Stop a running worker
        Examples
        --------
        >>> c = LocalCluster()  # doctest: +SKIP
        >>> w = c.start_worker(ncores=2)  # doctest: +SKIP
        >>> c.stop_worker(w)  # doctest: +SKIP
        """
        self.sync(self._stop_worker, w)

    @gen.coroutine
    def _close(self):
        # Can be 'closing' as we're called by close() below
        if self.status == 'closed':
            return

        try:
            with ignoring(gen.TimeoutError, CommClosedError, OSError):
                yield All([w._close() for w in self.workers])
            with ignoring(gen.TimeoutError, CommClosedError, OSError):
                yield self.scheduler.close(fast=True)
            del self.workers[:]
        finally:
            self.status = 'closed'

    def close(self, timeout=20):
        """ Close the cluster """
        if self.status == 'closed':
            return

        try:
            self.scheduler.clear_task_state()

            for w in self.workers:
                self.loop.add_callback(self._stop_worker, w)
            for i in range(10):
                if not self.workers:
                    break
                else:
                    sleep(0.01)
            del self.workers[:]
            try:
                self._loop_runner.run_sync(self._close, callback_timeout=timeout)
            except RuntimeError:  # IOLoop is closed
                pass
            self._loop_runner.stop()
        finally:
            self.status = 'closed'
        with ignoring(AttributeError):
            silence_logging(self._old_logging_level)

    @gen.coroutine
    def scale_up(self, n, **kwargs):
        """ Bring the total count of workers up to ``n``
        This function/coroutine should bring the total number of workers up to
        the number ``n``.
        This can be implemented either as a function or as a Tornado coroutine.
        """
        with log_errors():
            kwargs2 = toolz.merge(self.worker_kwargs, kwargs)
            yield [self._start_worker(**kwargs2)
                   for i in range(n - len(self.scheduler.workers))]

            # clean up any closed worker
            self.workers = [w for w in self.workers if w.status != 'closed']

    @gen.coroutine
    def scale_down(self, workers):
        """ Remove ``workers`` from the cluster
        Given a list of worker addresses this function should remove those
        workers from the cluster.  This may require tracking which jobs are
        associated to which worker address.
        This can be implemented either as a function or as a Tornado coroutine.
        """
        with log_errors():
            # clean up any closed worker
            self.workers = [w for w in self.workers if w.status != 'closed']
            workers = set(workers)

            # we might be given addresses
            if all(isinstance(w, str) for w in workers):
                workers = {w for w in self.workers if w.worker_address in workers}

            # stop the provided workers
            yield [self._stop_worker(w) for w in workers]

    def __del__(self):
        self.close()

    def __enter__(self):
        return self

    def __exit__(self, *args):
        self.close()

    @gen.coroutine
    def __aenter__(self):
        yield self._started
        raise gen.Return(self)

    @gen.coroutine
    def __aexit__(self, typ, value, traceback):
        yield self._close()

    @property
    def scheduler_address(self):
        try:
            return self.scheduler.address
        except ValueError:
            return '<unstarted>'


clusters_to_close = weakref.WeakSet()


'''
subclass KubeCluster
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

@atexit.register
def close_clusters():
    for cluster in list(clusters_to_close):
        cluster.close(timeout=10)

class MyKubeCluster(MyCluster):
    """ Launch a Dask cluster on Kubernetes
    This starts a local Dask scheduler and then dynamically launches
    Dask workers on a Kubernetes cluster. The Kubernetes cluster is taken
    to be either the current one on which this code is running, or as a
    fallback, the default one configured in a kubeconfig file.
    **Environments**
    Your worker pod image should have a similar environment to your local
    environment, including versions of Python, dask, cloudpickle, and any
    libraries that you may wish to use (like NumPy, Pandas, or Scikit-Learn).
    See examples below for suggestions on how to manage and check for this.
    **Network**
    Since the Dask scheduler is launched locally, for it to work, we need to
    be able to open network connections between this local node and all the
    workers nodes on the Kubernetes cluster. If the current process is not
    already on a Kubernetes node, some network configuration will likely be
    required to make this work.
    **Resources**
    Your Kubernetes resource limits and requests should match the
    ``--memory-limit`` and ``--nthreads`` parameters given to the
    ``dask-worker`` command.
    Parameters
    ----------
    pod_template: kubernetes.client.V1PodSpec
        A Kubernetes specification for a Pod for a dask worker.
    name: str (optional)
        Name given to the pods.  Defaults to ``dask-$USER-random``
    namespace: str (optional)
        Namespace in which to launch the workers.
        Defaults to current namespace if available or "default"
    n_workers: int
        Number of workers on initial launch.
        Use ``scale_up`` to increase this number in the future
    env: Dict[str, str]
        Dictionary of environment variables to pass to worker pod
    host: str
        Listen address for local scheduler.  Defaults to 0.0.0.0
    port: int
        Port of local scheduler
    **kwargs: dict
        Additional keyword arguments to pass to LocalCluster
    Examples
    --------
    >>> from dask_kubernetes import KubeCluster, make_pod_spec
    >>> pod_spec = make_pod_spec(image='daskdev/dask:latest',
    ...                          memory_limit='4G', memory_request='4G',
    ...                          cpu_limit=1, cpu_request=1,
    ...                          env={'EXTRA_PIP_PACKAGES': 'fastparquet git+https://github.com/dask/distributed'})
    >>> cluster = KubeCluster(pod_spec)
    >>> cluster.scale_up(10)
    You can also create clusters with worker pod specifications as dictionaries
    or stored in YAML files
    >>> cluster = KubeCluster.from_yaml('worker-template.yml')
    >>> cluster = KubeCluster.from_dict({...})
    Rather than explicitly setting a number of workers you can also ask the
    cluster to allocate workers dynamically based on current workload
    >>> cluster.adapt()
    You can pass this cluster directly to a Dask client
    >>> from dask.distributed import Client
    >>> client = Client(cluster)
    You can verify that your local environment matches your worker environments
    by calling ``client.get_versions(check=True)``.  This will raise an
    informative error if versions do not match.
    >>> client.get_versions(check=True)
    The ``daskdev/dask`` docker images support ``EXTRA_PIP_PACKAGES``,
    ``EXTRA_APT_PACKAGES`` and ``EXTRA_CONDA_PACKAGES`` environment variables
    to help with small adjustments to the worker environments.  We recommend
    the use of pip over conda in this case due to a much shorter startup time.
    These environment variables can be modified directly from the KubeCluster
    constructor methods using the ``env=`` keyword.  You may list as many
    packages as you like in a single string like the following:
    >>> pip = 'pyarrow gcsfs git+https://github.com/dask/distributed'
    >>> conda = '-c conda-forge scikit-learn'
    >>> KubeCluster.from_yaml(..., env={'EXTRA_PIP_PACKAGES': pip,
    ...                                 'EXTRA_CONDA_PACKAGES': conda})
    You can also start a KubeCluster with no arguments *if* the worker template
    is specified in the Dask config files, either as a full template in
    ``kubernetes.worker-template`` or a path to a YAML file in
    ``kubernetes.worker-template-path``.
    See https://docs.dask.org/en/latest/configuration.html for more
    information about setting configuration values.::
        $ export DASK_KUBERNETES__WORKER_TEMPLATE_PATH=worker_template.yaml
    >>> cluster = KubeCluster()  # automatically finds 'worker_template.yaml'
    See Also
    --------
    KubeCluster.from_yaml
    KubeCluster.from_dict
    KubeCluster.adapt
    """
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

    @classmethod
    def from_dict(cls, pod_spec, **kwargs):
        """ Create cluster with worker pod spec defined by Python dictionary
        Examples
        --------
        >>> spec = {
        ...     'metadata': {},
        ...     'spec': {
        ...         'containers': [{
        ...             'args': ['dask-worker', '$(DASK_SCHEDULER_ADDRESS)',
        ...                      '--nthreads', '1',
        ...                      '--death-timeout', '60'],
        ...             'command': None,
        ...             'image': 'daskdev/dask:latest',
        ...             'name': 'dask-worker',
        ...         }],
        ...     'restartPolicy': 'Never',
        ...     }
        ... }
        >>> cluster = KubeCluster.from_dict(spec, namespace='my-ns')  # doctest: +SKIP
        See Also
        --------
        KubeCluster.from_yaml
        """
        return cls(make_pod_from_dict(pod_spec), **kwargs)

    @classmethod
    def from_yaml(cls, yaml_path, **kwargs):
        """ Create cluster with worker pod spec defined by a YAML file
        We can start a cluster with pods defined in an accompanying YAML file
        like the following:
        .. code-block:: yaml
            kind: Pod
            metadata:
              labels:
                foo: bar
                baz: quux
            spec:
              containers:
              - image: daskdev/dask:latest
                name: dask-worker
                args: [dask-worker, $(DASK_SCHEDULER_ADDRESS), --nthreads, '2', --memory-limit, 8GB]
              restartPolicy: Never
        Examples
        --------
        >>> cluster = KubeCluster.from_yaml('pod.yaml', namespace='my-ns')  # doctest: +SKIP
        See Also
        --------
        KubeCluster.from_dict
        """
        if not yaml:
            raise ImportError("PyYaml is required to use yaml functionality, please install it!")
        with open(yaml_path) as f:
            d = yaml.safe_load(f)
            d = dask.config.expand_environment_variables(d)
            return cls.from_dict(d, **kwargs)

    @property
    def namespace(self):
        return self.pod_template.metadata.namespace

    @property
    def name(self):
        return self.pod_template.metadata.generate_name

    def __repr__(self):
        return 'KubeCluster("%s", workers=%d)' % (self.scheduler.address,
                                                  len(self.pods()))

    @property
    def scheduler(self):
        return self.cluster.scheduler

    @property
    def scheduler_address(self):
        return self.scheduler.address

    def pods(self):
        """ A list of kubernetes pods corresponding to current workers
        See Also
        --------
        KubeCluster.logs
        """
        return self.core_api.list_namespaced_pod(
            self.namespace,
            label_selector=format_labels(self.pod_template.metadata.labels)
        ).items

    def logs(self, pod=None):
        """ Logs from a worker pod
        You can get this pod object from the ``pods`` method.
        If no pod is specified all pod logs will be returned. On large clusters
        this could end up being rather large.
        Parameters
        ----------
        pod: kubernetes.client.V1Pod
            The pod from which we want to collect logs.
        See Also
        --------
        KubeCluster.pods
        Client.get_worker_logs
        """
        if pod is None:
            return {pod.status.pod_ip: self.logs(pod) for pod in self.pods()}

        return self.core_api.read_namespaced_pod_log(pod.metadata.name,
                                                     pod.metadata.namespace)

    def scale(self, n):
        """ Scale cluster to n workers
        Parameters
        ----------
        n: int
            Target number of workers
        Example
        -------
        >>> cluster.scale(10)  # scale cluster to ten workers
        See Also
        --------
        KubeCluster.scale_up
        KubeCluster.scale_down
        """
        pods = self._cleanup_terminated_pods(self.pods())
        if n >= len(pods):
            return self.scale_up(n, pods=pods)
        else:
            n_to_delete = len(pods) - n
            # Before trying to close running workers, check if we can cancel
            # pending pods (in case the kubernetes cluster was too full to
            # provision those pods in the first place).
            running_workers = list(self.scheduler.workers.keys())
            running_ips = set(urlparse(worker).hostname
                              for worker in running_workers)
            pending_pods = [p for p in pods
                            if p.status.pod_ip not in running_ips]
            if pending_pods:
                pending_to_delete = pending_pods[:n_to_delete]
                logger.debug("Deleting pending pods: %s", pending_to_delete)
                self._delete_pods(pending_to_delete)
                n_to_delete = n_to_delete - len(pending_to_delete)
                if n_to_delete <= 0:
                    return

            to_close = select_workers_to_close(self.scheduler, n_to_delete)
            logger.debug("Closing workers: %s", to_close)
            if len(to_close) < len(self.scheduler.workers):
                # Close workers cleanly to migrate any temporary results to
                # remaining workers.
                @gen.coroutine
                def f(to_close):
                    yield self.scheduler.retire_workers(
                        workers=to_close, remove=True, close_workers=True)
                    yield offload(self.scale_down, to_close)

                self.scheduler.loop.add_callback(f, to_close)
                return

            # Terminate all pods without waiting for clean worker shutdown
            self.scale_down(to_close)

    def _delete_pods(self, to_delete):
        for pod in to_delete:
            try:
                self.core_api.delete_namespaced_pod(
                    pod.metadata.name,
                    self.namespace,
                    kubernetes.client.V1DeleteOptions()
                )
                pod_info = pod.metadata.name
                if pod.status.reason:
                    pod_info += ' [{}]'.format(pod.status.reason)
                if pod.status.message:
                    pod_info += ' {}'.format(pod.status.message)
                logger.info('Deleted pod: %s', pod_info)
            except kubernetes.client.rest.ApiException as e:
                # If a pod has already been removed, just ignore the error
                if e.status != 404:
                    raise

    def _cleanup_terminated_pods(self, pods):
        terminated_phases = {'Succeeded', 'Failed'}
        terminated_pods = [p for p in pods if p.status.phase in terminated_phases]
        self._delete_pods(terminated_pods)
        return [p for p in pods if p.status.phase not in terminated_phases]

    def scale_up(self, n, pods=None, **kwargs):
        """
        Make sure we have n dask-workers available for this cluster
        Examples
        --------
        >>> cluster.scale_up(20)  # ask for twenty workers
        """
        maximum = dask.config.get('kubernetes.count.max')
        if maximum is not None and maximum < n:
            logger.info("Tried to scale beyond maximum number of workers %d > %d",
                        n, maximum)
            n = maximum
        pods = pods or self._cleanup_terminated_pods(self.pods())
        to_create = n - len(pods)
        new_pods = []
        for i in range(3):
            try:
                for _ in range(to_create):
                    new_pods.append(self.core_api.create_namespaced_pod(
                        self.namespace, self.pod_template))
                    to_create -= 1
                break
            except kubernetes.client.rest.ApiException as e:
                if e.status == 500 and 'ServerTimeout' in e.body:
                    logger.info("Server timeout, retry #%d", i + 1)
                    time.sleep(1)
                    last_exception = e
                    continue
                else:
                    raise
        else:
            raise last_exception

        return new_pods
        # fixme: wait for this to be ready before returning!

    def scale_down(self, workers, pods=None):
        """ Remove the pods for the requested list of workers
        When scale_down is called by the _adapt async loop, the workers are
        assumed to have been cleanly closed first and in-memory data has been
        migrated to the remaining workers.
        Note that when the worker process exits, Kubernetes leaves the pods in
        a 'Succeeded' state that we collect here.
        If some workers have not been closed, we just delete the pods with
        matching ip addresses.
        Parameters
        ----------
        workers: List[str] List of addresses of workers to close
        """
        # Get the existing worker pods
        pods = pods or self._cleanup_terminated_pods(self.pods())

        # Work out the list of pods that we are going to delete
        # Each worker to delete is given in the form "tcp://<worker ip>:<port>"
        # Convert this to a set of IPs
        ips = set(urlparse(worker).hostname for worker in workers)
        to_delete = [p for p in pods if p.status.pod_ip in ips]
        if not to_delete:
            return
        self._delete_pods(to_delete)

    def __enter__(self):
        return self

    def close(self):
        """ Close this cluster """
        self.scale_down(self.cluster.scheduler.workers)
        self.cluster.close()

    def __exit__(self, type, value, traceback):
        _cleanup_pods(self.namespace, self.pod_template.metadata.labels)
        self.cluster.__exit__(type, value, traceback)


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


def format_labels(labels):
    """ Convert a dictionary of labels into a comma separated string """
    if labels:
        return ','.join(['{}={}'.format(k, v) for k, v in labels.items()])
    else:
        return ''


def _namespace_default():
    """
    Get current namespace if running in a k8s cluster
    If not in a k8s cluster with service accounts enabled, default to
    'default'
    Taken from https://github.com/jupyterhub/kubespawner/blob/master/kubespawner/spawner.py#L125
    """
    ns_path = '/var/run/secrets/kubernetes.io/serviceaccount/namespace'
    if os.path.exists(ns_path):
        with open(ns_path) as f:
            return f.read().strip()
    return 'default'


def select_workers_to_close(scheduler, n_to_close):
    """ Select n workers to close from scheduler """
    workers = list(scheduler.workers.values())
    assert n_to_close <= len(workers)
    key = lambda ws: ws.metrics['memory']
    to_close = set(sorted(scheduler.idle, key=key)[:n_to_close])

    if len(to_close) < n_to_close:
        rest = sorted(workers, key=key, reverse=True)
        while len(to_close) < n_to_close:
            to_close.add(rest.pop())

    return [ws.address for ws in to_close]


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
