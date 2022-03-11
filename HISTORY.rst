
History
=======

.. current developments

v1.2.1
------
Bug fixes:
* raise error on gsutil nonzero status in ``rhg_compute_tools.gcs.cp`` (``PR #105``)

v1.2
----
New features:
* Adds google storage directory marker utilities and rctools gcs mkdirs command line app


v1.1.4
------
* Add ``dask_kwargs`` to the ``rhg_compute_tools.xarray`` functions

v1.1.3
------
* Add ``retry_with_timeout`` to ``rhg_compute_tools.utils.py``

v1.1.2
------
* Drop ``matplotlib.font_manager._rebuild()`` call in ``design.__init__`` - no longer supported (:issue:`96`)

v1.1.1
------
* Refactor ``datasets_from_delayed`` to speed up

v1.1
----
* Add `gcs.ls` function

v1.0.1
------
* Fix `tag` kwarg in `get_cluster`

v1.0.0
------
* Make the gsutil API consistent, so that we have `cp`, `sync` and `rm`, each of which
  accept the same args and kwargs (:issue:`69`)
* Swap ``bumpversion`` for ``setuptools_scm`` to handle versioning (:issue:`78`)
* Cast coordinates to dict before gathering in ``rhg_compute_tools.xarray.dataarrays_from_delayed`` and ``rhg_compute_tools.xarray.datasets_from_delayed``. This avoids a mysterious memory explosion on the local machine. Also add ``name`` in the metadata used by those functions so that the name of each dataarray or Variable is preserved. (:issue:`83`)
* Use ``dask-gateway`` when available when creating a cluster in ``rhg_compute_tools.kubernetes``. Add some tests using a local gateway cluster. TODO: More tests.
* Add ``tag`` kwarg to ``rhg_compute_tools.kuberentes.get_cluster`` function (PR #87)

v0.2.2
------
* ?

v0.2.1
------
* Add remote scheduler deployment (part of dask_kubernetes 0.10)
* Remove extraneous `GCSFUSE_TOKENS` env var no longer used in new worker images
* Set library thread limits based on how many cpus are available for a single dask thread
* Change formatting of the extra `env_items` passed to `get_cluster` to be a list rather than a list of dict-like name/value pairs

v0.2.0
------

* Add CLI tools (:issue:`37`). See ``rctools gcs repdirstruc --help`` to start
* Add new function ``rhg_compute_tools.gcs.replicate_directory_structure_on_gcs`` to copy directory trees into GCS. Users can authenticate with cred_file or with default google credentials (:issue:`51`)
* Fixes to docstrings and metadata (:issue:`43`) (:issue:`45`)
* Add new function ``rhg_compute_tools.gcs.rm`` to remove files/directories on GCS using the ``google.cloud.storage`` API
* Store one additional environment variable when passing ``cred_path`` to ``rhg_compute_tools.kubernetes.get_cluster`` so that the ``google.cloud.storage`` API will be authenticated in addition to ``gsutil``

v0.1.8
------

* Deployment fixes

v0.1.7
------

* Design tools: use RHG & CIL colors & styles
* Plotting helpers: generate cmaps with consistent colors & norms, and apply a colorbar to geopandas plots with nonlinear norms
* Autoscaling fix for kubecluster: switch to dask_kubernetes.KubeCluster to allow use of recent bug fixes


v0.1.6
------

* Add ``rhg_compute_tools.gcs.cp_gcs`` and ``rhg_compute_tools.gcs.sync_gcs`` utilities

v0.1.5
------

* need to figure out how to use this rever thing

v0.1.4
------

* Bug fix again in ``rhg_compute_tools.kubernetes.get_worker``


v0.1.3
------

* Bug fix in ``rhg_compute_tools.kubernetes.get_worker``


v0.1.2
------

* Add xarray from delayed methods in ``rhg_compute_tools.xarray`` (:issue:`12`)
* ``rhg_compute_tools.gcs.cp_to_gcs`` now calls ``gsutil`` in a subprocess instead of ``google.storage`` operations. This dramatically improves performance when transferring large numbers of small files (:issue:`11`)
* Additional cluster creation helpers (:issue:`3`)

v0.1.1
------

* New google compute helpers (see ``rhg_compute_tools.gcs.cp_to_gcs``, ``rhg_compute_tools.gcs.get_bucket``)
* New cluster creation helper (see ``rhg_compute_tools.kubernetes.get_worker``)
* Dask client.map helpers (see ``rhg_compute_tools.utils submodule``)

v0.1.0
------

* First release on PyPI.
