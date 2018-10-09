=======
History
=======

v0.1.2 (2018-10-09)
===================

* Add xarray from delayed methods in ``rhg_compute_tools.xarray`` (:issue:`12`)
* ``rhg_compute_tools.gcs.cp_to_gcs`` now calls ``gsutil`` in a subprocess instead of ``google.storage`` operations. This dramatically improves performance when transferring large numbers of small files (:issue:`11`)
* Additional cluster creation helpers (:issue:`3`)

v0.1.1 (2018-10-08)

===================

* New google compute helpers (see ``rhg_compute_tools.gcs.cp_to_gcs``, ``rhg_compute_tools.gcs.get_bucket``)
* New cluster creation helper (see ``rhg_compute_tools.kubernetes.get_worker``)
* Dask client.map helpers (see ``rhg_compute_tools.utils submodule``)

v0.1.0 (2018-08-07)
===================

* First release on PyPI.
