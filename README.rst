=================
RHG Compute Tools
=================


.. image:: https://img.shields.io/pypi/v/rhg_compute_tools.svg
        :target: https://pypi.python.org/pypi/rhg_compute_tools

.. image:: https://github.com/RhodiumGroup/rhg_compute_tools/workflows/Python%20package/badge.svg

.. image:: https://readthedocs.org/projects/rhg-compute-tools/badge/?version=latest
        :target: https://rhg-compute-tools.readthedocs.io/en/latest/?badge=latest
        :alt: Documentation Status

Tools for using compute.rhg.com and compute.impactlab.org


* Free software: MIT license
* Documentation: https://rhg-compute-tools.readthedocs.io.

Installation
------------

pip:

.. code-block:: bash

    pip install rhg_compute_tools



Features
--------

Kubernetes tools
~~~~~~~~~~~~~~~~

* easily spin up a preconfigured cluster with ``get_cluster()``, or flavors with ``get_micro_cluster()``, ``get_standard_cluster()``, ``get_big_cluster()``, or ``get_giant_cluster()``.

.. code-block:: python

    >>> import rhg_compute_tools.kubernetes as rhgk
    >>> cluster, client = rhgk.get_cluster()

Google cloud storage utilities
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

* Utilities for managing google cloud storage directories in parallel from the command line or via a python API

.. code-block:: python

   >>> import rhg_compute_tools.gcs as gcs
   >>> gcs.sync_gcs('my_data_dir', 'gs://my-bucket/my_data_dir')
