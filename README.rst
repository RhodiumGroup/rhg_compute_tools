=================
RHG Compute Tools
=================


.. image:: https://img.shields.io/pypi/v/rhg_compute_tools.svg
        :target: https://pypi.python.org/pypi/rhg_compute_tools

.. image:: https://img.shields.io/travis/RhodiumGroup/rhg_compute_tools.svg
        :target: https://travis-ci.org/RhodiumGroup/rhg_compute_tools

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

* easily spin up a preconfigured cluster with ``get_cluster()``, or flavors with ``get_micro_cluster()``, ``get_standard_cluster()``, ``get_big_cluster()``, or ``get_giant_cluster()``.

.. code-block::python

    >>> import rhg_compute_tools as rhg
    >>> cluster, client = rhg.get_cluster()
