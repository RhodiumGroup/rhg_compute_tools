#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

import re

from setuptools import find_packages, setup

with open("README.rst") as readme_file:
    readme = readme_file.read()

with open("HISTORY.rst") as history_file:
    history = re.sub(r"\(:issue:`[0-9]+`\)", "", history_file.read())

requirements = [
    "google-cloud-storage",
    "click",
    "dask-gateway",
    "pandas",
    "xarray",
    "matplotlib",
    "numpy",
    "bottleneck",
    # TODO: put package requirements here
]

setup(
    name="rhg_compute_tools",
    use_scm_version=True,
    description="Tools for using compute.rhg.com and compute.impactlab.org",
    long_description=readme + "\n\n" + history,
    long_description_content_type="text/x-rst",
    author="Michael Delgado",
    author_email="mdelgado@rhg.com",
    url="https://github.com/RhodiumGroup/rhg_compute_tools",
    packages=find_packages(include=["rhg_compute_tools"]),
    include_package_data=True,
    package_data={
        "rhg_compute_tools": ["*.mplstyle"],
    },
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords="rhg_compute_tools",
    classifiers=[
        "Development Status :: 4 - Beta",
        "Intended Audience :: Science/Research",
        "License :: OSI Approved :: MIT License",
        "Natural Language :: English",
        "Programming Language :: Python :: 3",
        "Programming Language :: Python :: 3.7",
        "Programming Language :: Python :: 3.8",
    ],
    entry_points={
        "console_scripts": [
            "rctools = rhg_compute_tools.cli:rctools_cli",
        ]
    },
)
