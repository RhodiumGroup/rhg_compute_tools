#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = history_file.read()

requirements = [
    'dask_kubernetes',
    'google-cloud-storage'
    # TODO: put package requirements here
]

setup_requirements = [
    'pytest-runner',
    # TODO(delgadom): put setup requirements (distutils extensions, etc.) here
]

test_requirements = [
    'pytest',
    # TODO: put package test requirements here
]

setup(
    name='rhg_compute_tools',
    version='0.1.6',
    description="Tools for using compute.rhg.com and compute.impactlab.org",
    long_description=readme + '\n\n' + history,
    author="Michael Delgado",
    author_email='mdelgado@rhg.com',
    url='https://github.com/RhodiumGroup/rhg_compute_tools',
    packages=find_packages(include=['rhg_compute_tools']),
    include_package_data=True,
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='rhg_compute_tools',
    classifiers=[
        'Development Status :: 2 - Pre-Alpha',
        'Intended Audience :: Developers',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        "Programming Language :: Python :: 2",
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6'
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
)
