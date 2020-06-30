#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""The setup script."""

from setuptools import setup, find_packages
import re

with open('README.rst') as readme_file:
    readme = readme_file.read()

with open('HISTORY.rst') as history_file:
    history = re.sub(r'\(:issue:`[0-9]+`\)', '', history_file.read())

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
    version='0.2.2',
    description="Tools for using compute.rhg.com and compute.impactlab.org",
    long_description=readme + '\n\n' + history,
    long_description_content_type='text/x-rst',
    author="Michael Delgado",
    author_email='mdelgado@rhg.com',
    url='https://github.com/RhodiumGroup/rhg_compute_tools',
    packages=find_packages(include=['rhg_compute_tools']),
    include_package_data=True,
    package_data={
        'rhg_compute_tools': ['*.mplstyle'],
    },
    install_requires=requirements,
    license="MIT license",
    zip_safe=False,
    keywords='rhg_compute_tools',
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: MIT License',
        'Natural Language :: English',
        'Programming Language :: Python :: 3',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
    ],
    test_suite='tests',
    tests_require=test_requirements,
    setup_requires=setup_requirements,
    entry_points={
        'console_scripts': [
            'rctools = rhg_compute_tools.cli:rctools_cli',
        ]
    },
)
