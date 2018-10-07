#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `rhg_compute_tools` package."""

import pytest
import inspect

from rhg_compute_tools import gcs, kubernetes, utils


@pytest.fixture
def response():
    """Sample pytest fixture.

    See more at: http://doc.pytest.org/en/latest/fixture.html
    """
    # import requests
    # return requests.get('https://github.com/audreyr/cookiecutter-pypackage')
    pass


def test_content(response):
    """Sample pytest test function with the pytest fixture as an argument."""
    # from bs4 import BeautifulSoup
    # assert 'GitHub' in BeautifulSoup(response.content).title.string
    pass


def test_docstrings():
    for mod in [gcs, kubernetes, utils]:
        for cname, component in mod.__dict__.items():
            if cname.startswith('_'):
                continue

            # check if the object belongs to the module
            if inspect.getmodule(component) is not mod:
                continue

            has_doc = False

            if hasattr(component, '__doc__'):
                if component.__doc__ is not None:
                    has_doc = len(component.__doc__.strip()) > 0

            msg = (
                'no docstring found for {} in module {}'
                .format(cname, inspect.getmodule(component).__spec__.name))

            assert has_doc, msg
