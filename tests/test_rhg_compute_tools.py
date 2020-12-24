#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `rhg_compute_tools` package."""

from time import sleep

import pytest
from dask_gateway import Gateway

from rhg_compute_tools import kubernetes


class LocalGateway(Gateway):
    def __init__(self):
        # initialize local gateway
        super().__init__("http://127.0.0.1:8000")


def monkeypatch_cluster(func):
    def inner(monkeypatch, mem, cpu, scale):
        def mock_KubeCluster_from_dict(dict_argument, **kwargs):
            return dict_argument

        def mock_dask_Client(cluster):
            return cluster

        monkeypatch.setattr("dask_gateway.Gateway", LocalGateway)

        monkeypatch.setattr("dask.distributed.Client", mock_dask_Client)

        return func(mem=mem, cpu=cpu, scale=scale)

    return inner


@pytest.mark.parametrize("mem,cpu,scale", [(None, None, None)])
@monkeypatch_cluster
def test_create_worker_gateway(mem=None, cpu=None, scale=None):

    # can only test options that are valid on a UnsafeLocalBackend (e.g. nothing that
    # has to do with kubernetes pod config)
    client, cluster = kubernetes._get_cluster_dask_gateway(
        profile="micro",
        cred_path="path/to/test_cred.json",
        env_items={"TEST_ITEM": "TEST_RESULT"},
    )

    cluster.scale(1)
    sleep(2)

    # check env_items worked
    def return_test_env_var(key):
        import os

        return os.environ[key]

    assert client.gather(
        client.map(return_test_env_var, ["GOOGLE_APPLICATION_CREDENTIALS", "TEST_ITEM"])
    ) == ["/opt/gcsfuse_tokens/test_cred.json", "TEST_RESULT"]

    # scale down gracefully
    cluster.scale(0), cluster.close(), client.close()


size_test_params = [(35, 7, None), (1, 6, None), (4, 1, None)]


scale_test_params = [
    (None, None, 0.5),
    (None, None, 1),
    (None, None, 2),
    (None, None, 4),
]
