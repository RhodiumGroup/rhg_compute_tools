#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `rhg_compute_tools` package."""

import pytest

from rhg_compute_tools import kubernetes
from dask_gateway import Gateway

        
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

        monkeypatch.setattr(
            "dask_kubernetes.KubeCluster.from_dict", mock_KubeCluster_from_dict
        )
        
        monkeypatch.setattr(
            "dask_gateway.Gateway", LocalGateway
        )

        monkeypatch.setattr("dask.distributed.Client", mock_dask_Client)

        return func(mem=mem, cpu=cpu, scale=scale)

    return inner


@pytest.mark.parametrize("mem,cpu,scale", [(None, None, None)])
@monkeypatch_cluster
def test_create_worker_kube(mem=None, cpu=None, scale=None):

    client, cluster = kubernetes._get_cluster_dask_kubernetes(
        template_path="tests/resources/worker_template.yml"
    )

    mem = cluster["spec"]["containers"][0]["args"][5]
    assert mem == "11.50GB", mem

    res_lim = cluster["spec"]["containers"][0]["resources"]["limits"]
    assert res_lim["memory"] == "11.50G", res_lim["memory"]
    assert res_lim["cpu"] == "1.75", res_lim["cpu"]

    res_req = cluster["spec"]["containers"][0]["resources"]["requests"]
    assert res_req["memory"] == "11.50G", res_req["memory"]
    assert res_req["cpu"] == "1.75", res_req["cpu"]


@pytest.mark.parametrize("mem,cpu,scale", [(None, None, None)])
@monkeypatch_cluster
def test_create_worker_gateway(mem=None, cpu=None, scale=None):

    client, cluster = kubernetes._get_cluster_dask_gateway(
        name="test-image",
        profile="small",
        cred_path="path/to/test_cred.json"
    )

    assert cluster.worker_image == "test-image"
    assert cluster.scheduler_image == "rhodium/scheduler:latest"
    assert cluster.profile == "small"
    assert cluster.cred_name == "test_cred"


size_test_params = [(35, 7, None), (1, 6, None), (4, 1, None)]


@pytest.mark.parametrize("mem,cpu,scale", size_test_params)
@monkeypatch_cluster
def test_size_worker_kube(mem, cpu, scale):

    client, cluster = kubernetes._get_cluster_dask_kubernetes(
        template_path="tests/resources/worker_template.yml", memory_gb=mem, cpus=cpu
    )

    mem_arg = cluster["spec"]["containers"][0]["args"][5]
    assert abs(mem - float(mem_arg.strip("GB"))) < 0.01, mem_arg

    res_lim = cluster["spec"]["containers"][0]["resources"]["limits"]
    mem_size = float(res_lim["memory"].strip("G"))
    assert abs(mem_size - mem) < 0.01, res_lim["memory"]
    assert abs(float(res_lim["cpu"]) - cpu) < 0.01, res_lim["cpu"]

    res_req = cluster["spec"]["containers"][0]["resources"]["requests"]
    mem_size = float(res_req["memory"].strip("G"))
    assert abs(mem_size - mem) < 0.01, res_req["memory"]
    assert abs(float(res_req["cpu"]) - cpu) < 0.01, res_req["cpu"]


scale_test_params = [
    (None, None, 0.5),
    (None, None, 1),
    (None, None, 2),
    (None, None, 4),
]


@pytest.mark.parametrize("mem,cpu,scale", scale_test_params)
@monkeypatch_cluster
def test_scale_worker_kube(mem, cpu, scale):

    client, cluster = kubernetes._get_cluster_dask_kubernetes(
        template_path="tests/resources/worker_template.yml", scaling_factor=scale
    )

    mem_arg = cluster["spec"]["containers"][0]["args"][5]
    assert abs((scale * 11.5) - float(mem_arg.strip("GB"))) < 0.01, mem_arg

    res_lim = cluster["spec"]["containers"][0]["resources"]["limits"]
    mem_val = float(res_lim["memory"].strip("G"))
    assert abs(mem_val - (scale * 11.5)) < 0.01, res_lim["memory"]
    assert abs(float(res_lim["cpu"]) - (scale * 1.75)) < 0.01, res_lim["cpu"]

    res_req = cluster["spec"]["containers"][0]["resources"]["requests"]
    mem_val = float(res_req["memory"].strip("G"))
    assert abs(mem_val - (scale * 11.5)) < 0.01, res_req["memory"]
    assert abs(float(res_req["cpu"]) - (scale * 1.75)) < 0.01, res_req["cpu"]

    
scale_test_params = [
    (None, None, "micro"),
    (None, None, "standard"),
    (None, None, "big"),
    (None, None, "giant"),
]


@pytest.mark.parametrize("mem,cpu,scale", scale_test_params)
@monkeypatch_cluster
def test_scale_worker_gateway(mem, cpu, scale):

    client, cluster = kubernetes._get_cluster_dask_gateway(
        profile=scale
    )
    
    assert cluster.profile == scale