#!/usr/bin/env python
# -*- coding: utf-8 -*-

"""Tests for `rhg_compute_tools` package."""

import pytest
import inspect

from rhg_compute_tools import gcs, kubernetes, utils


def monkeypatch_cluster(func):
    def inner(monkeypatch, mem, cpu, scale):
        def mock_KubeCluster_from_dict(dict_argument):
            return dict_argument

        def mock_dask_Client(cluster):
            return cluster

        monkeypatch.setattr(
            'dask_kubernetes.KubeCluster.from_dict',
            mock_KubeCluster_from_dict)

        monkeypatch.setattr(
            'dask.distributed.Client',
            mock_dask_Client)

        return func(mem=mem, cpu=cpu, scale=scale)

    return inner


@pytest.mark.parametrize("mem,cpu,scale", [(None, None, None)])
@monkeypatch_cluster
def test_create_worker(mem=None, cpu=None, scale=None):

    cluster, client = kubernetes.get_cluster(
        template_path='tests/resources/worker_template.yml')

    mem = cluster['spec']['containers'][0]['args'][5]
    assert mem == '11.50GB', mem

    res_lim = cluster['spec']['containers'][0]['resources']['limits']
    assert res_lim['memory'] == '11.50G', res_lim['memory']
    assert res_lim['cpu'] == '1.75', res_lim['cpu']

    res_req = cluster['spec']['containers'][0]['resources']['requests']
    assert res_req['memory'] == '11.50G', res_req['memory']
    assert res_req['cpu'] == '1.75', res_req['cpu']


size_test_params = [(35, 7, None), (1, 6, None), (4, 1, None)]


@pytest.mark.parametrize("mem,cpu,scale", size_test_params)
@monkeypatch_cluster
def test_size_worker(mem, cpu, scale):

    cluster, client = kubernetes.get_cluster(
        template_path='tests/resources/worker_template.yml',
        memory_gb=mem,
        cpus=cpu)

    mem_arg = cluster['spec']['containers'][0]['args'][5]
    assert abs(mem - float(mem_arg.strip('GB'))) < 0.01, mem_arg

    res_lim = cluster['spec']['containers'][0]['resources']['limits']
    mem_size = float(res_lim['memory'].strip('G'))
    assert abs(mem_size - mem) < 0.01, res_lim['memory']
    assert abs(float(res_lim['cpu']) - cpu) < 0.01, res_lim['cpu']

    res_req = cluster['spec']['containers'][0]['resources']['requests']
    mem_size = float(res_req['memory'].strip('G'))
    assert abs(mem_size - mem) < 0.01, res_req['memory']
    assert abs(float(res_req['cpu']) - cpu) < 0.01, res_req['cpu']


scale_test_params = [
    (None, None, 0.5),
    (None, None, 1),
    (None, None, 2),
    (None, None, 4)]


@pytest.mark.parametrize("mem,cpu,scale", scale_test_params)
@monkeypatch_cluster
def test_scale_worker(mem, cpu, scale):

    cluster, client = kubernetes.get_cluster(
        template_path='tests/resources/worker_template.yml',
        scaling_factor=scale)

    mem_arg = cluster['spec']['containers'][0]['args'][5]
    assert abs((scale * 11.5) - float(mem_arg.strip('GB'))) < 0.01, mem_arg

    res_lim = cluster['spec']['containers'][0]['resources']['limits']
    mem_val = float(res_lim['memory'].strip('G'))
    assert abs(mem_val - (scale * 11.5)) < 0.01, res_lim['memory']
    assert abs(float(res_lim['cpu']) - (scale * 1.75)) < 0.01, res_lim['cpu']

    res_req = cluster['spec']['containers'][0]['resources']['requests']
    mem_val = float(res_req['memory'].strip('G'))
    assert abs(mem_val - (scale * 11.5)) < 0.01, res_req['memory']
    assert abs(float(res_req['cpu']) - (scale * 1.75)) < 0.01, res_req['cpu']
