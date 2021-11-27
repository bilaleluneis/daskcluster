__author__ = "Bilal El Uneis"
__since__ = "October 2021"
__email__ = "bilaleluneis@gmail.com"

from configurable.cluster.ssh import SSHCluster as Cluster


def test_local_cluster_init():
    cluster = Cluster('cluster_configs/local.json', False)
    _ = cluster.client
    cluster.shutdown()


def test_full_cluster_init():
    cl = Cluster('cluster_configs/multi-node.json', False)
    _ = cl.client
    cl.shutdown()
