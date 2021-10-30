__author__ = "Bilal El Uneis"
__since__ = "October 2021"
__email__ = "bilaleluneis@gmail.com"

from configurable.cluster.ssh import SSHCluster as Cluster


def test_local_cluster_init():
    cluster = Cluster('local_cluster_config.json', False)
    client = cluster.client
    cluster.shutdown()


def test_full_cluster_init():
    cl = Cluster('full_cluster_config.json', False)
    client = cl.client
    cl.shutdown()
