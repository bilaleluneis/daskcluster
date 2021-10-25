__author__ = "Bilal El Uneis"
__since__ = "October 2021"
__email__ = "bilaleluneis@gmail.com"

from configurable.cluster.ssh import SSHCluster as Cluster


def test_local_cluster_init():
    with Cluster('local_cluster_config.json'):
        ...
