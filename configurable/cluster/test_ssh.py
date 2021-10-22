__author__ = "Bilal El Uneis"
__since__ = "October 2021"
__email__ = "bilaleluneis@gmail.com"

from configurable.cluster.ssh import SSHCluster


def test_init_cluster():
    cluster = SSHCluster('ssh_test_configs.json')
    client = cluster.client
    cluster.shutdown()


def test_force_kill_cluster():
    """ capture cluster process ids and kill by force using ssh, or kill locally and restart remote"""
    ...
