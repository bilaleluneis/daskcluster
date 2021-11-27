__author__ = "Bilal El Uneis"
__since__ = "November 2021"
__email__ = "bilaleluneis@gmail.com"

from random import sample
from typing import Final

import pytest

from configurable.cluster.ssh import SSHCluster as Cluster
from . import sort

SIZE: Final[int] = 5


@pytest.fixture
def list_to_sort() -> list[int]:
    sample_size: Final[int] = 10 ** SIZE
    return sample(range(sample_size), sample_size)


@pytest.fixture
def control(list_to_sort) -> list[int]:
    return sorted(list_to_sort)


def test_cluster_msort(list_to_sort, control):
    with Cluster('cluster_configs/multi-node.json'):
        result = sort.msort_bag_fold(list_to_sort, 5000)
    assert len(result) == len(list_to_sort)
    assert result == control
