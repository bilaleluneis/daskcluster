__author__ = "Bilal El Uneis"
__since__ = "November 2021"
__email__ = "bilaleluneis@gmail.com"

from itertools import islice

from dask import bag, delayed
from dask.bag import Bag
from dask.bag.core import split
from dask.delayed import Delayed
from distributed import Future

from configurable.cluster.client import Client


def msort_bag_accumlate(array: list[int], num_workers: int) -> list[int]:
    _ = split(array, num_workers)
    return bag.from_sequence(_).map(merge_sort).accumulate(merge, initial=list()).compute()[-1]


def msort_bag_fold(array: list[int], chunk_size: int) -> list[int]:
    _ = [array[i:i + chunk_size] for i in range(0, len(array), chunk_size)]
    m_sort: Bag = bag.from_sequence(_, npartitions=10).map(merge_sort).persist()
    return m_sort.fold(merge, initial=list()).compute()


def msort_delayed(array: list[int], num_workers: int) -> list[int]:
    split_array = split(array, num_workers)
    _ = delayed(split_array, nout=len(split_array)).persist()
    m_sort_op: tuple[Delayed, ...] = tuple(delayed(merge_sort)(i) for i in _)
    m_op = delayed(m_sort_op, nout=len(m_sort_op)).persist()
    while len(m_op) > 1:
        m_op = tuple(delayed(merge)(m_op[i], m_op[i + 1])
                     if i + 1 < len(m_op)
                     else delayed(merge)(m_op[i], delayed(list)())
                     for i in range(0, len(m_op), 2))
    return m_op[0].compute(optimize_graph=True)


def msort_futures(client: Client, array: list[int], num_workers: int) -> list[int]:
    m_sort_op: list[Future] = client.map(merge_sort, split(array, num_workers))
    m_sort_result: list[list[int]] = client.gather(m_sort_op)
    m_op: list[Future] = client.map(lambda i: merge(*i), __normalize(m_sort_result))
    while len(m_op) > 1:
        m_op = client.map(lambda i: merge(*i), __normalize(client.gather(m_op)))
    return m_op.pop().result()


def msort_futures_recursive(client: Client, array: list[int]) -> list[int]:
    def _(client_: Client, array_: list[int]) -> Future:
        if len(array_) < 2:
            return client.submit(list, array_)
        midpoint = len(array_) // 2
        left = _(client_, array_[:midpoint])
        right = _(client_, array_[midpoint:])
        return client_.submit(merge, left, right)

    return _(client, array).result()


def __normalize(list_: list[list]) -> list[tuple]:
    if len(list_) % 2:
        list_.append([])
    iter_ = iter(list_)
    return list(zip(iter_, iter_))


def to_list(array: list[int], num_workers: int) -> list[list[int]]:
    it = iter(array)
    _ = iter(lambda: list(islice(it, len(array) // num_workers)), [])
    return list(_)


def to_bag(array: list[int], num_workers: int) -> Bag:
    it = iter(array)
    _ = iter(lambda: list(islice(it, len(array) // num_workers)), [])
    return bag.from_sequence(_, npartitions=len(array) // num_workers)


"""
original merge_sort() from https://realpython.com/sorting-algorithms-python/#the-merge-sort-algorithm-in-python
"""


def merge_sort(array: list[int]) -> list[int]:
    # If the input array contains fewer than two elements,
    # then return it as the result of the function
    if len(array) < 2:
        return array

    midpoint = len(array) // 2

    # Sort the array by recursively splitting the input
    # into two equal halves, sorting each half and merging them
    # together into the final result
    return merge(left=merge_sort(array[:midpoint]), right=merge_sort(array[midpoint:]))


def merge(left: list[int], right: list[int]) -> list[int]:
    # If the first array is empty, then nothing needs
    # to be merged, and you can return the second array as the result
    if len(left) == 0:
        return right

    # If the second array is empty, then nothing needs
    # to be merged, and you can return the first array as the result
    if len(right) == 0:
        return left

    result: list[int] = []
    index_left = index_right = 0

    # Now go through both arrays until all the elements
    # make it into the resultant array
    while len(result) < len(left) + len(right):
        # The elements need to be sorted to add them to the
        # resultant array, so you need to decide whether to get
        # the next element from the first or the second array
        if left[index_left] <= right[index_right]:
            result.append(left[index_left])
            index_left += 1
        else:
            result.append(right[index_right])
            index_right += 1

        # If you reach the end of either array, then you can
        # add the remaining elements from the other array to
        # the result and break the loop
        if index_right == len(right):
            result += left[index_left:]
            break

        if index_left == len(left):
            result += right[index_right:]
            break

    return result
