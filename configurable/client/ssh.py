__author__ = "Bilal El Uneis"
__since__ = "October 2021"
__email__ = "bilaleluneis@gmail.com"

from typing import final, Callable, Any, Optional, Union, Iterable, Collection, Iterator

from distributed import Client as DaskClient, Future
from distributed.deploy.ssh import SpecCluster


@final
class Client:
    def __init__(self, cluster: SpecCluster) -> None:
        self._dsk_client: DaskClient = DaskClient(cluster)

    def submit(
            self,
            func: Callable,
            *args: Any,
            key: Optional[str] = None,
            workers: Optional[Union[str, Iterable[str]]] = None,
            resources: Optional[dict[str, Any]] = None,
            retries: int = 0,
            priority: int = 0,
            fifo_timeout: str = "100 ms",
            allow_other_workers: bool = False,
            actor: bool = False,
            actors: bool = False,
            pure: bool = False,
            **kwargs
    ) -> Future:
        return self._dsk_client.submit(
            func,
            args,
            key,
            workers,
            resources,
            retries,
            priority,
            fifo_timeout,
            allow_other_workers,
            actor,
            actors,
            pure,
            **kwargs)

    def map(
            self,
            func: Callable,
            *iterables,
            key: Optional[Union[str, list[str]]] = None,
            workers: Optional[Union[str, Iterable[str]]] = None,
            retries: int = 0,
            resources: Optional[dict[str, Any]] = None,
            priority: int = 0,
            allow_other_workers: bool = False,
            fifo_timeout: str = "100 ms",
            actor: bool = False,
            actors: bool = False,
            pure: bool = True,
            batch_size: int = 0,
            **kwargs
    ) -> Collection[Future]:
        return self._dsk_client.map(
            func,
            *iterables,
            key,
            workers,
            retries,
            resources,
            priority,
            allow_other_workers,
            fifo_timeout,
            actor,
            actors,
            pure,
            batch_size,
            **kwargs)

    def scatter(
            self,
            data: Union[Any, list, dict],
            workers: Optional[list[tuple[str, int]]] = None,
            broadcast: bool = False,
            direct: Optional[bool] = None,
            hash_: bool = True,
            timeout: str = "__no_default__",
            asynchronous: bool = False
    ) -> Optional[Future]:
        return self._dsk_client.scatter(
            data,
            workers,
            broadcast,
            direct,
            hash_,
            timeout,
            asynchronous)

    def gather(self,
               futures: Iterable[Future],
               errors: str = "raise",
               direct: bool = False,
               asynchronous: bool = False) -> Union[list, Iterator[Any], Collection[Any]]:
        return self._dsk_client.gather(futures, errors, direct, asynchronous)
