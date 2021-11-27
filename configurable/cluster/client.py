__author__ = "Bilal El Uneis"
__since__ = "October 2021"
__email__ = "bilaleluneis@gmail.com"

from typing import final, Callable, Any, Optional, Union, Iterable

from distributed import Client as DaskClient, Future
from distributed.deploy.ssh import SpecCluster


@final
class Client:
    def __init__(self, cluster: SpecCluster) -> None:
        self._dsk_client: DaskClient = DaskClient(cluster)

    @property
    def workers_count(self) -> int:
        return len(self._dsk_client.scheduler_info()['workers'])

    def submit(self, func: Callable, *func_args: Any, **func_kwargs: Any) -> Future:
        return self._dsk_client.submit(func, *func_args, **func_kwargs)

    def map(self, func: Callable, *iterable: Any, **func_kwargs: Any) -> list[Future]:
        return self._dsk_client.map(func, *iterable, **func_kwargs)

    def scatter(self, data: Union[Any, list, dict]) -> Optional[Future]:
        return self._dsk_client.scatter(data)

    def gather(self, futures: Iterable[Future]) -> list[Any]:
        return self._dsk_client.gather(futures)
