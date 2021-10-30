__author__ = "Bilal El Uneis"
__since__ = "October 2021"
__email__ = "bilaleluneis@gmail.com"

import json
from subprocess import Popen, DEVNULL
from typing import final, Any, Optional

from asyncssh import scp, connect
from distributed import Client as DaskClient
from distributed.deploy.ssh import SpecCluster, Worker
from unsync import unsync

from configurable.cluster.client import Client


# TODO:
# use work_space_path in remote workers


@final
class SSHCluster:

    def __init__(self, config_path: str, balanced: bool = True) -> None:
        with open(config_path) as config_:
            cluster_config: dict[str, Any] = json.load(config_)

        self.__req_modules: list[str] = cluster_config["required_modules"]
        self.__w_space_path: str = cluster_config["worker_space_path"]
        self.__rw_pypath: str = cluster_config["remote_workers_python_path"]
        self.__workers: dict[str, Any] = cluster_config["workers"]
        self.__remote_hosts = [(host, config["user"]) for host, config in self.__workers.items() if host != "localhost"]
        self.__local_workers: Optional[Popen] = None  # use to hold subprocess of local workers

        self.__scp_modules()  # copy required dependencies to remote workers
        self.__cluster: SpecCluster = SpecCluster(workers=self.__init_remote_workers())
        self.__scheduler_address = self.__cluster.scheduler_address

        if "localhost" in self.__workers.keys():
            self.__init_local_workers()

        self.__client: Client = Client(self.__cluster)
        # access to internal protected member, not the cleanest way.. but this is internal API anyway.
        self.__internal_dask_client: DaskClient = self.client.__dict__['_dsk_client']

        self.__is_balanced = balanced
        if balanced:
            self.__internal_dask_client.rebalance()

    def __enter__(self) -> Client:
        return self.__client

    # TODO: type hint params and provide handling hooks
    def __exit__(self, exc_type, exc_val, exc_tb) -> Optional[bool]:
        return self.shutdown()

    @property
    def client(self) -> Client:
        return self.__client

    def shutdown(self) -> None:
        if self.__local_workers is not None:
            self.__local_workers.kill()

        self.__internal_dask_client.shutdown()

        @unsync
        async def _():
            for host, user in self.__remote_hosts:
                async with connect(host=host, username=user) as conn:
                    await conn.run('pkill -9 python && pkill -9 python3')

        _().result()  # wait for conroutines to force kill python instances on remote nodes to finish

    def restart(self, balanced: Optional[bool] = None, refresh_modules: bool = False) -> None:
        if refresh_modules:
            self.__scp_modules()
        rebalance = self.__is_balanced if balanced is None else balanced
        if rebalance:
            self.__internal_dask_client.rebalance()

        self.__internal_dask_client.restart()

    def __init_remote_workers(self) -> dict[str, dict]:
        workers: dict[str, dict] = {}
        for worker_, config in self.__workers.items():
            if worker_ != 'localhost':
                for i in range(config["nprocs"]):
                    workers[f'{worker_}-{i}'] = {
                        "cls": Worker,
                        "options": {
                            "address": worker_,
                            "connect_options": {"username": config["user"]} if len(config["user"]) else {},
                            "kwargs": {"name": f'{worker_}-{i}',
                                       "nthreads": config["nthreads"],
                                       "memory_limit": config["mem_per_procs"]},
                            "remote_python": config["python_path"]
                        }
                    }
        return workers

    def __init_local_workers(self) -> None:
        config = self.__workers['localhost']
        py_path = f"{':'.join(self.__req_modules)}:$PYTHONPATH" if len(self.__req_modules) else "$PYTHONPATH"
        worker_module = f"{config['python_path']} -m distributed.cli.dask_worker {self.__scheduler_address}"
        worker_opts = f"--name localhost --nthreads {config['nthreads']} --nprocs {config['nprocs']}"
        worker_memory = f"--memory-limit {config['mem_per_procs']}"
        lh_workers_cli = " ".join([worker_module, worker_opts, worker_memory])
        kwargs = {
            'env': {"PYTHONPATH": py_path},
            'cwd': self.__w_space_path,
            'shell': True,
            'close_fds': True,
            'stderr': DEVNULL
        }
        self.__local_workers = Popen(lh_workers_cli, **kwargs)

    def __scp_modules(self) -> None:
        if not len(self.__req_modules):
            return

        @unsync
        async def _():
            for host, user in self.__remote_hosts:
                for module in self.__req_modules:
                    kwargs = {
                        'preserve': True,
                        'recurse': True,
                        'username': user
                    }
                    await scp(f'{module}/*', f'{host}:{self.__rw_pypath}', **kwargs)

        _().result()
