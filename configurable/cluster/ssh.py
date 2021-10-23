__author__ = "Bilal El Uneis"
__since__ = "October 2021"
__email__ = "bilaleluneis@gmail.com"

import json
from subprocess import Popen, DEVNULL
from typing import final, Any, Optional

from asyncssh import scp, connect
from distributed import Client as DaskClient
from distributed.deploy.ssh import Scheduler, Worker, SpecCluster
from unsync import unsync

from configurable.client.ssh import Client


# TODO:
# - log when transfering files.
# - add instance methods [start, shutdown , restart(reload-config=False)]
# - make class work with context manager __enter__() __exit__()

@final
class SSHCluster:

    def __init__(self, config_path: str) -> None:
        with open(config_path) as config_:
            cluster_config: dict[str, Any] = json.load(config_)

        self.__req_modules: list[str] = cluster_config["required_modules"]
        self.__w_space_path: str = cluster_config["worker_space_path"]
        self.__rw_pypath: str = cluster_config["remote_workers_python_path"]
        self.__scheduler: dict[str, Any] = cluster_config["scheduler"]
        self.__workers: dict[str, Any] = cluster_config["workers"]
        self.__remote_hosts = [(host, config["user"]) for host, config in self.__workers.items() if host != "localhost"]
        self.__local_workers: Optional[Popen] = None  # us to hold subprocess of local workers

        self.__scp_modules().result()  # copy required modules to remote workers
        scheduler = self.__init_scheduler()
        r_workers = self.__init_remote_workers()
        self.__cluster: SpecCluster = SpecCluster(workers=r_workers, scheduler=scheduler)
        self.__scheduler_address = self.__cluster.scheduler_address

        if "localhost" in self.__workers.keys():
            self.__init_local_workers()

        self.__client: Client = Client(self.__cluster)

    @property
    def client(self) -> Client:
        return self.__client

    def shutdown(self) -> None:
        if self.__local_workers is not None:
            self.__local_workers.kill()
        # access to internal protected member, not the cleanest way.. but this is internal API anyway.
        c: DaskClient = self.client.__dict__['_dsk_client']
        c.shutdown()

        @unsync
        async def _():
            for host, user in self.__remote_hosts:
                async with connect(host=host, username=user) as conn:
                    await conn.run('pkill -9 python && pkill -9 python3')

        _().result()  # force kill python instances on remote nodes

    def __init_scheduler(self) -> dict[str, Any]:
        return {
            "cls": Scheduler,
            "options": {
                "address": self.__scheduler['hostname'],
                "connect_options": {},
                "kwargs": {},  # scheduler options
                "remote_python": self.__scheduler['python_path']
            }
        }

    def __init_remote_workers(self) -> dict[str, Any]:
        return {
            worker_: {
                "cls": Worker,
                "options": {
                    "address": worker_,
                    "connect_options": {"username": config["user"]} if len(config["user"]) else {},
                    "kwargs": {"name": worker_,
                               "nprocs": config["nprocs"],
                               "nthreads": config["nthreads"],
                               "memory_limit": config["mem_per_procs"]},
                    "worker_module": "distributed.cli.dask_worker",
                    "remote_python": config["python_path"]
                }
            } for worker_, config in self.__workers.items() if worker_ != "localhost"
        }

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

    @unsync
    async def __scp_modules(self) -> None:
        for host, user in self.__remote_hosts:
            for module in self.__req_modules:
                await scp(f'{module}/*', f'{host}:{self.__rw_pypath}', preserve=True, recurse=True, username=user)
