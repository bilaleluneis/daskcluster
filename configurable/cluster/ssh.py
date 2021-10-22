__author__ = "Bilal El Uneis"
__since__ = "October 2021"
__email__ = "bilaleluneis@gmail.com"

import json
from subprocess import Popen, DEVNULL
from typing import final, Any, Optional

from asyncssh import scp
from distributed import Client as DaskClient
from distributed.deploy.ssh import Scheduler, Worker, SpecCluster
from unsync import unsync

from configurable.client.ssh import Client


# TODO:
# expose config as private props and conv static methods to instance methods
# - log when transfering files.
# - add method to restart remote machines on shutdown.
# - add instance methods [start, shutdown , restart(reload-config=False)]
# - make class work with context manager __enter__() __exit__()

@final
class SSHCluster:

    def __init__(self, config_path: str) -> None:
        with open(config_path) as config_:
            cluster_config: dict[str, Any] = json.load(config_)
        req_modules: list[str] = cluster_config["required_modules"]
        w_space_path: str = cluster_config["worker_space_path"]
        rw_pypath: str = cluster_config["remote_workers_python_path"]
        s_config: dict[str, Any] = cluster_config["scheduler"]
        w_config: dict[str, Any] = cluster_config["workers"]
        self.__scp_modules(w_config, req_modules, rw_pypath).result()
        scheduler_ = self.__init_scheduler(s_config)
        r_workers = self.__init_remote_workers(w_config)
        self.__cluster: SpecCluster = SpecCluster(workers=r_workers, scheduler=scheduler_)
        scheduler_address = self.__cluster.scheduler_address
        self.__local_workers: Optional[Popen] = None  # us to hold subprocess of local workers
        if "localhost" in w_config.keys():
            self.__init_local_workers(w_config["localhost"], req_modules, w_space_path, scheduler_address)

        self.__client: Client = Client(self.__cluster)

    @staticmethod
    def __init_scheduler(s_config: dict[str, Any]) -> dict[str, Any]:
        return {
            "cls": Scheduler,
            "options": {
                "address": s_config['hostname'],
                "connect_options": {},
                "kwargs": {},  # scheduler options
                "remote_python": s_config['python_path']
            }
        }

    @staticmethod
    def __init_remote_workers(w_config: dict[str, Any]) -> dict[str, Any]:
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
            } for worker_, config in w_config.items() if worker_ != "localhost"
        }

    def __init_local_workers(self, worker: dict[str, Any], modules: list[str], w_s_pth: str, s_addrs: str) -> None:
        py_path = f"{':'.join(modules)}:$PYTHONPATH" if len(modules) else "$PYTHONPATH"
        worker_module = f"{worker['python_path']} -m distributed.cli.dask_worker {s_addrs}"
        worker_opts = f"--name localhost --nthreads {worker['nthreads']} --nprocs {worker['nprocs']}"
        worker_memory = f"--memory-limit {worker['mem_per_procs']}"
        lh_workers_cli = " ".join([worker_module, worker_opts, worker_memory])
        env = {"PYTHONPATH": py_path}
        self.__local_workers = Popen(lh_workers_cli, env=env, cwd=w_s_pth, shell=True, close_fds=True, stderr=DEVNULL)

    @staticmethod
    @unsync
    async def __scp_modules(workers: dict[str, Any], req_modules: list[str], pypath: str) -> None:
        remote_hosts = [(host, prop["user"]) for host, prop in workers.items() if host != "localhost"]
        for host, user in remote_hosts:
            for module in req_modules:
                await scp(f'{module}/*', f'{host}:{pypath}', preserve=True, recurse=True, username=user)

    @property
    def client(self) -> Client:
        return self.__client

    # TODO: restart remote machines and wait before returning
    def shutdown(self) -> None:
        if self.__local_workers is not None:
            self.__local_workers.kill()
        # access to internal protected member, not the cleanest way.. but this is internal API anyway.
        c: DaskClient = self.client.__dict__['_dsk_client']
        c.shutdown()
