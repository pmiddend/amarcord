from dataclasses import dataclass
from getpass import getuser
from os import getuid
from urllib.parse import parse_qs
from urllib.parse import urlparse

from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    ConstantTokenRetriever,  # NOQA
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    DynamicTokenRetriever,  # NOQA
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    SlurmRestWorkloadManager,  # NOQA
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import TokenRetriever
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    retrieve_jwt_token,  # NOQA
)
from amarcord.amici.workload_manager.workload_manager import WorkloadManager


@dataclass(frozen=True)
class LocalJobControllerConfig:
    pass


@dataclass(frozen=True)
class SlurmRestJobControllerConfig:
    partition: str
    reservation: None | str
    user_id: int
    token: None | str
    user: str
    url: str


def parse_workload_manager_uri(
    s: str,
) -> LocalJobControllerConfig | SlurmRestJobControllerConfig:
    jcc = urlparse(s)
    qs: dict[str, list[str]] = parse_qs(jcc.query)

    def get_or_none(x: str) -> None | str:
        result: list[str] = qs.get(x, [])
        if not result:
            return None
        if len(result) > 1:
            raise Exception(f"query string argument {x} appears more than once")
        return result[0]

    def get_or(x: str, default_: str) -> str:
        result: list[str] = qs.get(x, [])
        if not result:
            return default_
        if len(result) > 1:
            raise Exception(f"query string argument {x} appears more than once")
        return result[0]

    def get_raise_missing(x: str) -> str:
        result: list[str] = qs.get(x, [])
        if not result:
            raise Exception(f"didn't find query string argument {x}, qs is {qs}")
        if len(result) > 1:
            raise Exception(f"query string argument {x} appears more than once")
        return result[0]

    if jcc.scheme == "local":
        return LocalJobControllerConfig()
    if jcc.scheme in ("slurmrest", "slurmrestsecure"):
        output_scheme = "http" if jcc.scheme == "slurmrest" else "https"
        return SlurmRestJobControllerConfig(
            partition=get_raise_missing("partition"),
            reservation=get_or_none("reservation"),
            user_id=int(get_or("userId", str(getuid()))),
            token=get_or_none("slurmToken"),
            user=get_or("user", getuser()),
            url=f"{output_scheme}://{jcc.hostname}"
            + (":" + str(jcc.port) if jcc.port is not None else "")
            + jcc.path,
        )
    raise Exception(f"invalid job controller url (invalid scheme {jcc.scheme}): {s}")


def create_workload_manager(
    config: LocalJobControllerConfig | SlurmRestJobControllerConfig,
) -> WorkloadManager:
    if isinstance(config, LocalJobControllerConfig):
        raise Exception("local job controller not supported right now")
    token_retriever: TokenRetriever
    if config.token is not None:
        token_retriever = ConstantTokenRetriever(config.token)
    else:
        token_retriever = DynamicTokenRetriever(retrieve_jwt_token)
    return SlurmRestWorkloadManager(
        partition=config.partition,
        reservation=config.reservation,
        token_retriever=token_retriever,
        rest_url=config.url,
        rest_user=config.user,
    )
