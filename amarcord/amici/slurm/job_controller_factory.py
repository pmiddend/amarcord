from dataclasses import dataclass
from getpass import getuser
from os import getuid
from urllib.parse import parse_qs
from urllib.parse import urlparse

from amarcord.amici.slurm.job_controller import JobController
from amarcord.amici.slurm.slurm_rest_job_controller import ConstantTokenRetriever
from amarcord.amici.slurm.slurm_rest_job_controller import DynamicTokenRetriever
from amarcord.amici.slurm.slurm_rest_job_controller import SlurmRestJobController
from amarcord.amici.slurm.slurm_rest_job_controller import TokenRetriever
from amarcord.amici.slurm.slurm_rest_job_controller import retrieve_jwt_token


@dataclass(frozen=True)
class LocalJobControllerConfig:
    pass


@dataclass(frozen=True)
class SlurmRestJobControllerConfig:
    partition: str
    user_id: int
    jwtToken: str | None
    tag: str | None
    user: str
    url: str


def parse_job_controller(
    s: str,
) -> LocalJobControllerConfig | SlurmRestJobControllerConfig:
    jcc = urlparse(s)
    qs: dict[str, list[str]] = parse_qs(jcc.query)

    def get_or_none(x: str) -> str | None:
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
        partition = get_raise_missing("partition")
        tag = get_or_none("tag")
        user_id = get_or("userId", str(getuid()))
        jwtToken = get_or_none("jwtToken")
        user = get_or("user", getuser())
        output_scheme = "http" if jcc.scheme == "slurmrest" else "https"
        return SlurmRestJobControllerConfig(
            partition,
            int(user_id),
            jwtToken,
            tag,
            user,
            f"{output_scheme}://{jcc.hostname}"
            + (":" + str(jcc.port) if jcc.port is not None else "")
            + jcc.path,
        )
    raise Exception(f"invalid job controller url (invalid scheme {jcc.scheme}): {s}")


def create_job_controller(
    config: LocalJobControllerConfig | SlurmRestJobControllerConfig,
) -> JobController:
    if isinstance(config, LocalJobControllerConfig):
        raise Exception("local job controller not supported right now")
    token_retriever: TokenRetriever
    if config.jwtToken is not None:
        token_retriever = ConstantTokenRetriever(config.jwtToken)
    else:
        token_retriever = DynamicTokenRetriever(retrieve_jwt_token)
    return SlurmRestJobController(
        partition=config.partition,
        token_retriever=token_retriever,
        user_id=config.user_id,
        rest_url=config.url,
        rest_user=config.user,
    )
