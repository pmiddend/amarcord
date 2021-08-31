from dataclasses import dataclass
from pathlib import Path
from typing import Dict
from typing import List
from typing import Union
from urllib.parse import parse_qs
from urllib.parse import urlparse

from amarcord.workflows.job_controller import JobController
from amarcord.workflows.local_job_controller import LocalJobController
from amarcord.workflows.slurm_rest_job_controller import SlurmRestJobController


@dataclass(frozen=True)
class LocalJobControllerConfig:
    baseDir: Path
    sqliteFile: Path


@dataclass(frozen=True)
class SlurmRestJobControllerConfig:
    baseDir: Path
    partition: str
    user_id: int
    jwtToken: str
    user: str
    url: str


def parse_job_controller(
    s: str,
) -> Union[LocalJobControllerConfig, SlurmRestJobControllerConfig]:
    jcc = urlparse(s)
    qs: Dict[str, List[str]] = parse_qs(jcc.query)

    def get_raise_missing(x: str) -> str:
        result: List[str] = qs.get(x, [])
        if not result:
            raise Exception(f"didn't find query string argument {x}, qs is {qs}")  # type: ignore
        if len(result) > 1:
            raise Exception(f"query string argument {x} appears more than once")  # type: ignore
        return result[0]

    if jcc.scheme == "local":
        p = Path(jcc.path)
        if not p.is_dir():
            raise Exception(
                f'local job controller base directory "{p}" doesn\'t exist or is not a directory'
            )

        return LocalJobControllerConfig(
            Path(jcc.path), Path(get_raise_missing("sqliteFile"))
        )
    if jcc.scheme == "slurmrest" or jcc.scheme == "slurmrestsecure":
        baseDir = get_raise_missing("baseDir")
        partition = get_raise_missing("partition")
        user_id = get_raise_missing("userId")
        jwtToken = get_raise_missing("jwtToken")
        user = get_raise_missing("user")
        output_scheme = "http" if jcc.scheme == "slurmrest" else "https"
        return SlurmRestJobControllerConfig(
            Path(baseDir),
            partition,
            int(user_id),
            jwtToken,
            user,
            f"{output_scheme}://{jcc.hostname}"
            + (":" + str(jcc.port) if jcc.port is not None else "")
            + jcc.path,
        )
    raise Exception(f"invalid job controller url (invalid scheme {jcc.scheme}): {s}")


def create_job_controller(
    config: Union[LocalJobControllerConfig, SlurmRestJobControllerConfig]
) -> JobController:
    if isinstance(config, LocalJobControllerConfig):
        return LocalJobController(config.sqliteFile, config.baseDir)
    return SlurmRestJobController(
        config.baseDir,
        config.partition,
        config.jwtToken,
        config.user_id,
        config.url,
        config.user,
    )
