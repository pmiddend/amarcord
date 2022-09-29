import datetime
from dataclasses import dataclass
from getpass import getuser
from pathlib import Path
from urllib.parse import parse_qs
from urllib.parse import urlparse

from amarcord.amici.petra3.beamline_metadata import locate_beamtime_metadata
from amarcord.amici.petra3.beamline_metadata import parse_beamline_metadata
from amarcord.amici.workload_manager.slurm_remote_workload_manager import (
    SlurmRemoteWorkloadManager,
)
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
class LocalWorkloadManagerConfig:
    pass


@dataclass(frozen=True)
class SlurmRestWorkloadManagerConfig:
    partition: str
    reservation: None | str
    token: None | str
    user: str
    url: str


@dataclass(frozen=True)
class RemotePetraSlurmWorkloadManagerConfig:
    beamtime_id_or_metadata_file: str | Path
    explicit_node: None | str
    additional_ssh_options: bool


def parse_workload_manager_uri(
    s: str,
) -> LocalWorkloadManagerConfig | SlurmRestWorkloadManagerConfig | RemotePetraSlurmWorkloadManagerConfig:
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

    match jcc.scheme:
        case "local":
            return LocalWorkloadManagerConfig()
        case "petra3slurmremote":
            # petra3slurmremote://?beamtime-id=11010000
            # petra3slurmremote:////tmp/metadata.json
            beamtime_id = get_or_none("beamtime-id")
            return RemotePetraSlurmWorkloadManagerConfig(
                beamtime_id_or_metadata_file=beamtime_id
                if beamtime_id is not None
                else Path(jcc.path),
                additional_ssh_options=get_or_none("use-additional-ssh-options") == "1",
                explicit_node=get_or_none("explicit-node"),
            )
        case "slurmrest" | "slurmrestsecure":
            # slurmrestsecure://max-portal.desy.de/sapi/slurm/v0.0.36?token=sdflskdfjsdlfsjd&user=pmidden
            output_scheme = "http" if jcc.scheme == "slurmrest" else "https"
            return SlurmRestWorkloadManagerConfig(
                partition=get_raise_missing("partition"),
                reservation=get_or_none("reservation"),
                token=get_or_none("slurmToken"),
                user=get_or("user", getuser()),
                url=f"{output_scheme}://{jcc.hostname}"
                + (":" + str(jcc.port) if jcc.port is not None else "")
                + jcc.path,
            )
        case _:
            raise Exception(
                f"invalid workload manager url (invalid scheme {jcc.scheme}): {s}"
            )


def create_workload_manager(
    config: LocalWorkloadManagerConfig
    | SlurmRestWorkloadManagerConfig
    | RemotePetraSlurmWorkloadManagerConfig,
) -> WorkloadManager:
    match config:
        case RemotePetraSlurmWorkloadManagerConfig(
            beamtime_id_or_metadata_file,
            additional_ssh_options=additional_ssh_options,
            explicit_node=explicit_node,
        ):
            if isinstance(beamtime_id_or_metadata_file, str):
                metadata = locate_beamtime_metadata(
                    beamtime_id_or_metadata_file,
                    beamline="p11",
                    year=datetime.datetime.now().year,
                )
                if metadata is None:
                    raise Exception(
                        f'couldn\'t locate beamline metadata for beamtime ID "{beamtime_id_or_metadata_file}"'
                    )
            else:
                metadata = parse_beamline_metadata(beamtime_id_or_metadata_file)
            return SlurmRemoteWorkloadManager(
                metadata,
                additional_ssh_options=additional_ssh_options,
                explicit_node=explicit_node,
            )
        case LocalWorkloadManagerConfig():
            raise Exception("local workload manager not supported right now")
        case SlurmRestWorkloadManagerConfig(
            partition=partition, reservation=reservation, url=rest_url, user=user
        ):
            token_retriever: TokenRetriever
            if config.token is not None:
                token_retriever = ConstantTokenRetriever(config.token)
            else:
                token_retriever = DynamicTokenRetriever(retrieve_jwt_token)
            return SlurmRestWorkloadManager(
                partition=partition,
                reservation=reservation,
                token_retriever=token_retriever,
                rest_url=rest_url,
                rest_user=user,
            )
