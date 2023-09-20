import datetime
from dataclasses import dataclass
from functools import partial
from getpass import getuser
from pathlib import Path

from amarcord.amici.petra3.beamline_metadata import locate_beamtime_metadata
from amarcord.amici.petra3.beamline_metadata import parse_beamline_metadata
from amarcord.amici.workload_manager.slurm_remote_workload_manager import (
    SlurmRemoteWorkloadManager,
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    MAXWELL_SLURM_URL,  # NOQA
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    ConstantTokenRetriever,
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    DynamicTokenRetriever,  # NOQA
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    SlurmRequestsHttpWrapper,
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    SlurmRestWorkloadManager,  # NOQA
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import TokenRetriever
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    retrieve_jwt_token_externally,
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    retrieve_jwt_token_on_maxwell_node,  # NOQA
)
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.simple_uri import parse_simple_uri


@dataclass(frozen=True, eq=True)
class LocalWorkloadManagerConfig:
    pass


@dataclass(frozen=True, eq=True)
class SlurmRestWorkloadManagerConfig:
    partition: str
    reservation: None | str
    explicit_node: None | str
    token: None | str
    portal_token: None | str
    user: str
    url: str


@dataclass(frozen=True, eq=True)
class RemotePetraSlurmWorkloadManagerConfig:
    beamtime_id_or_metadata_file: str | Path
    explicit_node: None | str
    additional_ssh_options: bool


def parse_workload_manager_config(
    s: str,
) -> LocalWorkloadManagerConfig | SlurmRestWorkloadManagerConfig | RemotePetraSlurmWorkloadManagerConfig:
    jcc = parse_simple_uri(s)

    if isinstance(jcc, str):
        raise Exception(f"invalid workload manager simple URI: {jcc}")

    match jcc.scheme:
        case "local":
            return LocalWorkloadManagerConfig()
        case "petra3-slurm-remote":
            beamtime_id = jcc.string_parameter("beamtime-id")
            beamtime_id_or_metadata_file = (
                beamtime_id if beamtime_id is not None else jcc.path_parameter("path")
            )
            if beamtime_id_or_metadata_file is None:
                raise Exception(
                    "invalid workload manager simple URI: got no beamtime ID and no metadata file"
                )
            additional_ssh_options = jcc.bool_parameter("use-additional-ssh-options")
            return RemotePetraSlurmWorkloadManagerConfig(
                beamtime_id_or_metadata_file=beamtime_id_or_metadata_file,
                additional_ssh_options=additional_ssh_options
                if additional_ssh_options is not None
                else True,
                explicit_node=jcc.string_parameter("explicit-node"),
            )
        case "maxwell-rest":
            partition = jcc.string_parameter("partition")
            if partition is None:
                raise Exception(
                    'invalid scheme for SLURM REST: "partition" is mandatory'
                )
            user = jcc.string_parameter("user")
            return SlurmRestWorkloadManagerConfig(
                partition=partition,
                reservation=jcc.string_parameter("reservation"),
                explicit_node=jcc.string_parameter("explicit-node"),
                token=jcc.string_parameter("token"),
                portal_token=jcc.string_parameter("portal-token"),
                user=user if user is not None else getuser(),
                url=MAXWELL_SLURM_URL,
            )
        case "slurm-rest":
            output_scheme = "http" if jcc.bool_parameter("use-http") else "https"
            partition = jcc.string_parameter("partition")
            if partition is None:
                raise Exception(
                    'invalid scheme for SLURM REST: "partition" is mandatory'
                )
            host = jcc.string_parameter("host")
            if host is None:
                raise Exception('invalid scheme for SLURM REST: "host" is mandatory')
            port = jcc.string_parameter("port")
            path = jcc.string_parameter("path")
            user = jcc.string_parameter("user")
            return SlurmRestWorkloadManagerConfig(
                partition=partition,
                reservation=jcc.string_parameter("reservation"),
                explicit_node=jcc.string_parameter("explicit-node"),
                token=jcc.string_parameter("token"),
                portal_token=None,
                user=user if user is not None else getuser(),
                url=f"{output_scheme}://{host}"
                + (f":{port}" if port is not None else "")
                + (path if path is not None else ""),
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
            partition=partition,
            reservation=reservation,
            url=rest_url,
            user=user,
            explicit_node=explicit_node,
        ):
            token_retriever: TokenRetriever
            if config.token is not None:
                token_retriever = ConstantTokenRetriever(config.token)
            elif config.portal_token is not None:
                token_retriever = DynamicTokenRetriever(
                    partial(
                        retrieve_jwt_token_externally, config.portal_token, config.user
                    )
                )
            else:
                token_retriever = DynamicTokenRetriever(
                    retrieve_jwt_token_on_maxwell_node
                )
            return SlurmRestWorkloadManager(
                partition=partition,
                reservation=reservation,
                explicit_node=explicit_node,
                token_retriever=token_retriever,
                request_wrapper=SlurmRequestsHttpWrapper(),
                rest_url=rest_url,
                rest_user=user,
            )
