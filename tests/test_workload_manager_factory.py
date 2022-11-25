from pathlib import Path

import pytest

from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    MAXWELL_SLURM_URL,
)
from amarcord.amici.workload_manager.workload_manager_factory import (
    LocalWorkloadManagerConfig,
)
from amarcord.amici.workload_manager.workload_manager_factory import (
    RemotePetraSlurmWorkloadManagerConfig,
)
from amarcord.amici.workload_manager.workload_manager_factory import (
    SlurmRestWorkloadManagerConfig,
)
from amarcord.amici.workload_manager.workload_manager_factory import (
    parse_workload_manager_config,
)


@pytest.mark.parametrize(
    "input_type_and_value",
    [
        ("local:", LocalWorkloadManagerConfig()),
        ("shit:", None),
        ("maxwell-rest:", None),
        (
            "maxwell-rest:partition=foo|user=user",
            SlurmRestWorkloadManagerConfig(
                partition="foo",
                reservation=None,
                token=None,
                user="user",
                url=MAXWELL_SLURM_URL,
                portal_token=None,
            ),
        ),
        (
            "maxwell-rest:partition=foo|reservation=bar|user=user|token=blub",
            SlurmRestWorkloadManagerConfig(
                partition="foo",
                reservation="bar",
                token="blub",
                user="user",
                url=MAXWELL_SLURM_URL,
                portal_token=None,
            ),
        ),
        (
            "maxwell-rest:partition=foo|reservation=bar|user=user|portal-token=blub",
            SlurmRestWorkloadManagerConfig(
                partition="foo",
                reservation="bar",
                token=None,
                user="user",
                url=MAXWELL_SLURM_URL,
                portal_token="blub",
            ),
        ),
        (
            "slurm-rest:partition=foo|reservation=bar|user=user|token=blub|host=myhost|port=80|path=/foo",
            SlurmRestWorkloadManagerConfig(
                partition="foo",
                reservation="bar",
                token="blub",
                user="user",
                url="https://myhost:80/foo",
                portal_token=None,
            ),
        ),
        (
            "petra3-slurm-remote:beamtime-id=131232",
            RemotePetraSlurmWorkloadManagerConfig(
                beamtime_id_or_metadata_file="131232",
                explicit_node=None,
                additional_ssh_options=True,
            ),
        ),
        (
            "petra3-slurm-remote:path=/foo|explicit-node=a|use-additional-ssh-options=false",
            RemotePetraSlurmWorkloadManagerConfig(
                beamtime_id_or_metadata_file=Path("/foo"),
                explicit_node="a",
                additional_ssh_options=False,
            ),
        ),
    ],
)
def test_parse_workload_manager_config(
    input_type_and_value: tuple[
        str,
        None
        | LocalWorkloadManagerConfig
        | SlurmRestWorkloadManagerConfig
        | RemotePetraSlurmWorkloadManagerConfig,
    ]
) -> None:
    if input_type_and_value[1] is None:
        with pytest.raises(Exception):
            parse_workload_manager_config(input_type_and_value[0])
    else:
        result = parse_workload_manager_config(input_type_and_value[0])
        assert result == input_type_and_value[1]
