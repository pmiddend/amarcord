import asyncio
import datetime
import re
import shlex
from pathlib import Path
from typing import Iterable

import structlog
from structlog.stdlib import BoundLogger

from amarcord.amici.petra3.beamline_metadata import BeamlineMetadata
from amarcord.amici.workload_manager.job import Job
from amarcord.amici.workload_manager.job import JobMetadata
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import JobStartResult
from amarcord.amici.workload_manager.workload_manager import WorkloadManager

logger = structlog.stdlib.get_logger(__name__)


def _ssh_command(
    bt_info: BeamlineMetadata, additional_options: bool = True
) -> list[str]:
    options: list[str] = ["/usr/bin/ssh"]
    if additional_options:
        options.extend(
            [
                inner
                for key, value in {
                    "BatchMode": "yes",
                    "CheckHostIP": "no",
                    "StrictHostKeyChecking": "no",
                    "GSSAPIAuthentication": "no",
                    "GSSAPIDelegateCredentials": "no",
                    "PasswordAuthentication": "no",
                    "PubkeyAuthentication": "yes",
                    "PreferredAuthentications": "publickey",
                    "IdentitiesOnly": "yes",
                    "ConnectTimeout": "10",
                }.items()
                for inner in ["-o", f"{key}={value}"]
            ]
        )
    if bt_info.onlineAnalysis.userAccount is not None:
        options.extend(["-l", str(bt_info.onlineAnalysis.userAccount)])
    if bt_info.onlineAnalysis.sshPrivateKeyPath:
        options.extend(["-i", str(bt_info.onlineAnalysis.sshPrivateKeyPath)])
    if bt_info.onlineAnalysis.reservedNodes:
        options.append(bt_info.onlineAnalysis.reservedNodes[0])
    else:
        options.append("max-display001.desy.de")
    return options


def _sbatch_command_prefix(
    bt_info: BeamlineMetadata,
    working_directory: Path,
    explicit_node: None | str,
    time: str,
    stdout: None | Path,
    stderr: None | Path,
) -> list[str]:
    options = [
        "/usr/bin/sbatch",
        f"--chdir={working_directory}",
        f"--time={time}",
    ]
    if explicit_node is not None:
        options.append(f"--nodelist={explicit_node}")
    if stdout is not None:
        options.append(f"--output={stdout}")
    if stderr is not None:
        options.append(f"--error={stderr}")
    if bt_info.onlineAnalysis.slurmPartition is not None:
        options.append(f"--partition={bt_info.onlineAnalysis.slurmPartition}")
    if bt_info.onlineAnalysis.slurmReservation is not None:
        options.append(f"--reservation={bt_info.onlineAnalysis.slurmReservation}")
    return options


def _squeue_command(user_account: str) -> list[str]:
    return ["/usr/bin/squeue", "--format=%A,%t,%V", f"--user={user_account}"]


def _parse_job_status(s: str) -> JobStatus:
    match s:
        case "CD":
            return JobStatus.SUCCESSFUL
        case "PF":
            return JobStatus.QUEUED
        case "PD" | "CF" | "CG" | "R" | "RD" | "RF" | "RH" | "RQ" | "RS" | "SI" | "SO":
            return JobStatus.RUNNING
        case _:
            return JobStatus.FAILED


def decode_job_list_result(decoded: str, stderr: bytes) -> list[Job]:
    # Ignore first line, it's the header
    segment_count = 3
    result: list[Job] = []
    for line in decoded.split("\n")[1:]:
        if not line.strip():
            continue
        parts = line.split(",", maxsplit=segment_count)
        if len(parts) < segment_count:
            raise Exception(
                f'error listing jobs, line "{line}" doesn\'t have {segment_count} segments! stderr is {stderr!r}'
            )
        try:
            job_id = int(parts[0])
        except:
            raise Exception(f"line {line}: job id {parts[0]} is not an integer")

        job_status = _parse_job_status(parts[1])
        date_format_spec = "%Y-%m-%dT%H:%M:%S"
        try:
            job_start = datetime.datetime.strptime(parts[2], date_format_spec)
        except:
            raise Exception(
                f'line {line}: date {parts[2]} doesn\'t conform to format spec "{date_format_spec}"'
            )
        result.append(Job(job_status, job_start, job_id, JobMetadata({})))
    return result


async def run_remote_list_jobs(
    parent_logger: BoundLogger,
    beamline_metadata: BeamlineMetadata,
    additional_options: bool,
) -> list[Job]:
    assert (
        beamline_metadata.onlineAnalysis.userAccount
    ), "Need a user account in the beamline metadata, but got none!"
    ssh_command_arg_list = _ssh_command(
        beamline_metadata, additional_options=additional_options
    )
    ssh_command_arg_list.extend(
        _squeue_command(beamline_metadata.onlineAnalysis.userAccount)
    )

    parent_logger.info("Executing " + str(ssh_command_arg_list))

    proc = await asyncio.create_subprocess_exec(
        ssh_command_arg_list[0],
        *ssh_command_arg_list[1:],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout_stream, stderr_stream = await proc.communicate()

    return decode_job_list_result(stdout_stream.decode("utf-8"), stderr_stream)


async def run_remote_sbatch(
    parent_logger: BoundLogger,
    beamline_metadata: BeamlineMetadata,
    working_directory: Path,
    explicit_node: None | str,
    stdout: None | Path,
    stderr: None | Path,
    remote_command_args: list[str],
    additional_ssh_options: bool,
) -> int:
    ssh_command_arg_list = _ssh_command(
        beamline_metadata, additional_options=additional_ssh_options
    )
    sbatch_command_arg_list = _sbatch_command_prefix(
        bt_info=beamline_metadata,
        working_directory=working_directory,
        time="1-0",
        explicit_node=explicit_node,
        stdout=stdout,
        stderr=stderr,
    )
    sbatch_command_arg_list.extend(remote_command_args)
    ssh_command_arg_list.extend(sbatch_command_arg_list)
    parent_logger.info("Executing " + str(ssh_command_arg_list))

    proc = await asyncio.create_subprocess_exec(
        ssh_command_arg_list[0],
        *ssh_command_arg_list[1:],
        stdout=asyncio.subprocess.PIPE,
        stderr=asyncio.subprocess.PIPE,
    )

    stdout_stream, stderr_stream = await proc.communicate()

    # completed = subprocess.run(ssh_command_arg_list, check=True, capture_output=True)
    parent_logger.info("completed")
    stdout_decoded = stdout_stream.decode()
    job_id_match = re.compile(r"Submitted batch job (\d+)\n").match(stdout_decoded)
    if job_id_match is None:
        raise Exception(
            f'unexpected output: {stdout_decoded}, expected something like "Submitted batch job blabla"; stderr is {stderr_stream!r}'
        )
    job_id_str = job_id_match.group(1)
    parent_logger.info(f"job ID is {job_id_str}")
    try:
        job_id = int(job_id_str)
    except:
        raise Exception(
            f'unexpected output (not an integer job ID): {stdout_decoded}, expected something like "Submitted batch job <integer>"; stderr is {stderr!r}'
        )
    return job_id


class SlurmRemoteWorkloadManager(WorkloadManager):
    def __init__(
        self,
        metadata: BeamlineMetadata,
        explicit_node: None | str,
        additional_ssh_options: bool,
    ) -> None:
        self._metadata = metadata
        self._additional_ssh_options = additional_ssh_options
        self._explicit_node = explicit_node

    async def list_jobs(self) -> Iterable[Job]:
        return await run_remote_list_jobs(
            logger, self._metadata, self._additional_ssh_options
        )

    async def start_job(
        self,
        working_directory: Path,
        executable: Path,
        command_line: str,
        time_limit: datetime.timedelta,
        stdout: None | Path = None,
        stderr: None | Path = None,
    ) -> JobStartResult:
        try:
            job_id = await run_remote_sbatch(
                parent_logger=logger,
                beamline_metadata=self._metadata,
                working_directory=working_directory,
                explicit_node=self._explicit_node,
                remote_command_args=[str(executable)] + shlex.split(command_line),
                stdout=stdout,
                stderr=stderr,
                additional_ssh_options=self._additional_ssh_options,
            )
            return JobStartResult(job_id, JobMetadata({"id": job_id}))
        except Exception as e:
            raise JobStartError(str(e))
