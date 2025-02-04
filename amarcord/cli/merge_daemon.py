import asyncio
import datetime
import inspect
import os
import shlex
from dataclasses import dataclass
from datetime import timedelta
from io import StringIO
from pathlib import Path
from time import time

import aiohttp
import anyio
import structlog
from structlog.stdlib import BoundLogger
from tap import Tap

import amarcord.cli.crystfel_merge
from amarcord.amici.crystfel.util import determine_output_directory
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.amici.workload_manager.workload_manager_factory import (
    create_workload_manager,
)
from amarcord.amici.workload_manager.workload_manager_factory import (
    parse_workload_manager_config,
)
from amarcord.cli.crystfel_index import CrystFELCellFile
from amarcord.cli.crystfel_index import coparse_cell_file
from amarcord.cli.crystfel_index import parse_cell_description
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.merge_result import JsonMergeJobFinishedInput
from amarcord.db.merge_result import JsonMergeJobStartedInput
from amarcord.db.scale_intensities import ScaleIntensities
from amarcord.util import overwrite_interpreter
from amarcord.web.json_models import JsonCreateFileOutput
from amarcord.web.json_models import JsonMergeJob
from amarcord.web.json_models import JsonMergeParameters
from amarcord.web.json_models import JsonReadMergeResultsOutput

logger = structlog.stdlib.get_logger(__name__)

MERGE_DAEMON_LONG_BREAK_DURATION_SECONDS_ENV_VAR = (
    "MERGE_DAEMON_LONG_BREAK_DURATION_SECONDS"
)

MERGE_DAEMON_SHORT_BREAK_DURATION_SECONDS_ENV_VAR = (
    "MERGE_DAEMON_SHORT_BREAK_DURATION_SECONDS"
)


def _long_break_duration_seconds() -> float:
    return float(os.environ.get(MERGE_DAEMON_LONG_BREAK_DURATION_SECONDS_ENV_VAR, "10"))


def _short_break_duration_seconds() -> float:
    return float(
        os.environ.get(MERGE_DAEMON_SHORT_BREAK_DURATION_SECONDS_ENV_VAR, "10"),
    )


_ZOMBIE_TIME_SECONDS = 10


class Arguments(Tap):
    workload_manager_uri: str  # Determines how and where jobs are started; refer to the manual on how this URL should look like
    amarcord_url: str  # URL the daemon uses to look up indexing jobs in the DB
    ccp4_path: str | None = None
    overwrite_interpreter: str | None = None
    amarcord_url_for_spawned_job: str | None = None
    crystfel_path: (  # Where the CrystFEL binaries are located (without the /bin suffix!)
        Path
    )


def merge_parameters_to_crystfel_parameters(p: JsonMergeParameters) -> list[str]:
    assert p.point_group is not None
    result = [
        f"--symmetry={p.point_group}",
        f"--iterations={p.iterations}",
        f"--max-rel-B={p.rel_b}",
    ]
    if p.no_pr:
        result.append("--no-pr")
    if p.force_bandwidth is not None:
        result.append(f"--force-bandwidth={p.force_bandwidth}")
    if p.force_radius is not None:
        result.append(f"--force-radius={p.force_radius}")
    if p.force_lambda is not None:
        result.append(f"--force-lambda={p.force_lambda}")
    match p.scale_intensities:
        case ScaleIntensities.OFF:
            result.append("--no-scale")
        case ScaleIntensities.NORMAL:
            result.append("--no-Bscale")
        case ScaleIntensities.DEBYE_WALLER:
            pass
    if p.no_delta_cc_half:
        result.append("--no-deltacchalf")
    if p.start_after is not None:
        result.append(f"--start-after={p.start_after}")
    if p.stop_after is not None:
        result.append(f"--stop-after={p.stop_after}")
    result.append(f"--model={p.merge_model.value}")
    if p.polarisation is not None:
        result.append(
            f"--polarisation={p.polarisation.angle}deg{p.polarisation.percent}",
        )
    if p.max_adu is not None:
        result.append(f"--max-adu={p.max_adu}")
    if p.min_res is not None:
        result.append(f"--min-res={p.min_res}")
    result.append(f"--min-measurements={p.min_measurements}")
    if p.push_res is not None:
        result.append(f"--push-res={p.push_res}")
    if not p.logs:
        result.append("--no-logs")
    if p.w is not None:
        result.extend(["-w", p.w])
    return result


@dataclass(frozen=True)
class MergeJobStartError:
    job_error: str
    time: datetime.datetime


@dataclass(frozen=True)
class MergeJobStartSuccess:
    job_id: int
    time: datetime.datetime


def make_cell_file_name(c: CrystFELCellFile) -> str:
    ua = c.unique_axis if c.unique_axis else "noaxis"
    return f"chemical_{c.lattice_type}_{c.centering}_{ua}_{c.a}_{c.b}_{c.c}_{c.alpha}_{c.beta}_{c.gamma}_{int(time())}.cell"


async def start_merge_job(
    session: aiohttp.ClientSession,
    parent_logger: BoundLogger,
    workload_manager: WorkloadManager,
    args: Arguments,
    merge_result: JsonMergeJob,
) -> MergeJobStartError | MergeJobStartSuccess:
    parent_logger.info(
        f"starting merge job, indexing results {[ir.id for ir in merge_result.indexing_results]}",
    )

    stream_files: list[str] = []
    for ir in merge_result.indexing_results:
        if ir.job_status != DBJobStatus.DONE:
            parent_logger.error(
                f"Indexing result {ir.id} is not finished yet! Status is {ir.job_status}",
            )
            return MergeJobStartError(
                job_error=f"Indexing result {ir.id} is not finished yet! Status is {ir.job_status}",
                time=datetime.datetime.now(datetime.timezone.utc),
            )
        # The stream file could be None due to an error - skip this then
        if ir.stream_file is not None:
            stream_files.append(ir.stream_file)
    pdb_file_id: None | int = next(
        iter(
            f.id
            for f in merge_result.files_from_indexing
            if f.file_name.endswith(".pdb")
        ),
        None,
    )
    restraints_cif_file_id: None | int = next(
        iter(
            f.id
            for f in merge_result.files_from_indexing
            if f.file_name.endswith(".cif")
        ),
        None,
    )
    parent_logger.info("All indexing results have finished, we can start merging")

    assert (
        merge_result.cell_description is not None
    ), f"the merge result {merge_result.id} has no cell description"

    parsed_cell_description = parse_cell_description(merge_result.cell_description)

    assert (
        parsed_cell_description is not None
    ), f'the merge result {merge_result.id} has no valid cell description: "{merge_result.cell_description}"'

    cell_file_contents = StringIO()
    coparse_cell_file(parsed_cell_description, cell_file_contents)
    # Need to seek to the beginning again, otherwise we'll always read 0 bytes
    cell_file_contents.seek(0)

    post_file_data = aiohttp.FormData(quote_fields=False)
    post_file_data.add_field("description", "auto-generated cell file")
    post_file_data.add_field("deduplicate", str(True))
    post_file_data.add_field(
        "file",
        cell_file_contents,
        filename=make_cell_file_name(parsed_cell_description),
    )
    async with session.post(
        f"{args.amarcord_url}/api/files",
        data=post_file_data,
    ) as cell_file_response:
        cell_file_id = JsonCreateFileOutput(**(await cell_file_response.json())).id
        parent_logger.info(f"cell file {cell_file_id} created")

    try:
        async with await anyio.open_file(
            inspect.getfile(amarcord.cli.crystfel_merge),
            "r",
            encoding="utf-8",
        ) as merge_file:
            # mandatory parameters
            env: dict[str, str] = {
                amarcord.cli.crystfel_merge.MERGE_ENVIRON_STREAM_FILES: ",".join(
                    stream_files
                ),
                amarcord.cli.crystfel_merge.MERGE_ENVIRON_API_URL: (
                    args.amarcord_url
                    if args.amarcord_url_for_spawned_job is None
                    else args.amarcord_url_for_spawned_job
                ),
                amarcord.cli.crystfel_merge.MERGE_ENVIRON_MERGE_RESULT_ID: str(
                    merge_result.id
                ),
                amarcord.cli.crystfel_merge.MERGE_ENVIRON_CELL_FILE_ID: str(
                    cell_file_id
                ),
                amarcord.cli.crystfel_merge.MERGE_ENVIRON_POINT_GROUP: merge_result.point_group,
                amarcord.cli.crystfel_merge.MERGE_ENVIRON_PARTIALATOR_ADDITIONAL: shlex.join(
                    merge_parameters_to_crystfel_parameters(merge_result.parameters),
                ),
                amarcord.cli.crystfel_merge.MERGE_ENVIRON_CRYSTFEL_PATH: str(
                    args.crystfel_path
                ),
            }
            # optional parameters
            if args.ccp4_path is not None:
                env[amarcord.cli.crystfel_merge.MERGE_ENVIRON_CCP4_PATH] = (
                    args.ccp4_path
                )
            if pdb_file_id is not None:
                env[amarcord.cli.crystfel_merge.MERGE_ENVIRON_PDB_FILE_ID] = str(
                    pdb_file_id
                )
            if restraints_cif_file_id is not None:
                env[
                    amarcord.cli.crystfel_merge.MERGE_ENVIRON_RESTRAINTS_CIF_FILE_ID
                ] = str(restraints_cif_file_id)
            if merge_result.parameters.stop_after is not None:
                env[amarcord.cli.crystfel_merge.MERGE_ENVIRON_RANDOM_CUT_LENGTH] = str(
                    merge_result.parameters.stop_after
                )
            if merge_result.parameters.space_group is not None:
                env[amarcord.cli.crystfel_merge.MERGE_ENVIRON_SPACE_GROUP] = str(
                    merge_result.parameters.space_group
                )
            merge_file_contents = await merge_file.read()
            if args.overwrite_interpreter is not None:
                merge_file_contents = overwrite_interpreter(
                    merge_file_contents,
                    args.overwrite_interpreter,
                )
            beamtime = merge_result.indexing_results[0].beamtime
            job_base_directory = (
                determine_output_directory(
                    beamtime,
                    {},
                )
                / "merge-results"
            )
            job_start_result = await workload_manager.start_job(
                working_directory=job_base_directory,
                name=f"mg_{merge_result.id}",
                script=merge_file_contents,
                time_limit=timedelta(days=1),
                environment=env,
                stdout=job_base_directory / f"merging-{merge_result.id}-stdout.txt",
                stderr=job_base_directory / f"merging-{merge_result.id}-stderr.txt",
            )

        job_logger = parent_logger.bind(job_id=job_start_result.job_id)
        job_logger.info(f"job start successful, ID {job_start_result.job_id}")
        return MergeJobStartSuccess(
            job_id=job_start_result.job_id,
            time=datetime.datetime.now(datetime.timezone.utc),
        )
    except JobStartError as e:
        logger.error(f"job start errored: {e}")
        return MergeJobStartError(
            job_error=e.message,
            time=datetime.datetime.now(datetime.timezone.utc),
        )


async def _start_new_jobs(
    session: aiohttp.ClientSession,
    workload_manager: WorkloadManager,
    args: Arguments,
) -> None:
    async with session.get(
        f"{args.amarcord_url}/api/merging?status={DBJobStatus.QUEUED.value}",
    ) as response:
        merge_results = JsonReadMergeResultsOutput(**await response.json()).merge_jobs

    for merge_result in merge_results:
        bound_logger = logger.bind(merge_result_id=merge_result.id)
        start_result = await start_merge_job(
            session,
            bound_logger,
            workload_manager,
            args,
            merge_result,
        )

        if isinstance(start_result, MergeJobStartSuccess):
            async with session.post(
                f"{args.amarcord_url}/api/merging/{merge_result.id}/start",
                json=JsonMergeJobStartedInput(
                    job_id=start_result.job_id,
                    time=datetime_to_attributo_int(start_result.time),
                ).dict(),
            ) as start_response:
                if start_response.status // 200 != 1:
                    bound_logger.error(
                        f"error starting merge job: {start_response.status}",
                    )
                else:
                    bound_logger.info(
                        f"new merge job started request sent, result: {start_response}",
                    )
        else:
            async with session.post(
                f"{args.amarcord_url}/api/merging/{merge_result.id}/finish",
                json=JsonMergeJobFinishedInput(
                    error=start_result.job_error, result=None, latest_log=None
                ).dict(),
            ) as update_response:
                if update_response.status // 200 != 1:
                    bound_logger.error(
                        f"merge job finished erroneously: {update_response.status}",
                    )
                else:
                    bound_logger.info(
                        f"merge job finished with error sent, result: {update_response}",
                    )

        bound_logger.info(
            f"new merge job submitted, taking a {_long_break_duration_seconds()}s break",
        )
        await asyncio.sleep(_long_break_duration_seconds())


async def _update_jobs(
    session: aiohttp.ClientSession,
    workload_manager: WorkloadManager,
    args: Arguments,
    zombie_job_times: dict[int, float],
) -> None:
    async with session.get(
        f"{args.amarcord_url}/api/merging?status={DBJobStatus.RUNNING.value}",
    ) as response:
        merge_results = JsonReadMergeResultsOutput(**await response.json()).merge_jobs

    jobs_on_workload_manager = {j.id: j for j in await workload_manager.list_jobs()}

    for merge_result in merge_results:
        assert merge_result.job_id is not None

        bound_logger = logger.bind(
            merge_job_id=merge_result.job_id,
            merge_result_id=merge_result.id,
        )

        workload_job = jobs_on_workload_manager.get(merge_result.job_id)
        if workload_job is not None and workload_job.status not in (
            JobStatus.FAILED,
            JobStatus.SUCCESSFUL,
        ):
            # Running job, let it keep running
            continue

        job_first_seen = zombie_job_times.get(merge_result.job_id)
        if job_first_seen is None:
            bound_logger.info("job finished, marking as a zombie")
            zombie_job_times[merge_result.job_id] = time()
            continue

        time_diff_s = time() - job_first_seen
        if time_diff_s < _ZOMBIE_TIME_SECONDS:
            bound_logger.info(
                f"job is zombie for {time_diff_s}s, waiting {_ZOMBIE_TIME_SECONDS} to declare this thing done",
            )
            continue

        if workload_job is None:
            bound_logger.info("finished because not in SLURM REST job list anymore")
            job_error = "Job has finished (not in job list anymore), but delivered no results. You can try running it again, but most likely, this is due to a programming bug, so please contact the software people!"
        else:
            job_error = f"Job has finished (status {workload_job.status.value}), but delivered no results. You can try running it again, but most likely, this is due to a programming bug, so please contact the software people!"
            bound_logger.info(
                f"finished because SLURM REST job status is {workload_job.status.value}",
            )

        async with session.post(
            f"{args.amarcord_url}/api/merging/{merge_result.id}/finish",
            json=JsonMergeJobFinishedInput(
                error=job_error, result=None, latest_log=None
            ).dict(),
        ) as finish_request:
            if finish_request.status // 200 != 1:
                bound_logger.info(
                    f"sent request to finish with error, failed: {finish_request.status}",
                )
            else:
                bound_logger.error("sent request to finish with error")

    logger.info("merge jobs stati updated, take a (longer) break")

    now = time()
    for job_id in list(zombie_job_times):
        if now - zombie_job_times[job_id] > _ZOMBIE_TIME_SECONDS:
            zombie_job_times.pop(job_id)


async def merging_loop_iteration(
    session: aiohttp.ClientSession,
    workload_manager: WorkloadManager,
    args: Arguments,
    zombie_job_times: dict[int, float],
) -> None:
    await _start_new_jobs(session, workload_manager, args)
    await _update_jobs(session, workload_manager, args, zombie_job_times)

    await asyncio.sleep(_short_break_duration_seconds())


# We can't really cover this code, it's pure glue
async def _merging_loop(args: Arguments) -> None:  # pragma: no cover
    logger.info("starting CrystFEL merging loop")
    workload_manager = create_workload_manager(
        parse_workload_manager_config(args.workload_manager_uri),
    )
    # Why this? Well, uvicorn keeps closing the connection, although
    # we'd like to Keep-Alive it. See
    #
    # https://stackoverflow.com/questions/51248714/aiohttp-client-exception-serverdisconnectederror-is-this-the-api-servers-issu
    #
    # We could also just create a session over and over, but this seems a bit more clean
    connector = aiohttp.TCPConnector(force_close=True)
    zombie_job_times: dict[int, float] = {}
    async with aiohttp.ClientSession(connector=connector) as session:
        while True:
            await merging_loop_iteration(
                session,
                workload_manager,
                args,
                zombie_job_times,
            )


def main() -> None:  # pragma: no cover
    asyncio.run(_merging_loop(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":  # pragma: no cover
    main()
