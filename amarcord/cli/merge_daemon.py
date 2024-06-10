import asyncio
import datetime
import inspect
import json
import shlex
from base64 import b64encode
from dataclasses import dataclass
from datetime import timedelta
from enum import IntEnum
from enum import auto
from io import StringIO
from pathlib import Path
from time import time
from typing import Optional

import aiohttp
import structlog
from structlog.stdlib import BoundLogger
from tap import Tap

import amarcord.cli.crystfel_merge
from amarcord.amici.crystfel.util import coparse_cell_file
from amarcord.amici.crystfel.util import determine_output_directory
from amarcord.amici.crystfel.util import make_cell_file_name
from amarcord.amici.crystfel.util import parse_cell_description
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.amici.workload_manager.workload_manager_factory import (
    create_workload_manager,
)
from amarcord.amici.workload_manager.workload_manager_factory import (
    parse_workload_manager_config,
)
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.merge_result import JsonMergeJobFinishedInput
from amarcord.db.merge_result import JsonMergeJobStartedInput
from amarcord.db.scale_intensities import ScaleIntensities
from amarcord.web.json_models import JsonCreateFileOutput
from amarcord.web.json_models import JsonMergeJob
from amarcord.web.json_models import JsonMergeParameters
from amarcord.web.json_models import JsonReadMergeResultsOutput

logger = structlog.stdlib.get_logger(__name__)

_SHORT_SLEEP_DURATION_SECONDS = 2.0
_LONG_BREAK_DURATION_SECONDS = 10.0
_ZOMBIE_TIME_SECONDS = 10


class Arguments(Tap):
    workload_manager_uri: str
    output_base_directory: Path
    amarcord_url: str
    # pylint: disable=consider-alternative-union-syntax
    amarcord_url_for_maxwell_job: Optional[str] = None
    crystfel_path: Path


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
            f"--polarisation={p.polarisation.angle}deg{p.polarisation.percent}"
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


async def start_merge_job(
    session: aiohttp.ClientSession,
    parent_logger: BoundLogger,
    workload_manager: WorkloadManager,
    args: Arguments,
    merge_result: JsonMergeJob,
) -> MergeJobStartError | MergeJobStartSuccess:
    parent_logger.info(
        f"starting merge job, indexing results {[ir.id for ir in merge_result.indexing_results]}"
    )

    stream_files: list[str] = []
    for ir in merge_result.indexing_results:
        if ir.job_status != DBJobStatus.DONE:
            parent_logger.error(
                f"Indexing result {ir.id} is not finished yet! Status is {ir.job_status}"
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
    ), f"the merge result {merge_result.id} has no valid cell description: {merge_result.cell_description}"

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
        f"{args.amarcord_url}/api/files", data=post_file_data
    ) as cell_file_response:
        cell_file_id = JsonCreateFileOutput(**(await cell_file_response.json())).id
        parent_logger.info(f"cell file {cell_file_id} created")

    try:
        with Path(inspect.getfile(amarcord.cli.crystfel_merge)).open(
            "r", encoding="utf-8"
        ) as merge_file:
            predefined_args = {
                "stream-files": stream_files,
                "api-url": (
                    args.amarcord_url
                    if args.amarcord_url_for_maxwell_job is None
                    else args.amarcord_url_for_maxwell_job
                ),
                "merge-result-id": merge_result.id,
                "cell-file-id": cell_file_id,
                "point-group": merge_result.point_group,
                "partialator-additional": shlex.join(
                    merge_parameters_to_crystfel_parameters(merge_result.parameters)
                ),
                "crystfel-path": str(args.crystfel_path),
                "pdb-file-id": pdb_file_id,
                "restraints-cif-file-id": restraints_cif_file_id,
            }
            parent_logger.info(
                "command line for this job is "
                + " ".join(f"{k}={v}" for k, v in predefined_args.items())
            )
            predefined_args_b64 = b64encode(
                json.dumps(predefined_args, allow_nan=False).encode("utf-8")
            ).decode("utf-8")
            merge_file_contents = merge_file.read().replace(
                "predefined_args: None | bytes = None",
                f'predefined_args = "{predefined_args_b64}"',
            )
            beamtime = merge_result.indexing_results[0].beamtime
            job_base_directory = determine_output_directory(
                beamtime,
                args.output_base_directory,
                {},
            )
            job_start_result = await workload_manager.start_job(
                working_directory=job_base_directory,
                name=f"mg_{merge_result.id}",
                script=merge_file_contents,
                time_limit=timedelta(days=1),
                stdout=job_base_directory / f"{merge_result.id}_stdout.txt",
                stderr=job_base_directory / f"{merge_result.id}_stderr.txt",
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
            job_error=e.message, time=datetime.datetime.now(datetime.timezone.utc)
        )


class CheckResult(IntEnum):
    CHECK_ACTION = auto()
    CHECK_NO_ACTION = auto()


async def _start_new_jobs(
    session: aiohttp.ClientSession,
    workload_manager: WorkloadManager,
    args: Arguments,
) -> None:
    async with session.get(
        f"{args.amarcord_url}/api/merging?status={DBJobStatus.QUEUED.value}"
    ) as response:
        merge_results = JsonReadMergeResultsOutput(**await response.json()).merge_jobs

    for merge_result in merge_results:
        bound_logger = logger.bind(merge_result_id=merge_result.id)
        start_result = await start_merge_job(
            session, bound_logger, workload_manager, args, merge_result
        )

        if isinstance(start_result, MergeJobStartSuccess):
            async with session.post(
                f"{args.amarcord_url}/api/merging/start/{merge_result.id}",
                json=JsonMergeJobStartedInput(
                    job_id=start_result.job_id,
                    time=datetime_to_attributo_int(start_result.time),
                ).dict(),
            ) as start_response:
                bound_logger.info(
                    f"new merge job started request sent, result: {start_response}"
                )
        else:
            async with session.post(
                f"{args.amarcord_url}/api/merging/finish/{merge_result.id}",
                json=JsonMergeJobFinishedInput(
                    error=start_result.job_error, result=None
                ).dict(),
            ) as update_response:
                bound_logger.info(
                    f"merge job finished with error sent, result: {update_response}"
                )

        bound_logger.info(
            f"new merge job submitted, taking a {_LONG_BREAK_DURATION_SECONDS}s break"
        )
        await asyncio.sleep(_LONG_BREAK_DURATION_SECONDS)


async def _update_jobs(
    session: aiohttp.ClientSession,
    workload_manager: WorkloadManager,
    args: Arguments,
    zombie_job_times: dict[int, float],
) -> None:
    async with session.get(
        f"{args.amarcord_url}/api/merging?status={DBJobStatus.RUNNING.value}"
    ) as response:
        merge_results = JsonReadMergeResultsOutput(**await response.json()).merge_jobs

    jobs_on_workload_manager = {j.id: j for j in await workload_manager.list_jobs()}

    for merge_result in merge_results:
        assert merge_result.job_id is not None

        bound_logger = logger.bind(
            merge_job_id=merge_result.job_id, merge_result_id=merge_result.id
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
                f"job is zombie for {time_diff_s}s, waiting {_ZOMBIE_TIME_SECONDS} to declare this thing done"
            )
            continue

        if workload_job is None:
            bound_logger.info("finished because not in SLURM REST job list anymore")
            job_error = "Job has finished on SLURM (not in job list anymore), but delivered no results. You can try running it again, but most likely, this is due to a programming bug, so please contact the software people!"
        else:
            job_error = f"Job has finished on SLURM (status {workload_job.status.value}), but delivered no results. You can try running it again, but most likely, this is due to a programming bug, so please contact the software people!"
            bound_logger.info(
                f"finished because SLURM REST job status is {workload_job.status.value}"
            )

        async with session.post(
            f"{args.amarcord_url}/api/merging/finish/{merge_result.id}",
            json=JsonMergeJobFinishedInput(error=job_error, result=None).dict(),
        ):
            bound_logger.info("sent request to finish with error")

    logger.info("merge jobs stati updated, take a (longer) break")

    now = time()
    for job_id in list(zombie_job_times):
        if now - zombie_job_times[job_id] > _ZOMBIE_TIME_SECONDS:
            zombie_job_times.pop(job_id)


async def _merging_loop_iteration(
    session: aiohttp.ClientSession,
    workload_manager: WorkloadManager,
    args: Arguments,
    zombie_job_times: dict[int, float],
) -> None:
    await _start_new_jobs(session, workload_manager, args)
    await _update_jobs(session, workload_manager, args, zombie_job_times)

    await asyncio.sleep(_SHORT_SLEEP_DURATION_SECONDS)


async def _merging_loop(args: Arguments) -> None:
    logger.info("starting CrystFEL merging loop")
    workload_manager = create_workload_manager(
        parse_workload_manager_config(args.workload_manager_uri)
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
            await _merging_loop_iteration(
                session, workload_manager, args, zombie_job_times
            )


def main() -> None:
    asyncio.run(_merging_loop(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":
    main()
