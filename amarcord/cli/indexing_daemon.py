import asyncio
import inspect
import json
from base64 import b64encode
from datetime import timedelta
from pathlib import Path
from typing import Optional

import aiohttp
import structlog
from structlog.stdlib import BoundLogger
from tap import Tap

import amarcord.cli.crystfel_index
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
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.indexing_result import DBIndexingResultRuntimeStatus
from amarcord.db.indexing_result import empty_indexing_fom
from amarcord.web.json_models import JsonIndexingJob
from amarcord.web.json_models import JsonIndexingResultRootJson
from amarcord.web.json_models import JsonReadIndexingResultsOutput
from amarcord.web.json_models import empty_json_indexing_result

logger = structlog.stdlib.get_logger(__name__)

_LONG_BREAK_DURATION_SECONDS = 5


class Arguments(Tap):
    amarcord_url: str
    # pylint: disable=consider-alternative-union-syntax
    amarcord_url_for_maxwell_job: Optional[str] = None
    output_base_directory: Path
    crystfel_path: Path
    use_auto_geom_refinement: bool = False
    # pylint: disable=consider-alternative-union-syntax
    dummy_h5_input: Optional[str] = None
    # pylint: disable=consider-alternative-union-syntax
    beamtime_id: Optional[int] = None
    workload_manager_uri: str
    asapo_source: str
    # pylint: disable=consider-alternative-union-syntax
    cpu_count_multiplier: Optional[float] = None


async def start_indexing_job(
    bound_logger: BoundLogger,
    workload_manager: WorkloadManager,
    args: Arguments,
    indexing_result: JsonIndexingJob,
) -> DBIndexingResultRuntimeStatus:
    bound_logger.info("starting indexing job")

    job_base_directory = determine_output_directory(
        indexing_result.beamtime, args.output_base_directory, {}
    )

    output_base_name = (
        f"run-{indexing_result.run_external_id}-indexing-{indexing_result.id}"
    )
    stream_file = job_base_directory / f"{output_base_name}.stream"

    try:
        with Path(inspect.getfile(amarcord.cli.crystfel_index)).open(
            "r", encoding="utf-8"
        ) as merge_file:
            predefined_args = {
                # We could give CrystFEL the internal ID as well, and
                # it wouldn't matter (the run ID isn't really used in
                # the logic of the indexing script, just for output),
                # but the user expects the external, beamtime-specific
                # one.
                "run-id": indexing_result.run_external_id,
                "job-id": indexing_result.id,
                "asapo-source": args.asapo_source,
                "cpu-count-multiplier": (
                    args.cpu_count_multiplier if args.cpu_count_multiplier else 0.5
                ),
                "api-url": (
                    args.amarcord_url
                    if args.amarcord_url_for_maxwell_job is None
                    else args.amarcord_url_for_maxwell_job
                ),
                "stream-file": str(stream_file),
                "dummy-h5-input": args.dummy_h5_input,
                "crystfel-path": str(args.crystfel_path),
                "use-auto-geom-refinement": args.use_auto_geom_refinement,
                "cell-description": indexing_result.cell_description,
            }
            bound_logger.info(
                "command line for this job is " + " ".join(predefined_args)
            )
            predefined_args_b64 = b64encode(
                json.dumps(predefined_args, allow_nan=False).encode("utf-8")
            ).decode("utf-8")
            indexing_file_contents = merge_file.read().replace(
                "predefined_args: None | bytes = None",
                f'predefined_args = "{predefined_args_b64}"',
            )
            job_start_result = await workload_manager.start_job(
                working_directory=job_base_directory,
                name=f"ix_run_{indexing_result.run_external_id}",
                script=indexing_file_contents,
                time_limit=timedelta(days=1),
                stdout=job_base_directory / f"{output_base_name}-stdout.txt",
                stderr=job_base_directory / f"{output_base_name}-stderr.txt",
            )
            bound_logger.info(
                "job start successful", indexing_job_id=job_start_result.job_id
            )
            return DBIndexingResultRunning(
                stream_file=stream_file,
                job_id=job_start_result.job_id,
                fom=empty_indexing_fom,
            )
    except JobStartError as e:
        bound_logger.error(f"job start errored: {e}")
        return DBIndexingResultDone(
            stream_file=stream_file,
            job_error=e.message,
            fom=empty_indexing_fom,
        )


async def _start_new_jobs(
    session: aiohttp.ClientSession, workload_manager: WorkloadManager, args: Arguments
) -> None:
    async with session.get(
        f"{args.amarcord_url}/api/indexing?status={DBJobStatus.QUEUED.value}"
        + (f"&beamtimeId={args.beamtime_id}" if args.beamtime_id is not None else "")
    ) as response:
        indexing_results = JsonReadIndexingResultsOutput(
            **await response.json()
        ).indexing_jobs

    number_of_started_jobs = 0
    for indexing_result in indexing_results:
        bound_logger = logger.bind(
            run_internal_id=indexing_result.run_internal_id,
            run_external_id=indexing_result.run_external_id,
            indexing_result_id=indexing_result.id,
        )
        new_status = await start_indexing_job(
            bound_logger, workload_manager, args, indexing_result
        )
        number_of_started_jobs += 1
        assert new_status is not None

        if isinstance(new_status, DBIndexingResultDone):
            update_request = JsonIndexingResultRootJson(
                error=new_status.job_error,
                job_id=None,
                stream_file=str(new_status.stream_file),
                result=empty_json_indexing_result(done=True),
            )
        else:
            update_request = JsonIndexingResultRootJson(
                error=None,
                job_id=new_status.job_id,
                stream_file=str(new_status.stream_file),
                result=empty_json_indexing_result(done=False),
            )

        async with session.post(
            f"{args.amarcord_url}/api/indexing/{indexing_result.id}",
            json=update_request.dict(),
        ) as update_response:
            bound_logger.info(f"new indexing job started, result: {update_response}")

        bound_logger.info(
            f"new indexing job submitted, taking a {_LONG_BREAK_DURATION_SECONDS}s break"
        )
        await asyncio.sleep(_LONG_BREAK_DURATION_SECONDS)
    if number_of_started_jobs == 0:
        logger.info(
            f"no new queued jobs, waiting for {_LONG_BREAK_DURATION_SECONDS}s until next iteration"
        )
        await asyncio.sleep(_LONG_BREAK_DURATION_SECONDS)


async def _update_jobs(
    session: aiohttp.ClientSession,
    workload_manager: WorkloadManager,
    amarcord_url: str,
    beamtime_id: None | BeamtimeId,
) -> None:
    jobs_on_workload_manager = {j.id: j for j in await workload_manager.list_jobs()}

    async with session.get(
        f"{amarcord_url}/api/indexing?status={DBJobStatus.RUNNING.value}"
        + (f"&beamtimeId={beamtime_id}" if beamtime_id is not None else "")
    ) as response:
        indexing_results = JsonReadIndexingResultsOutput(
            **await response.json()
        ).indexing_jobs

    for indexing_result in indexing_results:
        assert indexing_result.job_id is not None

        bound_logger = logger.bind(
            indexing_job_id=indexing_result.job_id,
            run_internal_id=indexing_result.run_internal_id,
            run_external_id=indexing_result.run_external_id,
        )

        bound_logger.info("job still running, checking on SLURM")

        workload_job = jobs_on_workload_manager.get(indexing_result.job_id)
        if workload_job is not None and workload_job.status not in (
            JobStatus.FAILED,
            JobStatus.SUCCESSFUL,
        ):
            bound_logger.info(
                f"job still running on slurm, status {workload_job.status}"
            )
            # Running job, let it keep running
            continue

        if workload_job is None:
            bound_logger.info("finished because not in SLURM REST job list anymore")
            job_error = "job has finished on SLURM (not in job list anymore), but delivered no results"
        else:
            job_error = f"job has finished on SLURM (status {workload_job.status}), but delivered no results"
            bound_logger.info(
                f"finished because SLURM REST job status is {workload_job.status}"
            )

        async with session.post(
            f"{amarcord_url}/api/indexing/{indexing_result.id}",
            json=JsonIndexingResultRootJson(
                error=job_error,
                job_id=indexing_result.job_id,
                stream_file=indexing_result.stream_file,
                result=empty_json_indexing_result(done=False),
            ).dict(),
        ) as update_response:
            bound_logger.info(f"indexing job finished, result: {update_response}")

    logger.info("indexing jobs stati updated, take a (longer) break")
    await asyncio.sleep(_LONG_BREAK_DURATION_SECONDS)


async def _indexing_loop_iteration(
    workload_manager: WorkloadManager,
    session: aiohttp.ClientSession,
    args: Arguments,
    start_new_jobs: bool,
) -> None:
    # This is weird, I know, but it stems from the fact that the DESY Maxwell REST API has rate limiting included,
    # so we cannot just do two REST API requests back to back. Instead, we have this weird counter.
    if start_new_jobs:
        await _start_new_jobs(session, workload_manager, args)

    else:
        await _update_jobs(
            session,
            workload_manager,
            amarcord_url=args.amarcord_url,
            beamtime_id=BeamtimeId(args.beamtime_id) if args.beamtime_id else None,
        )


async def _indexing_loop(args: Arguments) -> None:
    logger.info("starting Online CrystFEL indexing loop")

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
    async with aiohttp.ClientSession(connector=connector) as session:
        counter = 0
        while True:
            await _indexing_loop_iteration(
                workload_manager, session, args, counter % 10 != 0
            )

            counter += 1


def main() -> None:
    asyncio.run(_indexing_loop(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":
    main()
