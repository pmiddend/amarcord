import asyncio
import datetime
import inspect
import json
import os
from datetime import timedelta
from pathlib import Path
from typing import cast

import aiohttp
import structlog
from structlog.stdlib import BoundLogger
from tap import Tap

import amarcord.cli.crystfel_index
from amarcord.amici.crystfel.util import determine_output_directory
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    SlurmRestWorkloadManager,
)
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.amici.workload_manager.workload_manager_factory import (
    create_workload_manager,
)
from amarcord.amici.workload_manager.workload_manager_factory import (
    parse_workload_manager_config,
)
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.indexing_result import empty_indexing_fom
from amarcord.util import overwrite_interpreter
from amarcord.web.json_models import JsonIndexingJob
from amarcord.web.json_models import JsonIndexingResultFinishWithError
from amarcord.web.json_models import JsonIndexingResultStillRunning
from amarcord.web.json_models import JsonReadIndexingResultsOutput

logger = structlog.stdlib.get_logger(__name__)

INDEXING_DAEMON_LONG_BREAK_DURATION_SECONDS_ENV_VAR = (
    "INDEXING_DAEMON_LONG_BREAK_DURATION_SECONDS"
)


def _long_break_duration_seconds() -> float:
    return float(
        os.environ.get(INDEXING_DAEMON_LONG_BREAK_DURATION_SECONDS_ENV_VAR, "5"),
    )


# For now, hard-code the time-limit for all (offline/online) jobs.
_GLOBAL_JOB_TIME_LIMIT = timedelta(days=1)


class Arguments(Tap):
    amarcord_url: str  # URL the daemon uses to look up indexing jobs in the DB
    overwrite_interpreter: str | None = None
    amarcord_url_for_spawned_job: str | None = None
    crystfel_path: (  # Where the CrystFEL binaries are located (without the /bin suffix!)
        Path
    )
    gnuplot_path: Path | None = None
    beamtime_id: int | None = None  # Can be used to filter indexing jobs by beamtime
    workload_manager_uri: str  # Determines how and where jobs are started; refer to the manual on how this URL should look like
    online_workload_manager_uri: str | None = None
    asapo_source: str  # The default source given to online indexing jobs
    # fmt: off
    cpu_count_multiplier: float | None = (  # Constant to give a multiplier to the number of CPUs in started jobs
        None
    )
    # fmt: on
    # fmt: off
    max_parallel_offline_jobs: int = 3  # Maximum number of offline jobs started in parallel (to not deadlock yourself when the primary jobs are all started and none of the secondary jobs)
    # fmt: on


def _get_indexing_job_source_code(overwrite_interpreter_str: None | str) -> str:
    with Path(inspect.getfile(amarcord.cli.crystfel_index)).open(
        "r",
        encoding="utf-8",
    ) as source_code_obj:
        source_code = source_code_obj.read()
        if overwrite_interpreter_str is not None:
            source_code = overwrite_interpreter(source_code, overwrite_interpreter_str)
        return source_code


async def start_offline_indexing_job(
    bound_logger: BoundLogger,
    workload_manager: WorkloadManager,
    args: Arguments,
    indexing_result: JsonIndexingJob,
) -> DBIndexingResultRunning | DBIndexingResultDone:
    bound_logger.info("starting offline indexing job")

    job_base_directory = (
        determine_output_directory(indexing_result.beamtime, {}) / "indexing-results"
    )

    output_base_name = _build_output_base_name(indexing_result)
    stream_file = job_base_directory / f"{output_base_name}.stream"

    if not indexing_result.input_file_globs:
        bound_logger.error(
            f"cannot start indexing job {indexing_result.id}: no input files",
        )
        return DBIndexingResultDone(
            stream_file=stream_file,
            job_error="no input file glob",
            fom=empty_indexing_fom,
        )

    try:
        indexing_job_source_code = _get_indexing_job_source_code(
            args.overwrite_interpreter,
        )

        job_environment: dict[str, str] = {
            # General parameters
            # Refer to the docs for a general explanation of how this job
            # style works. Basically, we submit one script for both online,
            # offline and "offline secondary" tasks.
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_JOB_STYLE: amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_JOB_STYLE_PRIMARY,
            # This is a bit "unabstracted": whether we use SLURM to start
            # job arrays (if so, see the token below), or if we use the
            # local machine and start a child process for analysis.
            #
            # These are the two options. If there's more, we need to think
            # about something more abstract and clean  than this.
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_USE_SLURM: (
                "True"
                if isinstance(workload_manager, SlurmRestWorkloadManager)
                else "False"
            ),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_STREAM_FILE: str(stream_file),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_AMARCORD_INDEXING_RESULT_ID: str(
                indexing_result.id,
            ),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_AMARCORD_API_URL: (
                args.amarcord_url_for_spawned_job
                if args.amarcord_url_for_spawned_job is not None
                else args.amarcord_url
            ),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_CRYSTFEL_PATH: str(
                args.crystfel_path,
            ),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_INDEXAMAJIG_PARAMS: indexing_result.command_line,
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_GNUPLOT_PATH: (
                str(args.gnuplot_path) if args.gnuplot_path is not None else ""
            ),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_CELL_DESCRIPTION: (
                indexing_result.cell_description
                if indexing_result.cell_description is not None
                else ""
            ),
            # Offline-specific parameters
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_INPUT_FILE_GLOBS: json.dumps(
                indexing_result.input_file_globs,
            ),
        }
        # An explicit geometry file may be missing for a new job - it
        # will then be found dynamically in the beamtime directory.
        if indexing_result.geometry_file_input:
            job_environment[
                amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_GEOMETRY_FILE
            ] = indexing_result.geometry_file_input
        # Offline indexing jobs, if configured that way, can emit
        # other jobs in a job array. For that, we need the SLURM REST
        # token again, so we transmit it here.
        if isinstance(workload_manager, SlurmRestWorkloadManager):
            job_environment["SLURM_TOKEN"] = await workload_manager.get_token()
            job_environment[
                amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_SLURM_PARTITION_TO_USE
            ] = workload_manager.partition
        bound_logger.info("environment for this job is " + " ".join(job_environment))
        job_start_result = await workload_manager.start_job(
            working_directory=job_base_directory,
            name=f"ix_run_{indexing_result.run_external_id}_{indexing_result.id}",
            script=indexing_job_source_code,
            time_limit=_GLOBAL_JOB_TIME_LIMIT,
            environment=job_environment,
            stdout=job_base_directory / f"{output_base_name}-stdout.txt",
            stderr=job_base_directory / f"{output_base_name}-stderr.txt",
        )
        bound_logger.info(
            "job start successful",
            indexing_job_id=job_start_result.job_id,
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


def _build_output_base_name(indexing_result: JsonIndexingJob) -> str:
    return f"run-{indexing_result.run_external_id}-indexing-{indexing_result.id}"


async def start_online_indexing_job(
    bound_logger: BoundLogger,
    workload_manager: WorkloadManager,
    args: Arguments,
    indexing_result: JsonIndexingJob,
) -> DBIndexingResultRunning | DBIndexingResultDone:
    bound_logger.info("starting online indexing job")

    job_base_directory = determine_output_directory(indexing_result.beamtime, {})

    output_base_name = _build_output_base_name(indexing_result)
    stream_file = job_base_directory / f"{output_base_name}.stream"

    try:
        indexing_job_source_code = _get_indexing_job_source_code(
            args.overwrite_interpreter,
        )

        job_environment: dict[str, str] = {
            # General parameters
            # Refer to the docs for a general explanation of how this job style works. Basically, we submit
            # one script for both online, offline and "offline secondary" tasks.
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_JOB_STYLE: amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_JOB_STYLE_ONLINE,
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_USE_SLURM: (
                "True"
                if isinstance(workload_manager, SlurmRestWorkloadManager)
                else "False"
            ),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_GNUPLOT_PATH: (
                str(args.gnuplot_path) if args.gnuplot_path is not None else ""
            ),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_STREAM_FILE: str(stream_file),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_AMARCORD_INDEXING_RESULT_ID: str(
                indexing_result.id,
            ),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_CRYSTFEL_PATH: str(
                args.crystfel_path,
            ),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_AMARCORD_API_URL: (
                args.amarcord_url
                if args.amarcord_url_for_spawned_job is None
                else args.amarcord_url_for_spawned_job
            ),
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_INDEXAMAJIG_PARAMS: indexing_result.command_line,
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_CELL_DESCRIPTION: (
                indexing_result.cell_description
                if indexing_result.cell_description is not None
                else ""
            ),
            # Online-specific parameters
            amarcord.cli.crystfel_index.ON_INDEX_ENVIRON_ASAPO_SOURCE: (
                # The user can specify a source explicitly (in the
                # online indexing default parameters), but if it's
                # blank, we use the default source.
                indexing_result.source
                if indexing_result.source.strip()
                else args.asapo_source
            ),
            amarcord.cli.crystfel_index.ON_INDEX_ENVIRON_AMARCORD_CPU_COUNT_MULTIPLIER: str(
                args.cpu_count_multiplier if args.cpu_count_multiplier else 0.5,
            ),
            # This we need to ask asapo for the correct stream.
            amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_RUN_ID: str(
                indexing_result.run_external_id,
            ),
        }
        # An explicit geometry file may be missing for a new job - it
        # will then be found dynamically in the beamtime directory.
        if indexing_result.geometry_file_input:
            job_environment[
                amarcord.cli.crystfel_index.OFF_INDEX_ENVIRON_GEOMETRY_FILE
            ] = indexing_result.geometry_file_input
        bound_logger.info("environment for this job is " + " ".join(job_environment))
        job_start_result = await workload_manager.start_job(
            working_directory=job_base_directory,
            name=f"oix_run_{indexing_result.run_external_id}_{indexing_result.id}",
            script=indexing_job_source_code,
            time_limit=_GLOBAL_JOB_TIME_LIMIT,
            environment=job_environment,
            stdout=job_base_directory / f"{output_base_name}-stdout.txt",
            stderr=job_base_directory / f"{output_base_name}-stderr.txt",
        )
        bound_logger.info(
            "job start successful",
            indexing_job_id=job_start_result.job_id,
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
    session: aiohttp.ClientSession,
    workload_manager: WorkloadManager,
    online_workload_manager: None | WorkloadManager,
    args: Arguments,
) -> None:
    # withFiles means "also include the file path for every result". This is needed for the "offline indexing"
    # part of the job running. We need to give the offline indexing run the list of files to process.
    #
    # For online indexing, this is precisely not needed, of course.
    async with session.get(
        f"{args.amarcord_url}/api/indexing?status={DBJobStatus.QUEUED.value}&withFiles=True"
        + (f"&beamtimeId={args.beamtime_id}" if args.beamtime_id is not None else ""),
    ) as response:
        indexing_results = JsonReadIndexingResultsOutput(
            **await response.json(),
        ).indexing_jobs

    if not indexing_results:
        return

    # To not start too many jobs, in addition to the queued ones, we
    # get the running ones.
    async with session.get(
        f"{args.amarcord_url}/api/indexing?status={DBJobStatus.RUNNING.value}"
        + (f"&beamtimeId={args.beamtime_id}" if args.beamtime_id is not None else ""),
    ) as response:
        number_of_running_jobs = len(
            JsonReadIndexingResultsOutput(**await response.json()).indexing_jobs,
        )

    max_jobs_to_start = args.max_parallel_offline_jobs - number_of_running_jobs

    if max_jobs_to_start <= 0:
        return

    if indexing_results:
        logger.info(
            f"there are {len(indexing_results)} job(s) to start, will start {max_jobs_to_start} (because of limit)",
        )

    number_of_started_jobs = 0
    for indexing_result in indexing_results:
        bound_logger = logger.bind(
            run_internal_id=indexing_result.run_internal_id,
            run_external_id=indexing_result.run_external_id,
            indexing_result_id=indexing_result.id,
        )
        new_status = (
            await start_online_indexing_job(
                bound_logger,
                (
                    online_workload_manager
                    if online_workload_manager is not None
                    else workload_manager
                ),
                args,
                indexing_result,
            )
            if indexing_result.is_online
            else await start_offline_indexing_job(
                bound_logger,
                workload_manager,
                args,
                indexing_result,
            )
        )
        number_of_started_jobs += 1
        assert new_status is not None

        if isinstance(new_status, DBIndexingResultDone):
            async with session.post(
                f"{args.amarcord_url}/api/indexing/{indexing_result.id}/finish-with-error",
                json=JsonIndexingResultFinishWithError(
                    # If we start a job and it's immediately finished, then we must have an error
                    error_message=cast(str, new_status.job_error),
                    workload_manager_job_id=indexing_result.job_id,
                    latest_log="",
                ).model_dump(),
            ) as update_response:
                bound_logger.info(f"indexing job errored, result: {update_response}")
        else:
            update_request = JsonIndexingResultStillRunning(
                workload_manager_job_id=new_status.job_id,
                stream_file=str(new_status.stream_file),
                hits=0,
                frames=0,
                indexed_frames=0,
                indexed_crystals=0,
                detector_shift_x_mm=None,
                detector_shift_y_mm=None,
                geometry_file="",
                geometry_hash="",
                job_started=datetime_to_attributo_int(
                    datetime.datetime.now(tz=datetime.timezone.utc),
                ),
                # Initialize log with the empty string (None would have indicated "no change")
                latest_log="",
            )

            async with session.post(
                f"{args.amarcord_url}/api/indexing/{indexing_result.id}/still-running",
                json=update_request.model_dump(),
            ) as update_response:
                if update_response.status // 200 != 1:
                    bound_logger.error(
                        f"didn't receive status 200 but {update_response.status}",
                    )
                else:
                    bound_logger.info(
                        f"new indexing job started, result: {update_response}",
                    )

        bound_logger.info(
            f"new indexing job submitted, taking a {_long_break_duration_seconds()}s break",
        )
        await asyncio.sleep(_long_break_duration_seconds())
        if number_of_started_jobs > max_jobs_to_start:
            logger.info(
                f"not starting any more jobs, since we have a {args.max_parallel_offline_jobs} limit set",
            )
            break
    if number_of_started_jobs == 0:
        # Usually too spammy
        # logger.info(
        #     f"no new queued jobs, waiting for {_long_break_duration_seconds()}s until next iteration"
        # )
        await asyncio.sleep(_long_break_duration_seconds())


async def _update_jobs(
    session: aiohttp.ClientSession,
    workload_manager: WorkloadManager,
    online_workload_manager: None | WorkloadManager,
    amarcord_url: str,
    beamtime_id: None | BeamtimeId,
) -> None:
    jobs_on_workload_manager = {j.id: j for j in await workload_manager.list_jobs()}
    jobs_on_online_workload_manager = (
        {j.id: j for j in await online_workload_manager.list_jobs()}
        if online_workload_manager is not None
        else None
    )

    async with session.get(
        f"{amarcord_url}/api/indexing?status={DBJobStatus.RUNNING.value}"
        + (f"&beamtimeId={beamtime_id}" if beamtime_id is not None else ""),
    ) as response:
        indexing_results = JsonReadIndexingResultsOutput(
            **await response.json(),
        ).indexing_jobs

    for indexing_result in indexing_results:
        assert indexing_result.job_id is not None

        bound_logger = logger.bind(
            indexing_job_id=indexing_result.job_id,
            run_internal_id=indexing_result.run_internal_id,
            run_external_id=indexing_result.run_external_id,
        )

        bound_logger.info("job still running, checking on workload manager")

        # This is a bit tricky: we don't want to look at either the
        # online job "registry", if that's available and it's an
        # online job, otherwise the offline one.
        job_array = (
            jobs_on_online_workload_manager
            if indexing_result.is_online and jobs_on_online_workload_manager is not None
            else jobs_on_workload_manager
        )
        workload_job = job_array.get(indexing_result.job_id)
        if workload_job is not None and workload_job.status not in (
            JobStatus.FAILED,
            JobStatus.SUCCESSFUL,
        ):
            bound_logger.info(
                f"job still running on workload manager, status {workload_job.status}",
            )
            # Running job, let it keep running
            continue

        if workload_job is None:
            bound_logger.info("finished because not in job list anymore")
            job_error = f"job has finished on {workload_manager.name()} (not in job list anymore), but delivered no results"
        else:
            job_error = f"job has finished on {workload_manager.name()} (status {workload_job.status}), but delivered no results"
            bound_logger.info(
                f"finished because {workload_manager.name()} job status is {workload_job.status}",
            )

        async with session.post(
            f"{amarcord_url}/api/indexing/{indexing_result.id}/finish-with-error",
            json=JsonIndexingResultFinishWithError(
                error_message=job_error,
                workload_manager_job_id=indexing_result.job_id,
                latest_log="",
            ).model_dump(),
        ) as update_response:
            bound_logger.info(f"indexing job finished, result: {update_response}")

    # this is usually too spammy
    # logger.info("indexing jobs stati updated, take a (longer) break")
    await asyncio.sleep(_long_break_duration_seconds())


async def indexing_loop_iteration(
    workload_manager: WorkloadManager,
    online_workload_manager: None | WorkloadManager,
    session: aiohttp.ClientSession,
    args: Arguments,
    start_new_jobs: bool,
) -> None:
    # This is weird, I know, but it stems from the fact that the DESY Maxwell REST API has rate limiting included,
    # so we cannot just do two REST API requests back to back. Instead, we have this weird counter.
    if start_new_jobs:
        await _start_new_jobs(session, workload_manager, online_workload_manager, args)

    else:
        await _update_jobs(
            session,
            workload_manager,
            online_workload_manager,
            amarcord_url=args.amarcord_url,
            beamtime_id=BeamtimeId(args.beamtime_id) if args.beamtime_id else None,
        )


# We can't really test this code, it's pure glue
async def _indexing_loop(args: Arguments) -> None:  # pragma: no cover
    logger.info("starting CrystFEL indexing loop")

    workload_manager = create_workload_manager(
        parse_workload_manager_config(args.workload_manager_uri),
    )
    online_workload_manager = (
        workload_manager
        if args.online_workload_manager_uri is None
        else create_workload_manager(
            parse_workload_manager_config(args.online_workload_manager_uri),
        )
    )

    # Why this? Well, uvicorn keeps closing the connection, although
    # we'd like to Keep-Alive it. See
    #
    # https://stackoverflow.com/questions/51248714/aiohttp-client-exception-serverdisconnectederror-is-this-the-api-servers-issu
    #
    # We could also just create a session over and over, but this seems a bit more clean
    connector = aiohttp.TCPConnector(force_close=True)
    async with aiohttp.ClientSession(connector=connector) as session:
        # This is the heart of the daemon: a simple loop that
        # interleaves two operations: starting new runs, and checking
        # if any of the existing runs changed their status. To
        # implement these two operations, we have an infinite ocunter
        # and check the modulus.
        counter = 0
        while True:
            await indexing_loop_iteration(
                workload_manager,
                online_workload_manager,
                session,
                args,
                counter % 10 != 0,
            )

            counter += 1


def main() -> None:  # pragma: no cover
    asyncio.run(_indexing_loop(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":  # pragma: no cover
    main()
