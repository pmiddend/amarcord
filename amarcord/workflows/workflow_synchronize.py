import datetime
import json
import logging
from pathlib import Path

import sqlalchemy as sa

from amarcord.clock import Clock
from amarcord.clock import RealClock
from amarcord.modules.dbcontext import Connection
from amarcord.modules.json import JSONDict
from amarcord.newdb.db_data_reduction import DBDataReduction
from amarcord.newdb.db_reduction_job import DBReductionJob
from amarcord.newdb.newdb import NewDB
from amarcord.util import deglob_path
from amarcord.util import find_by
from amarcord.workflows.job_controller import JobController
from amarcord.workflows.job_status import JobStatus
from amarcord.workflows.parser import parse_workflow_result_file

logger = logging.getLogger(__name__)


def _process_running_job(
    conn: Connection,
    table_job_to_diffraction: sa.Table,
    job_id: str,
    job_metadata: JSONDict,
) -> None:
    conn.execute(
        sa.update(table_job_to_diffraction)
        .values(metadata=job_metadata)
        .where(table_job_to_diffraction.c.id == job_id)
    )


def _process_completed_job(
    conn: Connection,
    db: NewDB,
    job: DBReductionJob,
) -> None:
    assert job.output_directory is not None

    # Check the output
    output_description = job.output_directory / "amarcord-output.json"

    if not output_description.is_file():
        logger.info("job %s: output file %s not a file", job.id, output_description)

        db.update_job(
            conn,
            job.id,
            JobStatus.COMPLETED,
            f"output file not found: {output_description}",
            metadata=job.metadata,
            stopped=datetime.datetime.utcnow(),
        )
        return

    parse_results = parse_workflow_result_file(output_description)

    if isinstance(parse_results, str):
        logger.info(
            "job %s: output file %s parsing failed: %s",
            job.id,
            output_description,
            parse_results,
        )
        db.update_job(
            conn,
            job.id,
            JobStatus.COMPLETED,
            f"invalid output file {output_description}: {parse_results}",
            job.metadata,
            stopped=datetime.datetime.utcnow(),
        )
        return

    for parse_result in parse_results:
        logger.info("job %s: ingesting result", job.id)
        data_reduction_id = db.insert_data_reduction(
            conn,
            DBDataReduction(
                data_reduction_id=None,
                crystal_id=job.crystal_id,
                run_id=job.run_id,
                analysis_time=parse_result.analysis_time,
                method=parse_result.method,
                folder_path=parse_result.base_path,
                mtz_path=parse_result.mtz_file,
                comment=None,
                resolution_cc=parse_result.resolution_cc,
                resolution_isigma=parse_result.resolution_isigma,
                a=parse_result.a,
                b=parse_result.b,
                c=parse_result.c,
                alpha=parse_result.alpha,
                beta=parse_result.beta,
                gamma=parse_result.gamma,
                space_group=parse_result.space_group,
                isigi=parse_result.isigi,
                rmeas=parse_result.rmeas,
                cchalf=parse_result.cchalf,
                rfactor=parse_result.rfactor,
                wilson_b=parse_result.wilson_b,
            ),
        )
        db.insert_job_reduction_result(conn, job.id, data_reduction_id)

    logger.info("job %s: completing without error", job.id)
    db.update_job(
        conn,
        job.id,
        JobStatus.COMPLETED,
        failure_reason=None,
        metadata=job.metadata,
        stopped=datetime.datetime.utcnow(),
    )


def process_tool_command_line(
    command_line: str, diffraction_data_path: Path, tool_inputs: JSONDict
) -> str:
    result = command_line.replace("${diffraction.path}", str(diffraction_data_path))
    for param_name, param_value in tool_inputs.items():
        if isinstance(param_value, list):
            raise Exception(f"param {param_name} is a list: {param_value}")
        if isinstance(param_value, dict):
            raise Exception(f"param {param_name} is a dict: {param_value}")
        result = result.replace("${" + param_name + "}", str(param_value))
    return result


def _start_job(
    conn: Connection,
    job_controller: JobController,
    db: NewDB,
    job: DBReductionJob,
) -> None:
    if job.data_raw_filename_pattern is None:
        logger.info("wanted to start queued job, but have no diffraction path")
        db.update_job(
            conn,
            job.id,
            JobStatus.COMPLETED,
            "no filename pattern in diffraction",
            job.metadata,
            stopped=datetime.datetime.utcnow(),
        )
        return

    data_path = deglob_path(job.data_raw_filename_pattern)

    try:
        result = job_controller.start_job(
            Path(job.crystal_id) / str(job.run_id) / str(job.id),
            job.tool.executable_path,
            process_tool_command_line(
                job.tool.command_line, data_path, job.tool_inputs
            ),
            job.tool.extra_files,
        )
    except:
        logger.exception("failed to start job %s", job.id)
        db.update_job(
            conn,
            job.id,
            JobStatus.COMPLETED,
            "failed to start job",
            job.metadata,
            stopped=datetime.datetime.utcnow(),
        )
        return

    logger.info(
        "started queued job %s for crystal %s, run %s, metadata: %s",
        job.id,
        job.crystal_id,
        job.run_id,
        json.dumps(result.metadata),
    )

    db.update_job(
        conn,
        job.id,
        JobStatus.RUNNING,
        failure_reason=None,
        metadata=result.metadata,
        started=datetime.datetime.utcnow(),
        output_directory=result.output_directory,
    )


def check_jobs(
    job_controller: JobController,
    conn: Connection,
    db: NewDB,
    clock: Clock = RealClock(),
) -> None:
    # First, list jobs, add jobs that are unknown
    # Second, check reduction jobs in DB and remove all of them that are completed, ingesting the results
    with conn.begin():
        db_jobs = db.retrieve_reduction_jobs(conn)

        for db_job in (x for x in db_jobs if x.status == JobStatus.QUEUED):
            _start_job(conn, job_controller, db, db_job)

        controller_jobs = list(job_controller.list_jobs())
        # Iterate over the completed jobs, align with running DB jobs
        for job in (x for x in controller_jobs if x.status == JobStatus.COMPLETED):
            db_job = next(
                iter(
                    x
                    for x in db_jobs
                    if x.status == JobStatus.RUNNING
                    and job_controller.equals(job.metadata, x.metadata)
                ),
                # No idea why this doesn't type check (mypy)
                None,  # type: ignore
            )

            md_str = json.dumps(job.metadata)

            if db_job is None:
                logger.info("job %s: not in DB (yet?), status %s", md_str, job.status)
                continue

            logger.info("processing completed job %s (DB %s)", md_str, db_job.id)
            _process_completed_job(
                conn,
                db,
                db_job,
            )

        # Iterate over (old) running DB jobs that we don't find in the current job list
        # They might have died without us realizing it.
        now = clock.now()
        for db_job in (
            x
            for x in db_jobs
            if x.status == JobStatus.RUNNING
            and x.queued + datetime.timedelta(seconds=10) < now
        ):
            if (
                find_by(
                    controller_jobs,
                    lambda cj: job_controller.equals(db_job.metadata, cj.metadata),
                )
                is None
            ):
                logger.info("found stale job %s", db_job.id)
                _process_completed_job(
                    conn,
                    db,
                    db_job,
                )
