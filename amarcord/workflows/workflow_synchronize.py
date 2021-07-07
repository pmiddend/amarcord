import datetime
import json
import logging
from dataclasses import dataclass
from pathlib import Path
from typing import List
from typing import Optional

import sqlalchemy as sa

from amarcord.amici.p11.db_ingest import ingest_analysis_result
from amarcord.clock import Clock
from amarcord.clock import RealClock
from amarcord.modules.dbcontext import Connection
from amarcord.modules.json import JSONDict
from amarcord.util import find_by
from amarcord.workflows.job_controller import JobController
from amarcord.workflows.job_status import JobStatus
from amarcord.workflows.parser import parse_workflow_result_file

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class _Tool:
    id: int
    inputs: JSONDict
    executable_path: Path
    extra_files: List[Path]
    command_line: str


@dataclass(frozen=True)
class _ReductionJob:
    id: int
    queued: datetime.datetime
    started: Optional[datetime.datetime]
    stopped: Optional[datetime.datetime]
    status: JobStatus
    output_directory: Optional[Path]
    run_id: int
    crystal_id: str
    data_raw_filename_pattern: Optional[str]
    metadata: JSONDict
    tool: _Tool


def _update_job_status(
    conn: Connection,
    table_jobs: sa.Table,
    job_id: int,
    job_metadata: JSONDict,
    failure_message: Optional[str],
) -> None:
    conn.execute(
        sa.update(table_jobs)
        .values(
            failure_reason=failure_message,
            stopped=datetime.datetime.utcnow(),
            status=JobStatus.COMPLETED,
            metadata=job_metadata,
        )
        .where(table_jobs.c.id == job_id)
    )


def _process_running_job(
    conn: Connection,
    table_reduction_jobs: sa.Table,
    job_id: str,
    job_metadata: JSONDict,
) -> None:
    conn.execute(
        sa.update(table_reduction_jobs)
        .values(metadata=job_metadata)
        .where(table_reduction_jobs.c.id == job_id)
    )


def _process_completed_job(
    conn: Connection,
    table_data_reduction: sa.Table,
    table_reduction_jobs: sa.Table,
    table_job_to_reductions: sa.Table,
    job: _ReductionJob,
) -> None:
    assert job.output_directory is not None

    # Check the output
    output_description = job.output_directory / "amarcord-output.json"

    if not output_description.is_file():
        logger.info("job %s: output file %s not a file", job.id, output_description)
        _update_job_status(
            conn,
            table_reduction_jobs,
            job.id,
            job.metadata,
            f"output file not found: {output_description}",
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
        _update_job_status(
            conn,
            table_reduction_jobs,
            job.id,
            job.metadata,
            f"invalid output file {output_description}: {parse_results}",
        )
        return

    for parse_result in parse_results:
        logger.info("job %s: ingesting result", job.id)
        data_reduction_id = ingest_analysis_result(
            conn,
            job.crystal_id,
            table_data_reduction,
            job.run_id,
            parse_result,
        )
        conn.execute(
            sa.insert(table_job_to_reductions).values(
                job_id=job.id, data_reduction_id=data_reduction_id
            )
        )

    logger.info("job %s: completing without error", job.id)
    _update_job_status(
        conn, table_reduction_jobs, job.id, job.metadata, failure_message=None
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
    table_reduction_jobs: sa.Table,
    job: _ReductionJob,
) -> None:
    if job.data_raw_filename_pattern is None:
        logger.info("wanted to start queued job, but have no diffraction path")
        _update_job_status(
            conn,
            table_reduction_jobs,
            job.id,
            job.metadata,
            "no filename pattern in Diffraction",
        )
        return

    data_path = Path(
        job.data_raw_filename_pattern[:-5]
        if job.data_raw_filename_pattern.endswith("*.cbf")
        else job.data_raw_filename_pattern
    )

    try:
        result = job_controller.start_job(
            Path(job.crystal_id) / str(job.run_id) / str(job.id),
            job.tool.executable_path,
            process_tool_command_line(
                job.tool.command_line, data_path, job.tool.inputs
            ),
            job.tool.extra_files,
        )
    except:
        logger.exception("failed to start job %s", job.id)
        _update_job_status(
            conn,
            table_reduction_jobs,
            job.id,
            job.metadata,
            "failed to start job",
        )
        return

    logger.info(
        "started queued job %s for crystal %s, run %s, metadata: %s",
        job.id,
        job.crystal_id,
        job.run_id,
        json.dumps(result.metadata),
    )

    conn.execute(
        sa.update(table_reduction_jobs)
        .values(
            status=JobStatus.RUNNING,
            started=datetime.datetime.utcnow(),
            output_directory=str(result.output_directory),
            metadata=result.metadata,
        )
        .where(table_reduction_jobs.c.id == job.id)
    )


def check_jobs(
    job_controller: JobController,
    conn: Connection,
    table_tools: sa.Table,
    table_reduction_jobs: sa.Table,
    table_job_to_reductions: sa.Table,
    table_diffractions: sa.Table,
    table_data_reduction: sa.Table,
    clock: Clock = RealClock(),
) -> None:
    # First, list jobs, add jobs that are unknown
    # Second, check reduction jobs in DB and remove all of them that are completed, ingesting the results
    with conn.begin():
        db_jobs: List[_ReductionJob] = [
            _ReductionJob(
                id=job["id"],
                started=job["started"],
                stopped=job["stopped"],
                queued=job["queued"],
                status=job["status"],
                output_directory=Path(job["output_directory"])
                if job["output_directory"] is not None
                else None,
                data_raw_filename_pattern=job["data_raw_filename_pattern"],
                run_id=job["run_id"],
                crystal_id=job["crystal_id"],
                metadata=job["metadata"],
                tool=_Tool(
                    id=job["tool_id"],
                    inputs=job["tool_inputs"],
                    executable_path=Path(job["executable_path"]),
                    extra_files=[Path(p) for p in job["extra_files"]],
                    command_line=job["command_line"],
                ),
            )
            for job in conn.execute(
                sa.select(
                    [
                        table_reduction_jobs.c.id,
                        table_reduction_jobs.c.started,
                        table_reduction_jobs.c.stopped,
                        table_reduction_jobs.c.queued,
                        table_reduction_jobs.c.output_directory,
                        table_reduction_jobs.c.run_id,
                        table_reduction_jobs.c.crystal_id,
                        table_reduction_jobs.c.tool_id,
                        table_reduction_jobs.c.tool_inputs,
                        table_reduction_jobs.c.metadata,
                        table_reduction_jobs.c.status,
                        table_tools.c.executable_path,
                        table_tools.c.command_line,
                        table_tools.c.extra_files,
                        table_diffractions.c.data_raw_filename_pattern,
                    ]
                ).select_from(
                    table_reduction_jobs.join(table_tools).join(table_diffractions)
                )
            ).fetchall()
        ]

        for db_job in (x for x in db_jobs if x.status == JobStatus.QUEUED):
            _start_job(conn, job_controller, table_reduction_jobs, db_job)

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
                table_data_reduction,
                table_reduction_jobs,
                table_job_to_reductions,
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
                    table_data_reduction,
                    table_reduction_jobs,
                    table_job_to_reductions,
                    db_job,
                )
