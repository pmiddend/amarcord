import datetime
import json
import logging
from pathlib import Path
from typing import List
from typing import Optional
from typing import Set
from typing import Tuple

import sqlalchemy as sa

from amarcord.amici.p11.analysis_result import AnalysisResult
from amarcord.amici.p11.refinement_result import RefinementResult
from amarcord.clock import Clock
from amarcord.clock import RealClock
from amarcord.modules.dbcontext import Connection
from amarcord.modules.json import JSONDict
from amarcord.newdb.db_data_reduction import DBDataReduction
from amarcord.newdb.db_job import DBJob
from amarcord.newdb.db_reduction_job import DBJobWithInputsAndOutputs
from amarcord.newdb.db_reduction_job import DBMiniDiffraction
from amarcord.newdb.db_reduction_job import DBMiniReduction
from amarcord.newdb.db_refinement import DBRefinement
from amarcord.newdb.newdb import NewDB
from amarcord.newdb.refinement_method import RefinementMethod
from amarcord.util import deglob_path
from amarcord.util import find_by
from amarcord.workflows.job_controller import JobController
from amarcord.workflows.job_status import JobStatus
from amarcord.workflows.parser import parse_workflow_result_file

logger = logging.getLogger(__name__)


def bulk_start_jobs(
    conn: Connection,
    db: NewDB,
    tool_id: int,
    filter_query: str,
    limit: Optional[int],
    tool_inputs: JSONDict,
) -> List[int]:
    tool = next(iter(t for t in db.retrieve_tools(conn) if t.id == tool_id), None)
    if tool is None:
        raise Exception(f"tool with ID {tool_id} not found!")

    if tool.inputs is None:
        raise Exception(f"tool {tool.name} (ID {tool_id}) has no inputs!")

    # If the inputs contain reduction parameters, fetch reductions, then start jobs for every reduction
    # Otherwise, do the same for diffractions
    if tool.inputs.contains_reductions():
        return _bulk_start_jobs_reductions(
            conn, db, tool_id, filter_query, limit, tool_inputs
        )

    return _bulk_start_jobs_diffractions(
        conn, db, tool_id, filter_query, limit, tool_inputs
    )


def _bulk_start_jobs_reductions(
    conn: Connection,
    db: NewDB,
    tool_id: int,
    filter_query: str,
    limit: Optional[int],
    tool_inputs: JSONDict,
) -> List[int]:
    processed_reductions: Set[int] = set()
    inserted_jobs: List[int] = []
    for data_reduction_id in db.retrieve_analysis_reductions(conn, filter_query):
        if len(processed_reductions) == limit:
            break

        if data_reduction_id not in processed_reductions:
            processed_reductions.add(data_reduction_id)
            job_id = db.insert_job(
                conn,
                DBJob(
                    id=None,
                    queued=datetime.datetime.utcnow(),
                    status=JobStatus.QUEUED,
                    tool_id=tool_id,
                    tool_inputs=tool_inputs,
                ),
            )
            db.insert_job_to_reduction(
                conn,
                job_id=job_id,
                data_reduction_id=data_reduction_id,
            )
            inserted_jobs.append(job_id)

    return inserted_jobs


def _bulk_start_jobs_diffractions(
    conn: Connection,
    db: NewDB,
    tool_id: int,
    filter_query: str,
    limit: Optional[int],
    tool_inputs: JSONDict,
) -> List[int]:
    diffractions = db.retrieve_analysis_diffractions(conn, filter_query)
    processed_diffractions: Set[Tuple[str, int]] = set()
    inserted_jobs: List[int] = []
    for crystal_id, run_id in diffractions:
        if len(processed_diffractions) == limit:
            break

        if (crystal_id, run_id) not in processed_diffractions:
            processed_diffractions.add((crystal_id, run_id))
            job_id = db.insert_job(
                conn,
                DBJob(
                    id=None,
                    queued=datetime.datetime.utcnow(),
                    status=JobStatus.QUEUED,
                    tool_id=tool_id,
                    tool_inputs=tool_inputs,
                ),
            )
            db.insert_job_to_diffraction(
                conn,
                job_id=job_id,
                crystal_id=crystal_id,
                run_id=run_id,
            )
            inserted_jobs.append(job_id)

    return inserted_jobs


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
    job: DBJobWithInputsAndOutputs,
) -> None:
    assert job.job.output_directory is not None

    # Check the output
    output_description = job.job.output_directory / "amarcord-output.json"

    if not output_description.is_file():
        logger.info("job %s: output file %s not a file", job.job.id, output_description)

        assert job.job.id is not None
        db.update_job(
            conn,
            job.job.id,
            JobStatus.COMPLETED,
            f"output file not found: {output_description}",
            metadata=job.job.metadata,
            stopped=datetime.datetime.utcnow(),
        )
        return

    parse_results = parse_workflow_result_file(output_description)

    if isinstance(parse_results, str):
        logger.info(
            "job %s: output file %s parsing failed: %s",
            job.job.id,
            output_description,
            parse_results,
        )
        assert job.job.id is not None
        db.update_job(
            conn,
            job.job.id,
            JobStatus.COMPLETED,
            f"invalid output file {output_description}: {parse_results}",
            job.job.metadata,
            stopped=datetime.datetime.utcnow(),
        )
        return

    for parse_result in parse_results:
        logger.info("job %s: ingesting result", job.job.id)
        if isinstance(parse_result, AnalysisResult):
            if not isinstance(job.io, DBMiniDiffraction):
                raise Exception(
                    "got an analysis result, but job was not a reduction job; that's not an error per se, "
                    "but we're not supporting it right now"
                )
            data_reduction_id = db.insert_data_reduction(
                conn,
                DBDataReduction(
                    data_reduction_id=None,
                    crystal_id=job.io.crystal_id,
                    run_id=job.io.run_id,
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
            assert job.job.id is not None
            db.insert_job_reduction_result(conn, job.job.id, data_reduction_id)
        elif isinstance(parse_result, RefinementResult):
            if not isinstance(job.io, DBMiniReduction):
                raise Exception(
                    "got a refinement result, but job was not a refinement job; that's not an error per se, "
                    "but we're not supporting it right now"
                )
            ja = job.io
            refinement_id = db.insert_refinement(
                conn,
                DBRefinement(
                    refinement_id=None,
                    data_reduction_id=ja.data_reduction_id,
                    analysis_time=parse_result.analysis_time,
                    folder_path=parse_result.folder_path,
                    initial_pdb_path=parse_result.initial_pdb_path,
                    final_pdb_path=parse_result.final_pdb_path,
                    refinement_mtz_path=parse_result.refinement_mtz_path,
                    method=RefinementMethod.HZB,
                    comment=parse_result.comment,
                    resolution_cut=parse_result.resolution_cut,
                    rfree=parse_result.rfree,
                    rwork=parse_result.rwork,
                    rms_bond_length=parse_result.rms_bond_length,
                    rms_bond_angle=parse_result.rms_bond_angle,
                    num_blobs=parse_result.num_blobs,
                    average_model_b=parse_result.average_model_b,
                ),
            )
            assert job.job.id is not None
            db.insert_job_refinement_result(conn, job.job.id, refinement_id)
        else:
            raise Exception(f"invalid analysis result type: {type(parse_result)}")

    logger.info("job %s: completing without error", job.job.id)
    assert job.job.id is not None
    db.update_job(
        conn,
        job.job.id,
        JobStatus.COMPLETED,
        failure_reason=None,
        metadata=job.job.metadata,
        stopped=datetime.datetime.utcnow(),
    )


def process_tool_command_line(
    command_line: str,
    mtz_path: Optional[Path],
    diffraction_data_path: Optional[Path],
    tool_inputs: JSONDict,
) -> str:
    result = command_line
    if diffraction_data_path is not None:
        result = result.replace("${diffraction.path}", str(diffraction_data_path))
    if mtz_path is not None:
        result = result.replace("${reduction.mtz_path}", str(mtz_path))
    for param_name, param_value in tool_inputs.items():
        if isinstance(param_value, list):
            raise Exception(f"param {param_name} is a list: {param_value}")
        if isinstance(param_value, dict):
            raise Exception(f"param {param_name} is a dict: {param_value}")
        result = result.replace("${" + param_name + "}", str(param_value))
    return result


def _start_job_on_reduction(
    conn: Connection,
    job_controller: JobController,
    db: NewDB,
    job: DBJobWithInputsAndOutputs,
    reduction: DBMiniReduction,
) -> None:
    assert job.job.id is not None
    try:
        assert job.job.tool_inputs is not None
        result = job_controller.start_job(
            Path(reduction.crystal_id)
            / str(reduction.run_id)
            / f"reduction-{reduction.data_reduction_id}-refinement-job-{job.job.id}",
            job.tool.executable_path,
            process_tool_command_line(
                job.tool.command_line,
                mtz_path=reduction.mtz_path,
                diffraction_data_path=None,
                tool_inputs=job.job.tool_inputs,
            ),
            job.tool.extra_files,
        )
    except:
        logger.exception("failed to start refinement job %s", job.job.id)
        db.update_job(
            conn,
            job.job.id,
            JobStatus.COMPLETED,
            "failed to start job",
            job.job.metadata,
            stopped=datetime.datetime.utcnow(),
        )
        return

    logger.info(
        "started queued reduction job %s for reduction %s, metadata: %s",
        job.job.id,
        reduction.data_reduction_id,
        json.dumps(result.metadata),
    )

    db.update_job(
        conn,
        job.job.id,
        JobStatus.RUNNING,
        failure_reason=None,
        metadata=result.metadata,
        started=datetime.datetime.utcnow(),
        output_directory=result.output_directory,
    )


def _start_job_on_diffraction(
    conn: Connection,
    job_controller: JobController,
    db: NewDB,
    job: DBJobWithInputsAndOutputs,
    diffraction: DBMiniDiffraction,
) -> None:
    assert job.job.id is not None
    if diffraction.data_raw_filename_pattern is None:
        logger.info("wanted to start queued job, but have no diffraction path")
        db.update_job(
            conn,
            job.job.id,
            JobStatus.COMPLETED,
            "no filename pattern in diffraction",
            job.job.metadata,
            stopped=datetime.datetime.utcnow(),
        )
        return

    data_path = deglob_path(diffraction.data_raw_filename_pattern)

    try:
        assert job.job.tool_inputs is not None
        result = job_controller.start_job(
            Path(diffraction.crystal_id)
            / str(diffraction.run_id)
            / f"reduction-job-{job.job.id}",
            job.tool.executable_path,
            process_tool_command_line(
                job.tool.command_line,
                mtz_path=None,
                diffraction_data_path=data_path,
                tool_inputs=job.job.tool_inputs,
            ),
            job.tool.extra_files,
        )
    except:
        logger.exception("failed to start reduction job %s", job.job.id)
        db.update_job(
            conn,
            job.job.id,
            JobStatus.COMPLETED,
            "failed to start job",
            job.job.metadata,
            stopped=datetime.datetime.utcnow(),
        )
        return

    logger.info(
        "started queued reduction job %s for crystal %s, run %s, metadata: %s",
        job.job.id,
        diffraction.crystal_id,
        diffraction.run_id,
        json.dumps(result.metadata),
    )

    db.update_job(
        conn,
        job.job.id,
        JobStatus.RUNNING,
        failure_reason=None,
        metadata=result.metadata,
        started=datetime.datetime.utcnow(),
        output_directory=result.output_directory,
    )


def _start_job(
    conn: Connection,
    job_controller: JobController,
    db: NewDB,
    job: DBJobWithInputsAndOutputs,
) -> None:
    if isinstance(job.io, DBMiniDiffraction):
        _start_job_on_diffraction(conn, job_controller, db, job, job.io)
    else:
        _start_job_on_reduction(conn, job_controller, db, job, job.io)


def check_jobs(
    job_controller: JobController,
    conn: Connection,
    db: NewDB,
    clock: Clock = RealClock(),
) -> None:
    # First, list jobs, add jobs that are unknown
    # Second, check reduction jobs in DB and remove all of them that are completed, ingesting the results
    with conn.begin():
        db_jobs = db.retrieve_jobs_with_attached(conn)

        for db_job in (x for x in db_jobs if x.job.status == JobStatus.QUEUED):
            _start_job(conn, job_controller, db, db_job)

        controller_jobs = list(job_controller.list_jobs())
        # Iterate over the completed jobs, align with running DB jobs
        for rt_job in (y for y in controller_jobs if y.status == JobStatus.COMPLETED):
            db_job = next(
                iter(
                    x
                    for x in db_jobs
                    if x.job.status == JobStatus.RUNNING
                    and x.job.metadata is not None
                    and job_controller.equals(rt_job.metadata, x.job.metadata)
                ),
                # No idea why this doesn't type check (mypy)
                None,  # type: ignore
            )

            md_str = json.dumps(rt_job.metadata)

            if db_job is None:
                logger.info(
                    "job %s: not in DB (yet?), status %s", md_str, rt_job.status
                )
                continue

            logger.info("processing completed job %s (DB %s)", md_str, db_job.job.id)
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
            if x.job.status == JobStatus.RUNNING
            and x.job.queued + datetime.timedelta(seconds=10) < now
        ):
            if (
                find_by(
                    controller_jobs,
                    lambda cj: job_controller.equals(db_job.job.metadata, cj.metadata),  # type: ignore
                )
                is None
            ):
                logger.info("found stale job %s", db_job.job.id)
                _process_completed_job(
                    conn,
                    db,
                    db_job,
                )
