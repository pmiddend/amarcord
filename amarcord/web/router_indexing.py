import datetime
from statistics import mean
from typing import Annotated

import structlog
from fastapi import APIRouter
from fastapi import Depends
from fastapi import HTTPException
from fastapi.responses import PlainTextResponse
from sqlalchemy import true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.cli.crystfel_index import sha256_bytes
from amarcord.db import orm
from amarcord.db.attributi import utc_datetime_to_local_int
from amarcord.db.attributi import utc_datetime_to_utc_int
from amarcord.db.attributi import utc_int_to_utc_datetime
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.constants import CELL_DESCRIPTION_ATTRIBUTO
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.indexing_result import IndexingResultSummary
from amarcord.db.indexing_result import empty_indexing_fom
from amarcord.db.orm_utils import add_geometry_template_replacements_to_ir
from amarcord.db.orm_utils import all_geometry_metadatas
from amarcord.db.run_internal_id import RunInternalId
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.fastapi_utils import retrieve_runs_matching_data_set
from amarcord.web.json_models import JsonAlignDetectorGroup
from amarcord.web.json_models import JsonBeamtimeOutput
from amarcord.web.json_models import JsonCreateIndexingForDataSetInput
from amarcord.web.json_models import JsonCreateIndexingForDataSetOutput
from amarcord.web.json_models import JsonImportFinishedIndexingJobInput
from amarcord.web.json_models import JsonImportFinishedIndexingJobOutput
from amarcord.web.json_models import JsonIndexingFom
from amarcord.web.json_models import JsonIndexingJob
from amarcord.web.json_models import JsonIndexingJobUpdateOutput
from amarcord.web.json_models import JsonIndexingResultFinishSuccessfully
from amarcord.web.json_models import JsonIndexingResultFinishWithError
from amarcord.web.json_models import JsonIndexingResultStillRunning
from amarcord.web.json_models import JsonReadIndexingParametersOutput
from amarcord.web.json_models import JsonReadIndexingResultsOutput

logger = structlog.stdlib.get_logger(__name__)
router = APIRouter()


async def json_indexing_job_from_orm(
    ij: orm.IndexingResult,
    with_files: bool,
) -> JsonIndexingJob:
    bt = ij.run.beamtime
    return JsonIndexingJob(
        id=ij.id,
        job_id=ij.job_id,
        job_status=ij.job_status,
        is_online=ij.indexing_parameters.is_online,
        stream_file=ij.stream_file,
        cell_description=ij.indexing_parameters.cell_description,
        source=ij.indexing_parameters.source,
        geometry_id=ij.indexing_parameters.geometry_id,
        generated_geometry_id=ij.generated_geometry_id,
        run_external_id=ij.run.external_id,
        run_internal_id=ij.run.id,
        beamtime=JsonBeamtimeOutput(
            id=bt.id,
            external_id=bt.external_id,
            proposal=bt.proposal,
            beamline=bt.beamline,
            title=bt.title,
            comment=bt.comment,
            start=utc_datetime_to_utc_int(bt.start),
            start_local=utc_datetime_to_local_int(bt.start),
            end=utc_datetime_to_utc_int(bt.end),
            end_local=utc_datetime_to_local_int(bt.end),
            chemical_names=[],
            analysis_output_path=bt.analysis_output_path,
        ),
        input_file_globs=(
            []
            if not with_files
            else [
                f.glob
                for f in await ij.run.awaitable_attrs.files
                if f.source == ij.indexing_parameters.source
            ]
        ),
        command_line=ij.indexing_parameters.command_line,
        started=(
            utc_datetime_to_utc_int(ij.job_started)
            if ij.job_started is not None
            else None
        ),
        started_local=(
            utc_datetime_to_local_int(ij.job_started)
            if ij.job_started is not None
            else None
        ),
        stopped=(
            utc_datetime_to_utc_int(ij.job_stopped)
            if ij.job_stopped is not None
            else None
        ),
        stopped_local=(
            utc_datetime_to_local_int(ij.job_stopped)
            if ij.job_stopped is not None
            else None
        ),
    )


def encode_indexing_fom_to_json(summary: IndexingResultSummary) -> JsonIndexingFom:
    return JsonIndexingFom(
        hit_rate=summary.hit_rate,
        indexing_rate=summary.indexing_rate,
        indexed_frames=summary.indexed_frames,
        align_detector_groups=summary.align_detector_groups,
    )


def fom_for_indexing_result(jr: orm.IndexingResult) -> IndexingResultSummary:
    assert jr.hits is not None
    assert jr.frames is not None
    assert jr.indexed_frames is not None
    return IndexingResultSummary(
        hit_rate=jr.hits / jr.frames * 100.0 if jr.frames > 0 else 0.0,
        indexing_rate=jr.indexed_frames / jr.hits * 100.0 if jr.hits > 0 else 0.0,
        indexed_frames=jr.indexed_frames,
        align_detector_groups=[
            JsonAlignDetectorGroup(
                group=g.group,
                x_translation_mm=g.x_translation_mm,
                y_translation_mm=g.y_translation_mm,
                z_translation_mm=g.z_translation_mm,
                x_rotation_deg=g.x_rotation_deg,
                y_rotation_deg=g.y_rotation_deg,
            )
            for g in jr.align_detector_groups
        ],
    )


async def _locate_or_create_geometry(
    session: AsyncSession, beamtime_id: BeamtimeId, name: str, contents: str
) -> orm.Geometry:
    geometry_hash = sha256_bytes(contents.encode("utf-8"))
    geometry: None | orm.Geometry = (
        await session.scalars(
            select(orm.Geometry).where(orm.Geometry.hash == geometry_hash)
        )
    ).one_or_none()

    if geometry is None:
        geometry = orm.Geometry(
            beamtime_id=beamtime_id,
            content=contents,
            hash=geometry_hash,
            name=name,
            created=datetime.datetime.now(datetime.timezone.utc),
        )
        session.add(geometry)
        # To get the new geometry ID
        await session.flush()
    return geometry


def summary_from_foms(ir: list[IndexingResultSummary]) -> IndexingResultSummary:
    if not ir:
        return empty_indexing_fom
    return IndexingResultSummary(
        hit_rate=mean(x.hit_rate for x in ir),
        indexing_rate=mean(x.indexing_rate for x in ir),
        indexed_frames=sum(x.indexed_frames for x in ir),
        # For now, we don't summarize detector shifts. We could, but we're not sure it makes actual sense
        align_detector_groups=[],
    )


@router.post(
    "/api/indexing/import",
    tags=["processing"],
    response_model_exclude_defaults=True,
)
async def import_finished_indexing_job(
    input_: JsonImportFinishedIndexingJobInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonImportFinishedIndexingJobOutput:
    """
    This will import an already finished indexing job, from another beamline for example.

    It's not possible to do this with the other methods here, since you'd have to queue
    something for a whole dataset and then finish something with a concrete indexing
    job ID.
    """
    run: None | orm.Run = (
        await session.scalars(
            select(orm.Run).where(orm.Run.id == input_.run_internal_id)
        )
    ).one_or_none()

    if run is None:
        raise HTTPException(
            status_code=404,
            detail=f"internal run ID {input_.run_internal_id} not found",
        )

    geometry = await _locate_or_create_geometry(
        session,
        run.beamtime_id,
        f"import for run {run.external_id}",
        input_.geometry_contents,
    )

    new_indexing_parameters = orm.IndexingParameters(
        is_online=input_.is_online,
        cell_description=input_.cell_description.strip(),
        command_line=input_.command_line.strip(),
        geometry_id=geometry.id,
        source=input_.source.strip(),
    )
    session.add(new_indexing_parameters)
    await session.flush()
    new_indexing_result = orm.IndexingResult(
        created=datetime.datetime.now(datetime.timezone.utc),
        run_id=RunInternalId(input_.run_internal_id),
        stream_file=input_.stream_file,
        program_version=input_.program_version,
        frames=input_.frames,
        hits=input_.hits,
        indexed_frames=input_.indexed_frames,
        generated_geometry_id=None,
        unit_cell_histograms_file_id=None,
        job_id=None,
        job_status=DBJobStatus.DONE,
        job_error=None,
        job_latest_log=input_.job_log,
        job_started=None,
        job_stopped=None,
        indexing_parameters_id=new_indexing_parameters.id,
    )
    for g in input_.align_detector_groups:
        new_indexing_result.align_detector_groups.append(
            orm.AlignDetectorGroup(
                group=g.group,
                x_translation_mm=g.x_translation_mm,
                y_translation_mm=g.y_translation_mm,
                z_translation_mm=g.z_translation_mm,
                x_rotation_deg=g.x_rotation_deg,
                y_rotation_deg=g.y_rotation_deg,
            )
        )
    await add_geometry_template_replacements_to_ir(new_indexing_result, run, geometry)
    session.add(new_indexing_result)
    await session.commit()
    return JsonImportFinishedIndexingJobOutput(
        indexing_result_id=new_indexing_result.id
    )


@router.post(
    "/api/indexing",
    tags=["processing"],
    response_model_exclude_defaults=True,
)
async def indexing_job_queue_for_data_set(
    input_: JsonCreateIndexingForDataSetInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonCreateIndexingForDataSetOutput:
    job_logger = logger.bind(data_set_id=input_.data_set_id)
    job_logger.info("start creation of indexing result")

    # Determine which runs belong to the data set (and if they are complete).
    # Then we create jobs for each run separately.

    # First, get the actual data set (it contains the beamtime ID)
    data_set: None | orm.DataSet = (
        await session.scalars(
            select(orm.DataSet).where(orm.DataSet.id == input_.data_set_id),
        )
    ).one_or_none()

    if data_set is None:
        raise HTTPException(
            status_code=400,
            detail=f"no data set with ID {input_.data_set_id} found",
        )

    beamtime_id = (await data_set.awaitable_attrs.experiment_type).beamtime_id
    runs_in_data_set: list[orm.Run] = await retrieve_runs_matching_data_set(
        session, input_.data_set_id, beamtime_id, source=input_.source.strip()
    )
    if not runs_in_data_set:
        raise HTTPException(
            status_code=400,
            detail=f"data set {input_.data_set_id} doesn't have any runs that have finished and that have files attached to them",
        )

    existing_indexing_results: list[orm.IndexingResult] = list(
        (
            await session.scalars(
                select(orm.IndexingResult)
                .where(orm.IndexingResult.run_id.in_([r.id for r in runs_in_data_set]))
                .options(selectinload(orm.IndexingResult.indexing_parameters)),
            )
        ).all(),
    )
    run_ids_without_matching_indexing_results: set[int] = {
        r.id for r in runs_in_data_set
    }
    new_parameters = orm.IndexingParameters(
        is_online=input_.is_online,
        cell_description=input_.cell_description.strip(),
        command_line=input_.command_line.strip(),
        geometry_id=input_.geometry_id,
        source=input_.source.strip(),
    )
    geometry = (
        await session.scalars(
            select(orm.Geometry).where(orm.Geometry.id == input_.geometry_id)
        )
    ).one()
    for ir in existing_indexing_results:
        # Finished, but erroneous indexing results are no use, they should be redoable
        if ir.job_error is not None:
            continue
        # Run doesn't need to be reprocessed, nice.
        if orm.are_indexing_parameters_equal(ir.indexing_parameters, new_parameters):
            job_logger.info(
                f"indexing parameters {ir.indexing_parameters.id} match {new_parameters}",
            )
            run_ids_without_matching_indexing_results.remove(ir.run_id)

    if not run_ids_without_matching_indexing_results:
        raise HTTPException(
            status_code=400,
            detail=f"With the given parameters, all runs are already indexed properly, so nothing to do here. Specifically, for data set {input_.data_set_id} I found these runs (internal IDs): "
            + ", ".join(str(r.id) for r in runs_in_data_set)
            + ", and the following indexing results: "
            + ", ".join(str(ir.id) for ir in existing_indexing_results),
        )

    session.add(new_parameters)
    await session.flush()

    runs_to_start_jobs_for: list[orm.Run] = [
        r for r in runs_in_data_set if r.id in run_ids_without_matching_indexing_results
    ]
    for run in runs_to_start_jobs_for:
        if run.id not in run_ids_without_matching_indexing_results:
            continue
        job_logger.info(
            f"creating indexing result for run {run.id} (external ID {run.external_id})",
        )
        indexing_result = orm.IndexingResult(
            created=datetime.datetime.now(datetime.timezone.utc),
            run_id=run.id,
            stream_file=None,
            frames=0,
            hits=0,
            indexed_frames=0,
            unit_cell_histograms_file_id=None,
            generated_geometry_id=None,
            job_id=None,
            # program version will be determined by the job itself
            program_version="",
            job_status=DBJobStatus.QUEUED,
            job_error=None,
            job_started=None,
            job_stopped=None,
            job_latest_log="",
            indexing_parameters_id=new_parameters.id,
        )
        await add_geometry_template_replacements_to_ir(indexing_result, run, geometry)
        session.add(indexing_result)

    await session.commit()
    return JsonCreateIndexingForDataSetOutput(
        jobs_started_run_external_ids=[
            run.external_id for run in runs_to_start_jobs_for
        ],
        indexing_result_id=indexing_result.id,
        data_set_id=input_.data_set_id,
        indexing_parameters_id=new_parameters.id,
    )


@router.get(
    "/api/indexing/{indexingResultId}/log",
    tags=["processing"],
    response_model_exclude_defaults=True,
    response_class=PlainTextResponse,
)
async def indexing_job_get_log(
    indexingResultId: int,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> str:
    async with session.begin():
        result = (
            (
                await session.scalars(
                    select(orm.IndexingResult).where(
                        orm.IndexingResult.id == indexingResultId,
                    ),
                )
            ).one()
        ).job_latest_log
        return result if result is not None else ""


@router.get(
    "/api/indexing/{indexingResultId}/errorlog",
    tags=["processing"],
    response_model_exclude_defaults=True,
    response_class=PlainTextResponse,
)
async def indexing_job_get_errorlog(
    indexingResultId: int,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> str:
    async with session.begin():
        result = (
            (
                await session.scalars(
                    select(orm.IndexingResult).where(
                        orm.IndexingResult.id == indexingResultId,
                    ),
                )
            ).one()
        ).job_error
        return result if result is not None else ""


@router.post(
    "/api/indexing/{indexingResultId}/finish-with-error",
    tags=["processing"],
    response_model_exclude_defaults=True,
)
async def indexing_job_finish_with_error(
    indexingResultId: int,  # noqa: N803
    json_result: JsonIndexingResultFinishWithError,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonIndexingJobUpdateOutput:
    job_logger = logger.bind(
        indexing_result_id=indexingResultId,
        workload_manager_job_id=json_result.workload_manager_job_id,
    )
    job_logger.info("signal error")

    async with session.begin():
        current_indexing_result = (
            await session.scalars(
                select(orm.IndexingResult).where(
                    orm.IndexingResult.id == indexingResultId,
                ),
            )
        ).one_or_none()
        if current_indexing_result is None:
            job_logger.error(f"indexing result with ID {indexingResultId} not found")
            return JsonIndexingJobUpdateOutput(result=False)
        if json_result.workload_manager_job_id is not None:
            current_indexing_result.job_id = json_result.workload_manager_job_id
        current_indexing_result.job_error = json_result.error_message
        current_indexing_result.job_status = DBJobStatus.DONE
        if json_result.latest_log:
            current_indexing_result.job_latest_log = json_result.latest_log
        current_indexing_result.job_stopped = datetime.datetime.now(
            tz=datetime.timezone.utc,
        )
        # Pathological case
        if current_indexing_result.job_started is None:
            current_indexing_result.job_started = current_indexing_result.job_stopped

        await session.commit()
    return JsonIndexingJobUpdateOutput(result=True)


@router.post(
    "/api/indexing/{indexingResultId}/still-running",
    tags=["processing"],
    response_model_exclude_defaults=True,
)
async def indexing_job_still_running(
    indexingResultId: int,  # noqa: N803
    json_result: JsonIndexingResultStillRunning,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonIndexingJobUpdateOutput:
    job_logger = logger.bind(
        indexing_result_id=indexingResultId,
        workload_manager_job_id=json_result.workload_manager_job_id,
    )
    job_logger.info("update still running")

    async with session.begin():
        current_indexing_result = (
            await session.scalars(
                select(orm.IndexingResult).where(
                    orm.IndexingResult.id == indexingResultId,
                ),
            )
        ).one_or_none()
        if current_indexing_result is None:
            job_logger.error(f"indexing result with ID {indexingResultId} not found")
            return JsonIndexingJobUpdateOutput(result=False)
        current_indexing_result.job_id = json_result.workload_manager_job_id
        current_indexing_result.stream_file = json_result.stream_file
        jr = json_result
        current_indexing_result.job_status = DBJobStatus.RUNNING
        current_indexing_result.frames = jr.frames
        current_indexing_result.hits = jr.hits
        current_indexing_result.indexed_frames = jr.indexed_frames
        # job started can be missing, in case we don't have that information
        if jr.job_started is not None:
            current_indexing_result.job_started = utc_int_to_utc_datetime(
                jr.job_started,
            )
        if json_result.latest_log is not None:
            current_indexing_result.job_latest_log = json_result.latest_log
        session.add(
            orm.IndexingResultHasStatistic(
                indexing_result_id=current_indexing_result.id,
                time=datetime.datetime.now(datetime.timezone.utc),
                frames=jr.frames,
                hits=jr.hits,
                indexed_frames=jr.indexed_frames,
                indexed_crystals=jr.indexed_crystals,
            ),
        )

        await session.commit()
    return JsonIndexingJobUpdateOutput(result=True)


@router.post(
    "/api/indexing/{indexingResultId}/success",
    tags=["processing"],
    response_model_exclude_defaults=True,
)
async def indexing_job_finish_successfully(
    indexingResultId: int,  # noqa: N803
    json_result: JsonIndexingResultFinishSuccessfully,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonIndexingJobUpdateOutput:
    job_logger = logger.bind(
        indexing_result_id=indexingResultId,
        workload_manager_job_id=json_result.workload_manager_job_id,
    )
    job_logger.info("success")

    async with session.begin():
        current_indexing_result = (
            await session.scalars(
                select(orm.IndexingResult)
                .where(
                    orm.IndexingResult.id == indexingResultId,
                )
                .options(selectinload(orm.IndexingResult.run)),
            )
        ).one_or_none()
        if current_indexing_result is None:
            job_logger.error(f"indexing result with ID {indexingResultId} not found")
            return JsonIndexingJobUpdateOutput(result=False)
        jr = json_result
        current_indexing_result.job_status = DBJobStatus.DONE
        current_indexing_result.stream_file = jr.stream_file
        current_indexing_result.program_version = jr.program_version
        current_indexing_result.job_id = jr.workload_manager_job_id
        current_indexing_result.job_status = DBJobStatus.DONE
        current_indexing_result.frames = jr.frames
        current_indexing_result.hits = jr.hits
        current_indexing_result.unit_cell_histograms_file_id = (
            json_result.unit_cell_histograms_id
        )
        current_indexing_result.job_stopped = datetime.datetime.now(
            tz=datetime.timezone.utc,
        )
        # Pathological case
        if current_indexing_result.job_started is None:
            current_indexing_result.job_started = current_indexing_result.job_stopped
        current_indexing_result.indexed_frames = jr.indexed_frames

        if jr.generated_geometry_contents:
            geometry = await _locate_or_create_geometry(
                session,
                current_indexing_result.run.beamtime_id,
                f"import for run {current_indexing_result.run.external_id}, indexing ID {indexingResultId}",
                jr.generated_geometry_contents,
            )
            current_indexing_result.generated_geometry_id = geometry.id
        if json_result.latest_log is not None:
            current_indexing_result.job_latest_log = json_result.latest_log
        for g in json_result.align_detector_groups:
            current_indexing_result.align_detector_groups.append(
                orm.AlignDetectorGroup(
                    group=g.group,
                    x_translation_mm=g.x_translation_mm,
                    y_translation_mm=g.y_translation_mm,
                    z_translation_mm=g.z_translation_mm,
                    x_rotation_deg=g.x_rotation_deg,
                    y_rotation_deg=g.y_rotation_deg,
                )
            )

        await session.commit()
    return JsonIndexingJobUpdateOutput(result=True)


# Weird to have this under a different path, I know. But I wasn't sure
# what to do with this thing. It's part of the whole "indexing"
# scenario. But I didn't want to break the nice "/api/indexing/bla"
# hierarchy (where "bla" refers to a concrete, existing or to be
# created, indexing job, instead of parameters).
@router.get(
    "/api/indexing-parameters/{dataSetId}",
    tags=["processing"],
    response_model_exclude_defaults=True,
)
async def read_indexing_parameters(
    dataSetId: int,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadIndexingParametersOutput:
    data_set = (
        await session.scalars(
            select(orm.DataSet)
            .where(orm.DataSet.id == dataSetId)
            .options(
                selectinload(orm.DataSet.attributo_values)
                .selectinload(orm.DataSetHasAttributoValue.chemical)
                .selectinload(orm.Chemical.attributo_values)
                .selectinload(orm.ChemicalHasAttributoValue.attributo),
            ),
        )
    ).one()

    cell_descriptions: list[str] = [
        c_av.string_value
        for ds_av in data_set.attributo_values
        if ds_av.chemical is not None
        for c_av in ds_av.chemical.attributo_values
        if c_av.attributo.name == CELL_DESCRIPTION_ATTRIBUTO and c_av.string_value
    ]

    beamtime_id = (await data_set.awaitable_attrs.experiment_type).beamtime_id
    runs_in_data_set: list[orm.Run] = await retrieve_runs_matching_data_set(
        session,
        dataSetId,
        beamtime_id,
    )
    sources: set[str] = set()
    for run in runs_in_data_set:
        for has_file in await run.awaitable_attrs.files:
            sources.add(has_file.source)

    return JsonReadIndexingParametersOutput(
        data_set_id=dataSetId,
        cell_description=cell_descriptions[0] if cell_descriptions else "",
        sources=list(sources),
        geometries=await all_geometry_metadatas(session, beamtime_id),
    )


@router.get(
    "/api/indexing",
    tags=["processing"],
    response_model_exclude_defaults=True,
)
async def read_indexing_jobs(
    session: Annotated[AsyncSession, Depends(get_orm_db)],
    status: None | DBJobStatus = None,
    beamtimeId: None | int = None,  # noqa: N803
    withFiles: bool = False,  # noqa: N803, FBT002
) -> JsonReadIndexingResultsOutput:
    return JsonReadIndexingResultsOutput(
        indexing_jobs=[
            await json_indexing_job_from_orm(
                ij,
                withFiles,
            )
            for ij in await session.scalars(
                select(orm.IndexingResult)
                .join(orm.Run, orm.IndexingResult.run_id == orm.Run.id)
                .options(
                    selectinload(orm.IndexingResult.run).selectinload(orm.Run.beamtime)
                    if not withFiles
                    else selectinload(orm.IndexingResult.run).selectinload(
                        orm.Run.beamtime,
                    ),
                )
                .options(selectinload(orm.IndexingResult.indexing_parameters))
                .where(
                    (
                        orm.IndexingResult.job_status == status
                        if status is not None
                        else true()
                    )
                    & (
                        orm.Run.beamtime_id == beamtimeId
                        if beamtimeId is not None
                        else true()
                    ),
                ),
            )
        ],
    )
