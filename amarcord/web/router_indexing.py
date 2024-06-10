import datetime
from statistics import mean

import structlog
from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy import true
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import empty_indexing_fom
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonBeamtime
from amarcord.web.json_models import JsonIndexingFom
from amarcord.web.json_models import JsonIndexingJob
from amarcord.web.json_models import JsonIndexingJobUpdateOutput
from amarcord.web.json_models import JsonIndexingResultRootJson
from amarcord.web.json_models import JsonReadIndexingResultsOutput

logger = structlog.stdlib.get_logger(__name__)
router = APIRouter()


def json_indexing_job_from_orm(ij: orm.IndexingResult) -> JsonIndexingJob:
    return JsonIndexingJob(
        id=ij.id,
        job_id=ij.job_id,
        job_status=ij.job_status,
        stream_file=ij.stream_file,
        cell_description=ij.cell_description,
        run_external_id=ij.run.external_id,
        run_internal_id=ij.run.id,
        beamtime=JsonBeamtime(
            id=ij.run.beamtime.id,
            external_id=ij.run.beamtime.external_id,
            proposal=ij.run.beamtime.proposal,
            beamline=ij.run.beamtime.beamline,
            title=ij.run.beamtime.title,
            comment=ij.run.beamtime.comment,
            start=datetime_to_attributo_int(ij.run.beamtime.start),
            end=datetime_to_attributo_int(ij.run.beamtime.end),
            chemical_names=[],
        ),
    )


def encode_summary(summary: DBIndexingFOM) -> JsonIndexingFom:
    return JsonIndexingFom(
        hit_rate=summary.hit_rate,
        indexing_rate=summary.indexing_rate,
        indexed_frames=summary.indexed_frames,
        detector_shift_x_mm=summary.detector_shift_x_mm,
        detector_shift_y_mm=summary.detector_shift_y_mm,
    )


def fom_for_indexing_result(jr: orm.IndexingResult) -> DBIndexingFOM:
    assert jr.hits is not None
    assert jr.frames is not None
    assert jr.indexed_frames is not None
    return DBIndexingFOM(
        hit_rate=jr.hit_rate,
        indexing_rate=jr.indexing_rate,
        indexed_frames=jr.indexed_frames,
        detector_shift_x_mm=jr.detector_shift_x_mm,
        detector_shift_y_mm=jr.detector_shift_y_mm,
    )


def summary_from_foms(ir: list[DBIndexingFOM]) -> DBIndexingFOM:
    if not ir:
        return empty_indexing_fom
    shifts_x = [e.detector_shift_x_mm for e in ir if e.detector_shift_x_mm is not None]
    shifts_y = [e.detector_shift_y_mm for e in ir if e.detector_shift_y_mm is not None]
    return DBIndexingFOM(
        hit_rate=mean(x.hit_rate for x in ir),
        indexing_rate=mean(x.indexing_rate for x in ir),
        indexed_frames=sum(x.indexed_frames for x in ir),
        detector_shift_x_mm=mean(shifts_x) if shifts_x else None,
        detector_shift_y_mm=mean(shifts_y) if shifts_y else None,
    )


@router.post(
    "/api/indexing/{indexingResultId}",
    tags=["analysis", "processing"],
    response_model_exclude_defaults=True,
)
async def indexing_job_update(
    indexingResultId: int,
    json_result: JsonIndexingResultRootJson,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonIndexingJobUpdateOutput:
    job_logger = logger.bind(indexing_result_id=indexingResultId)
    job_logger.info("update")

    async with session.begin():
        current_indexing_result = (
            await session.scalars(
                select(orm.IndexingResult).where(
                    orm.IndexingResult.id == indexingResultId
                )
            )
        ).one_or_none()
        if current_indexing_result is None:
            job_logger.error(f"indexing result with ID {indexingResultId} not found")
            return JsonIndexingJobUpdateOutput(result=False)
        if json_result.job_id is not None:
            current_indexing_result.job_id = json_result.job_id
        current_indexing_result.stream_file = json_result.stream_file
        if json_result.error is not None:
            current_indexing_result.job_error = json_result.error
            current_indexing_result.job_status = DBJobStatus.DONE
        elif json_result.result is not None:
            jr = json_result.result
            current_indexing_result.job_status = (
                DBJobStatus.DONE if jr.done else DBJobStatus.RUNNING
            )
            current_indexing_result.frames = jr.frames
            current_indexing_result.hits = jr.hits
            current_indexing_result.indexed_frames = jr.indexed_frames
            current_indexing_result.indexing_rate = (
                jr.indexed_frames / jr.hits * 100 if jr.hits != 0 else 0.0
            )
            current_indexing_result.hit_rate = (
                jr.hits / jr.frames * 100.0 if jr.frames != 0 else 0
            )
            current_indexing_result.detector_shift_x_mm = jr.detector_shift_x_mm
            current_indexing_result.detector_shift_y_mm = jr.detector_shift_y_mm
            session.add(
                orm.IndexingResultHasStatistic(
                    indexing_result_id=current_indexing_result.id,
                    time=datetime.datetime.now(datetime.timezone.utc),
                    frames=jr.frames,
                    hits=jr.hits,
                    indexed_frames=jr.indexed_frames,
                    indexed_crystals=jr.indexed_crystals,
                )
            )
        else:
            job_logger.error(
                "couldn't parse indexing result: both error and result are None"
            )
            return JsonIndexingJobUpdateOutput(result=False)

        await session.commit()
    return JsonIndexingJobUpdateOutput(result=True)


@router.get(
    "/api/indexing",
    tags=["analysis", "processing"],
    response_model_exclude_defaults=True,
)
async def read_indexing_jobs(
    status: None | DBJobStatus,
    beamtimeId: None | int = None,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonReadIndexingResultsOutput:
    result = JsonReadIndexingResultsOutput(
        indexing_jobs=[
            json_indexing_job_from_orm(ij)
            for ij in await session.scalars(
                select(orm.IndexingResult)
                .join(orm.Run, orm.IndexingResult.run_id == orm.Run.id)
                .options(
                    selectinload(orm.IndexingResult.run).selectinload(orm.Run.beamtime)
                )
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
                    )
                )
            )
        ]
    )
    return result
