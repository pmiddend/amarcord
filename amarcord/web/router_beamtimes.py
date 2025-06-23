from typing import Annotated

from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.attributi import local_int_to_utc_datetime
from amarcord.db.attributi import utc_datetime_to_local_int
from amarcord.db.attributi import utc_datetime_to_utc_int
from amarcord.db.orm_utils import encode_beamtime
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonBeamtimeInput
from amarcord.web.json_models import JsonBeamtimeOutput
from amarcord.web.json_models import JsonReadBeamtime
from amarcord.web.json_models import JsonUpdateBeamtimeInput

router = APIRouter()


@router.post("/api/beamtimes", tags=["beamtimes"])
async def create_beamtime(
    input_: JsonBeamtimeInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonBeamtimeOutput:
    async with session.begin():
        new_beamtime = orm.Beamtime(
            external_id=input_.external_id,
            beamline=input_.beamline,
            proposal=input_.proposal,
            title=input_.title,
            comment=input_.comment,
            start=local_int_to_utc_datetime(input_.start_local),
            end=local_int_to_utc_datetime(input_.end_local),
            analysis_output_path=input_.analysis_output_path,
        )
        session.add(new_beamtime)
        # we need to the ID in the next line, so have to flush
        await session.flush()
        return JsonBeamtimeOutput(
            id=new_beamtime.id,
            external_id=new_beamtime.external_id,
            proposal=new_beamtime.proposal,
            beamline=new_beamtime.beamline,
            title=new_beamtime.title,
            comment=new_beamtime.comment,
            start=utc_datetime_to_utc_int(new_beamtime.start),
            start_local=utc_datetime_to_local_int(new_beamtime.start),
            end=utc_datetime_to_utc_int(new_beamtime.end),
            end_local=utc_datetime_to_local_int(new_beamtime.end),
            analysis_output_path=new_beamtime.analysis_output_path,
            chemical_names=[],
        )


@router.patch("/api/beamtimes", tags=["beamtimes"])
async def update_beamtime(
    input_: JsonUpdateBeamtimeInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonBeamtimeOutput:
    async with session.begin():
        existing_beamtime = (
            await session.scalars(
                select(orm.Beamtime).where(orm.Beamtime.id == input_.id),
            )
        ).one()

        existing_beamtime.external_id = input_.external_id
        existing_beamtime.beamline = input_.beamline
        existing_beamtime.proposal = input_.proposal
        existing_beamtime.title = input_.title
        existing_beamtime.comment = input_.comment
        existing_beamtime.start = local_int_to_utc_datetime(input_.start_local)
        existing_beamtime.end = local_int_to_utc_datetime(input_.end_local)
        existing_beamtime.analysis_output_path = input_.analysis_output_path

        await session.flush()
        return JsonBeamtimeOutput(
            id=existing_beamtime.id,
            external_id=existing_beamtime.external_id,
            proposal=existing_beamtime.proposal,
            beamline=existing_beamtime.beamline,
            title=existing_beamtime.title,
            comment=existing_beamtime.comment,
            start=utc_datetime_to_utc_int(existing_beamtime.start),
            start_local=utc_datetime_to_local_int(existing_beamtime.start),
            end=utc_datetime_to_utc_int(existing_beamtime.end),
            end_local=utc_datetime_to_local_int(existing_beamtime.end),
            analysis_output_path=existing_beamtime.analysis_output_path,
            chemical_names=[],
        )


@router.get("/api/beamtimes", tags=["beamtimes"], response_model_exclude_defaults=True)
async def read_beamtimes(
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonReadBeamtime:
    return JsonReadBeamtime(
        beamtimes=[
            encode_beamtime(bt, with_chemicals=True)
            for bt in await session.scalars(
                select(orm.Beamtime)
                .options(selectinload(orm.Beamtime.chemicals))
                .order_by(orm.Beamtime.start.desc()),
            )
        ],
    )


@router.get(
    "/api/beamtimes/{beamtimeId}",
    tags=["beamtimes"],
    response_model_exclude_defaults=True,
)
async def read_beamtime(
    beamtimeId: int,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonBeamtimeOutput:
    return encode_beamtime(
        (
            await session.scalars(
                select(orm.Beamtime)
                .where(orm.Beamtime.id == beamtimeId)
                .options(selectinload(orm.Beamtime.chemicals)),
            )
        ).one(),
        with_chemicals=True,
    )
