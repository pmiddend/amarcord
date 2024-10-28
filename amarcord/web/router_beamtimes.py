from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.attributi import datetime_from_attributo_int
from amarcord.db.orm_utils import encode_beamtime
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonBeamtime
from amarcord.web.json_models import JsonBeamtimeOutput
from amarcord.web.json_models import JsonReadBeamtime
from amarcord.web.json_models import JsonUpdateBeamtimeInput

router = APIRouter()


@router.post("/api/beamtimes", tags=["beamtimes"])
async def create_beamtime(
    input_: JsonUpdateBeamtimeInput, session: AsyncSession = Depends(get_orm_db)
) -> JsonBeamtimeOutput:
    async with session.begin():
        new_beamtime = orm.Beamtime(
            external_id=input_.external_id,
            beamline=input_.beamline,
            proposal=input_.proposal,
            title=input_.title,
            comment=input_.comment,
            start=datetime_from_attributo_int(input_.start),
            end=datetime_from_attributo_int(input_.end),
        )
        session.add(new_beamtime)
        # we need to the ID in the next line, so have to flush
        await session.flush()
        return JsonBeamtimeOutput(id=new_beamtime.id)


@router.patch("/api/beamtimes", tags=["beamtimes"])
async def update_beamtime(
    input_: JsonUpdateBeamtimeInput, session: AsyncSession = Depends(get_orm_db)
) -> JsonBeamtimeOutput:
    async with session.begin():
        existing_beamtime = (
            await session.scalars(
                select(orm.Beamtime).where(orm.Beamtime.id == input_.id)
            )
        ).one()

        existing_beamtime.external_id = input_.external_id
        existing_beamtime.beamline = input_.beamline
        existing_beamtime.proposal = input_.proposal
        existing_beamtime.title = input_.title
        existing_beamtime.comment = input_.comment
        existing_beamtime.start = datetime_from_attributo_int(input_.start)
        existing_beamtime.end = datetime_from_attributo_int(input_.end)
        return JsonBeamtimeOutput(id=input_.id)


@router.get("/api/beamtimes", tags=["beamtimes"], response_model_exclude_defaults=True)
async def read_beamtimes(
    session: AsyncSession = Depends(get_orm_db),
) -> JsonReadBeamtime:
    return JsonReadBeamtime(
        beamtimes=[
            encode_beamtime(bt)
            for bt in await session.scalars(
                select(orm.Beamtime)
                .options(selectinload(orm.Beamtime.chemicals))
                .order_by(orm.Beamtime.start.desc())
            )
        ]
    )


@router.get(
    "/api/beamtimes/{beamtimeId}",
    tags=["beamtimes"],
    response_model_exclude_defaults=True,
)
async def read_beamtime(
    beamtimeId: int, session: AsyncSession = Depends(get_orm_db)
) -> JsonBeamtime:
    return encode_beamtime(
        (
            await session.scalars(
                select(orm.Beamtime)
                .where(orm.Beamtime.id == beamtimeId)
                .options(selectinload(orm.Beamtime.chemicals))
            )
        ).one()
    )
