import structlog
from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import delete
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonBeamtimeSchedule
from amarcord.web.json_models import JsonBeamtimeScheduleOutput
from amarcord.web.json_models import JsonBeamtimeScheduleRow
from amarcord.web.json_models import JsonUpdateBeamtimeScheduleInput

logger = structlog.stdlib.get_logger(__name__)

router = APIRouter()


@router.get(
    "/api/schedule/{beamtimeId}",
    tags=["schedule"],
    response_model_exclude_defaults=True,
)
async def get_beamtime_schedule(
    beamtimeId: BeamtimeId, session: AsyncSession = Depends(get_orm_db)
) -> JsonBeamtimeSchedule:
    return JsonBeamtimeSchedule(
        schedule=[
            JsonBeamtimeScheduleRow(
                users=shift_dict.users,
                date=shift_dict.date,
                shift=shift_dict.shift,
                comment=shift_dict.comment,
                td_support=shift_dict.td_support,
                chemicals=[c.id for c in shift_dict.chemicals],
            )
            for shift_dict in await session.scalars(
                select(orm.BeamtimeSchedule).where(
                    orm.BeamtimeSchedule.beamtime_id == beamtimeId
                )
            )
        ]
    )


@router.post("/api/schedule", tags=["schedule"], response_model_exclude_defaults=True)
async def update_beamtime_schedule(
    input_: JsonUpdateBeamtimeScheduleInput, session: AsyncSession = Depends(get_orm_db)
) -> JsonBeamtimeScheduleOutput:
    async with session.begin():
        await session.execute(
            delete(orm.BeamtimeSchedule).where(
                orm.BeamtimeSchedule.beamtime_id == input_.beamtime_id
            )
        )
        for shift_dict in input_.schedule:
            session.add(
                orm.BeamtimeSchedule(
                    beamtime_id=input_.beamtime_id,
                    users=shift_dict.users,
                    date=shift_dict.date,
                    shift=shift_dict.shift,
                    comment=shift_dict.comment,
                    td_support=shift_dict.td_support,
                    # probably super slow, because one select per
                    # shift, but this is just a beamtime schedule, nobody
                    # cares...
                    chemicals=list(
                        (
                            await session.scalars(
                                select(orm.Chemical).where(
                                    orm.Chemical.id.in_(shift_dict.chemicals)
                                )
                            )
                        ).all()
                    ),
                )
            )
        await session.commit()
        return JsonBeamtimeScheduleOutput(schedule=input_.schedule)
