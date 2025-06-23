import datetime
from typing import Annotated

import structlog
from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import delete
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.attributi import utc_datetime_to_local_int
from amarcord.db.attributi import utc_datetime_to_utc_int
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.util import get_local_tz
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonBeamtimeScheduleOutput
from amarcord.web.json_models import JsonBeamtimeScheduleRowOutput
from amarcord.web.json_models import JsonUpdateBeamtimeScheduleInput

logger = structlog.stdlib.get_logger(__name__)

router = APIRouter()


async def _get_current_schedule(
    beamtime_id: BeamtimeId, session: AsyncSession
) -> JsonBeamtimeScheduleOutput:
    def convert_row(shift_dict: orm.BeamtimeSchedule) -> JsonBeamtimeScheduleRowOutput:
        shift_parts = [x.strip() for x in shift_dict.shift.split("-", maxsplit=2)]

        if len(shift_parts) != 2:
            raise Exception(
                "invalid schedule, shift {shift_dict.shift} doesn't have start/end"
            )

        shift_start, shift_end = shift_parts

        start_utc = (
            datetime.datetime.strptime(
                f"{shift_dict.date} {shift_start}", "%Y-%m-%d %H:%M"
            )
            .replace(tzinfo=get_local_tz())
            .astimezone(datetime.timezone.utc)
        )
        stop_utc = (
            datetime.datetime.strptime(
                f"{shift_dict.date} {shift_end}", "%Y-%m-%d %H:%M"
            )
            .replace(tzinfo=get_local_tz())
            .astimezone(datetime.timezone.utc)
        )

        return JsonBeamtimeScheduleRowOutput(
            users=shift_dict.users,
            date=shift_dict.date,
            shift=shift_dict.shift,
            comment=shift_dict.comment,
            td_support=shift_dict.td_support,
            chemicals=[c.id for c in shift_dict.chemicals],
            start=utc_datetime_to_utc_int(start_utc),
            start_local=utc_datetime_to_local_int(start_utc),
            stop=utc_datetime_to_utc_int(stop_utc),
            stop_local=utc_datetime_to_local_int(stop_utc),
        )

    return JsonBeamtimeScheduleOutput(
        schedule=[
            convert_row(shift_dict)
            for shift_dict in await session.scalars(
                select(orm.BeamtimeSchedule).where(
                    orm.BeamtimeSchedule.beamtime_id == beamtime_id,
                ),
            )
        ],
    )


@router.get(
    "/api/schedule/{beamtimeId}",
    tags=["schedule"],
    response_model_exclude_defaults=True,
)
async def get_beamtime_schedule(
    beamtimeId: BeamtimeId,  # noqa: N803
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonBeamtimeScheduleOutput:
    return await _get_current_schedule(beamtimeId, session)


@router.post("/api/schedule", tags=["schedule"], response_model_exclude_defaults=True)
async def update_beamtime_schedule(
    input_: JsonUpdateBeamtimeScheduleInput,
    session: Annotated[AsyncSession, Depends(get_orm_db)],
) -> JsonBeamtimeScheduleOutput:
    async with session.begin():
        await session.execute(
            delete(orm.BeamtimeSchedule).where(
                orm.BeamtimeSchedule.beamtime_id == input_.beamtime_id,
            ),
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
                                    orm.Chemical.id.in_(shift_dict.chemicals),
                                ),
                            )
                        ).all(),
                    ),
                ),
            )
        return await _get_current_schedule(input_.beamtime_id, session)
