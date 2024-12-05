import datetime
import os

import pytz
import structlog
from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.sql import delete
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.attributi import datetime_to_attributo_int
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
    current_tz = pytz.timezone(os.environ.get("AMARCORD_TZ", "Europe/Berlin"))

    def convert_to_posix(date_time_str: str) -> int:
        return datetime_to_attributo_int(
            current_tz.localize(
                datetime.datetime.strptime(date_time_str, "%Y-%m-%d %H:%M")
            ).astimezone(pytz.utc)
        )

    def convert_start_end_to_posix(shift_dict: orm.BeamtimeSchedule) -> tuple[int, int]:
        shift_parts = [x.strip() for x in shift_dict.shift.split("-", maxsplit=2)]
        if len(shift_parts) != 2:
            return 0, 0
        shift_start, shift_end = shift_parts
        return convert_to_posix(f"{shift_dict.date} {shift_start}"), convert_to_posix(
            f"{shift_dict.date} {shift_end}"
        )

    return JsonBeamtimeSchedule(
        schedule=[
            JsonBeamtimeScheduleRow(
                users=shift_dict.users,
                date=shift_dict.date,
                shift=shift_dict.shift,
                comment=shift_dict.comment,
                td_support=shift_dict.td_support,
                chemicals=[c.id for c in shift_dict.chemicals],
                start_posix=convert_start_end_to_posix(shift_dict)[0],
                stop_posix=convert_start_end_to_posix(shift_dict)[1],
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
