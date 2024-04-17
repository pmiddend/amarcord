import datetime

from fastapi import APIRouter
from fastapi import Depends
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import delete
from sqlalchemy.sql import select

from amarcord.db import orm
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.orm_utils import live_stream_image_name
from amarcord.web.fastapi_utils import get_orm_db
from amarcord.web.json_models import JsonDeleteEventInput
from amarcord.web.json_models import JsonDeleteEventOutput
from amarcord.web.json_models import JsonEvent
from amarcord.web.json_models import JsonEventTopLevelInput
from amarcord.web.json_models import JsonEventTopLevelOutput
from amarcord.web.json_models import JsonReadEvents
from amarcord.web.router_files import encode_file_output

router = APIRouter()


@router.post("/api/events", tags=["events"])
async def create_event(
    input_: JsonEventTopLevelInput, session: AsyncSession = Depends(get_orm_db)
) -> JsonEventTopLevelOutput:
    beamtime_id = input_.beamtime_id

    async with session.begin():
        event = input_.event

        new_event = orm.EventLog(
            beamtime_id=beamtime_id,
            level=EventLogLevel(event.level),
            source=event.source,
            text=event.text,
            created=datetime.datetime.utcnow(),
        )
        new_event.files.extend(
            await session.scalars(
                select(orm.File).where(orm.File.id.in_(event.fileIds))
            )
        )
        if input_.with_live_stream:
            existing_live_stream = (
                await session.scalars(
                    select(orm.File).where(
                        orm.File.file_name == live_stream_image_name(beamtime_id)
                    )
                )
            ).one_or_none()
            if existing_live_stream is not None:
                # copy it, because the live stream is updated in-place
                new_file_name = live_stream_image_name(beamtime_id) + "-copy"
                copied_live_stream = orm.File(
                    type=existing_live_stream.type,
                    modified=datetime.datetime.utcnow(),
                    # The contents aren't loaded by default (since
                    # they might be huge). And we're risking OOM here
                    # for big files, since we're reading it in memory,
                    # but we'll risk it for now. There doesn't seem to
                    # be an sqlalchemy streaming API anyway, see:
                    #
                    # https://stackoverflow.com/questions/55043981/stream-blob-to-mysql-with-sqlalchemy
                    contents=await existing_live_stream.awaitable_attrs.contents,
                    file_name=new_file_name,
                    original_path=existing_live_stream.original_path,
                    description=existing_live_stream.description,
                    sha256=existing_live_stream.sha256,
                    size_in_bytes=existing_live_stream.size_in_bytes,
                )
                new_event.files.append(copied_live_stream)
        session.add(new_event)
        await session.commit()
        return JsonEventTopLevelOutput(id=new_event.id)


@router.delete("/api/events", tags=["events"], response_model_exclude_defaults=True)
async def delete_event(
    input_: JsonDeleteEventInput, session: AsyncSession = Depends(get_orm_db)
) -> JsonDeleteEventOutput:
    async with session.begin():
        await session.execute(delete(orm.EventLog).where(orm.EventLog.id == input_.id))
        await session.commit()

    return JsonDeleteEventOutput(result=True)


def encode_event(e: orm.EventLog) -> JsonEvent:
    return JsonEvent(
        id=e.id,
        text=e.text,
        source=e.source,
        created=datetime_to_attributo_int(e.created),
        level=e.level.value,
        files=[encode_file_output(f) for f in e.files],
    )


@router.get(
    "/api/events/{beamtimeId}", tags=["events"], response_model_exclude_defaults=True
)
async def read_events(
    beamtimeId: BeamtimeId,
    session: AsyncSession = Depends(get_orm_db),
) -> JsonReadEvents:
    return JsonReadEvents(
        events=[
            encode_event(e)
            for e in await session.scalars(
                select(orm.EventLog)
                .where(orm.EventLog.beamtime_id == beamtimeId)
                .order_by(orm.EventLog.created)
                # The list contains the files for each event, so we might as well load it here directly
                .options(selectinload(orm.EventLog.files))
            )
        ]
    )
