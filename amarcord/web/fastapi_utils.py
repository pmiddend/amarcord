import datetime
import json
import os
from typing import Any
from typing import AsyncGenerator
from typing import Final
from typing import Iterable

import structlog
from fastapi import HTTPException
from sqlalchemy import NullPool
from sqlalchemy import StaticPool
from sqlalchemy import event
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import async_sessionmaker
from sqlalchemy.ext.asyncio import create_async_engine

from amarcord.db import orm
from amarcord.db.attributi import datetime_from_attributo_int
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.orm_utils import validate_json_attributo_return_error
from amarcord.util import create_intervals
from amarcord.web.json_models import JsonAttributoValue

DATE_FORMAT: Final = "%Y-%m-%d"
ELVEFLOW_OB1_MAX_NUMBER_OF_CHANNELS: Final = 4
AUTOMATIC_ATTRIBUTI_GROUP: Final = "automatic"


def _json_serializer_allow_nan_false(obj: Any, **kwargs: Any) -> str:
    return json.dumps(obj, **kwargs, allow_nan=False)


async def get_orm_db() -> AsyncGenerator[AsyncSession, None]:
    db_url: Any = os.environ["DB_URL"]

    # For the sqlite in-memory stuff, see here:
    #
    # https://docs.sqlalchemy.org/en/13/dialects/sqlite.html#threading-pooling-behavior
    #
    # In short, with in-memory databases, we normally get one DB per thread, which is bad if we have
    # background threads updating the DB (in test scenarios, of course).
    in_memory_db = db_url == "sqlite+aiosqlite://"

    engine = create_async_engine(
        db_url,
        echo="DB_ECHO" in os.environ,
        connect_args={"check_same_thread": False} if in_memory_db else {},
        poolclass=StaticPool if in_memory_db else NullPool,
        json_serializer=_json_serializer_allow_nan_false,
    )

    # sqlite doesn't care about foreign keys unless you do this dance, see
    # https://stackoverflow.com/questions/2614984/sqlite-sqlalchemy-how-to-enforce-foreign-keys
    def _fk_pragma_on_connect(dbapi_con: Any, _con_record: Any) -> None:
        dbapi_con.execute("pragma foreign_keys=ON")

    if "sqlite" in db_url:
        event.listen(engine.sync_engine, "connect", _fk_pragma_on_connect)

    async_session = async_sessionmaker(engine, expire_on_commit=False)

    async with async_session() as session:
        yield session


def json_attributo_to_data_set_orm_attributo(
    new_attributo: JsonAttributoValue,
) -> orm.DataSetHasAttributoValue:
    return orm.DataSetHasAttributoValue(
        attributo_id=new_attributo.attributo_id,
        integer_value=new_attributo.attributo_value_int,
        float_value=new_attributo.attributo_value_float,
        string_value=new_attributo.attributo_value_str,
        bool_value=new_attributo.attributo_value_bool,
        datetime_value=(
            datetime_from_attributo_int(new_attributo.attributo_value_datetime)
            if new_attributo.attributo_value_datetime
            else None
        ),
        list_value=(
            new_attributo.attributo_value_list_str
            if new_attributo.attributo_value_list_str
            else (
                new_attributo.attributo_value_list_float
                if new_attributo.attributo_value_list_float
                else (
                    new_attributo.attributo_value_list_bool
                    if new_attributo.attributo_value_list_bool
                    else None
                )
            )
        ),
        chemical_value=new_attributo.attributo_value_chemical,
    )


def json_attributo_to_run_orm_attributo(
    new_attributo: JsonAttributoValue,
) -> orm.RunHasAttributoValue:
    return orm.RunHasAttributoValue(
        attributo_id=new_attributo.attributo_id,
        integer_value=new_attributo.attributo_value_int,
        float_value=new_attributo.attributo_value_float,
        string_value=new_attributo.attributo_value_str,
        bool_value=new_attributo.attributo_value_bool,
        datetime_value=(
            datetime_from_attributo_int(new_attributo.attributo_value_datetime)
            if new_attributo.attributo_value_datetime
            else None
        ),
        list_value=(
            new_attributo.attributo_value_list_str
            if new_attributo.attributo_value_list_str
            else (
                new_attributo.attributo_value_list_float
                if new_attributo.attributo_value_list_float
                else (
                    new_attributo.attributo_value_list_bool
                    if new_attributo.attributo_value_list_bool
                    else None
                )
            )
        ),
        chemical_value=new_attributo.attributo_value_chemical,
    )


def json_attributo_to_chemical_orm_attributo(
    new_attributo: JsonAttributoValue,
) -> orm.ChemicalHasAttributoValue:
    return orm.ChemicalHasAttributoValue(
        attributo_id=new_attributo.attributo_id,
        integer_value=new_attributo.attributo_value_int,
        float_value=new_attributo.attributo_value_float,
        string_value=new_attributo.attributo_value_str,
        bool_value=new_attributo.attributo_value_bool,
        datetime_value=(
            datetime_from_attributo_int(new_attributo.attributo_value_datetime)
            if new_attributo.attributo_value_datetime
            else None
        ),
        list_value=(
            new_attributo.attributo_value_list_str
            if new_attributo.attributo_value_list_str
            else (
                new_attributo.attributo_value_list_float
                if new_attributo.attributo_value_list_float
                else (
                    new_attributo.attributo_value_list_bool
                    if new_attributo.attributo_value_list_bool
                    else None
                )
            )
        ),
    )


async def update_attributi_from_json(
    session: AsyncSession,
    db_item: orm.Run | orm.Chemical | orm.DataSet,
    attributi_by_id: dict[int, orm.Attributo],
    new_attributi: list[JsonAttributoValue],
) -> None:
    attributo_ids_to_update = set(x.attributo_id for x in new_attributi)
    for existing_attributo in db_item.attributo_values:
        if existing_attributo.attributo_id in attributo_ids_to_update:
            await session.delete(existing_attributo)
    for new_attributo in new_attributi:
        validation_result = validate_json_attributo_return_error(
            new_attributo, attributi_by_id[new_attributo.attributo_id]
        )
        if validation_result is not None:
            raise HTTPException(
                status_code=400,
                detail=f"error validating attributi: {validation_result}",
            )
        if isinstance(db_item, orm.Run):
            db_item.attributo_values.append(
                json_attributo_to_run_orm_attributo(new_attributo)
            )
        elif isinstance(db_item, orm.Chemical):
            db_item.attributo_values.append(
                json_attributo_to_chemical_orm_attributo(new_attributo)
            )
        else:
            db_item.attributo_values.append(
                json_attributo_to_data_set_orm_attributo(new_attributo)
            )


def format_run_id_intervals(run_ids: Iterable[int]) -> list[str]:
    return [
        str(t[0]) if t[0] == t[1] else f"{t[0]}-{t[1]}"
        for t in create_intervals(list(run_ids))
    ]


async def safe_create_new_event(
    this_logger: structlog.stdlib.BoundLogger,
    session: AsyncSession,
    beamtime_id: BeamtimeId,
    text: str,
    level: EventLogLevel,
    source: str,
) -> None:
    try:
        session.add(
            orm.EventLog(
                beamtime_id=beamtime_id,
                level=level,
                source=source,
                text=text,
                created=datetime.datetime.now(datetime.timezone.utc),
            )
        )
    except:
        this_logger.exception("error writing event log")


def run_has_started_date(run: orm.Run, date_filter: str) -> bool:
    return run.started.strftime(DATE_FORMAT) == date_filter


def event_has_date(e: orm.EventLog, date_filter: str) -> bool:
    return e.created.strftime(DATE_FORMAT) == date_filter


def encode_data_set_attributo_value(
    d: orm.DataSetHasAttributoValue,
) -> JsonAttributoValue:
    return JsonAttributoValue(
        attributo_id=d.attributo_id,
        attributo_value_str=d.string_value,
        attributo_value_int=d.integer_value,
        attributo_value_chemical=d.chemical_value,
        attributo_value_datetime=(
            datetime_to_attributo_int(d.datetime_value)
            if d.datetime_value is not None
            else None
        ),
        attributo_value_float=d.float_value,
        attributo_value_bool=d.bool_value,
        attributo_value_list_str=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], str)  # type: ignore
            else None
        ),
        attributo_value_list_float=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], float)  # type: ignore
            else None
        ),
        attributo_value_list_bool=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], bool)  # type: ignore
            else None
        ),
    )


def encode_run_attributo_value(
    d: orm.RunHasAttributoValue,
) -> JsonAttributoValue:
    return JsonAttributoValue(
        attributo_id=d.attributo_id,
        attributo_value_str=d.string_value,
        attributo_value_int=d.integer_value,
        attributo_value_chemical=d.chemical_value,
        attributo_value_datetime=(
            datetime_to_attributo_int(d.datetime_value)
            if d.datetime_value is not None
            else None
        ),
        attributo_value_float=d.float_value,
        attributo_value_bool=d.bool_value,
        attributo_value_list_str=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], str)  # type: ignore
            else None
        ),
        attributo_value_list_float=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], float)  # type: ignore
            else None
        ),
        attributo_value_list_bool=(
            d.list_value
            if isinstance(d.list_value, list) and isinstance(d.list_value[0], bool)  # type: ignore
            else None
        ),
    )
