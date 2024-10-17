import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import magic
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.cli.crystfel_index import CrystFELCellFile
from amarcord.cli.crystfel_index import parse_cell_description
from amarcord.db import orm
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributi import schema_dict_to_attributo_type
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.constants import CELL_DESCRIPTION_ATTRIBUTO
from amarcord.db.constants import POINT_GROUP_ATTRIBUTO
from amarcord.db.migrations.alembic_utilities import upgrade_to_head_connection
from amarcord.util import sha256_file
from amarcord.web.constants import ELVEFLOW_OB1_MAX_NUMBER_OF_CHANNELS
from amarcord.web.json_models import JsonAttributoValue
from amarcord.web.json_models import JsonBeamtime

ATTRIBUTO_GROUP_MANUAL = "manual"


def live_stream_image_name(beamtime_id: int) -> str:
    return f"live-stream-image-{beamtime_id}"


def default_user_configuration(beamtime_id: int) -> orm.UserConfiguration:
    return orm.UserConfiguration(
        beamtime_id=BeamtimeId(beamtime_id),
        auto_pilot=False,
        use_online_crystfel=False,
        current_experiment_type_id=None,
        created=datetime.datetime.now(tz=datetime.timezone.utc),
        current_online_indexing_parameters_id=None,
    )


def default_online_indexing_parameters() -> orm.IndexingParameters:
    return orm.IndexingParameters(
        is_online=True,
        cell_description="",
        # empty means look for it in the current beam time
        geometry_file="",
        command_line="--peaks=peakfinder8 --min-snr=5 --min-res=50 --threshold=4 --min-pix-count=2"
        + " --max-pix-count=50 --peakfinder8-fast --min-peaks=10 --local-bg-radius=3"
        + " --int-radius=4,5,7 --indexing=asdf --asdf-fast --no-retry",
        # source is empty for online indexing, since then it can be determined by the daemon
        source="",
    )


async def retrieve_latest_config(
    session: AsyncSession, beamtime_id: BeamtimeId
) -> orm.UserConfiguration:
    result = (
        await session.scalars(
            select(orm.UserConfiguration)
            .where(orm.UserConfiguration.beamtime_id == beamtime_id)
            .order_by(orm.UserConfiguration.id.desc())
        )
    ).first()
    return result if result is not None else default_user_configuration(beamtime_id)


def duplicate_run_attributo(a: orm.RunHasAttributoValue) -> orm.RunHasAttributoValue:
    return orm.RunHasAttributoValue(
        attributo_id=a.attributo_id,
        integer_value=a.integer_value,
        float_value=a.float_value,
        string_value=a.string_value,
        bool_value=a.bool_value,
        datetime_value=a.datetime_value,
        list_value=a.list_value,
        chemical_value=a.chemical_value,
    )


async def retrieve_latest_run(
    session: AsyncSession, beamtime_id: BeamtimeId
) -> None | orm.Run:
    return (
        await session.scalars(
            select(orm.Run)
            .where(orm.Run.beamtime_id == beamtime_id)
            .order_by(orm.Run.started.desc())
        )
    ).first()


def update_file_with_contents(f: orm.File, temp_file: Any) -> None:
    file_path = Path(temp_file.name)
    f.sha256 = sha256_file(file_path)

    mime = magic.from_file(str(file_path), mime=True)  # type: ignore
    assert isinstance(mime, str), f"mime type is not a string: {mime}"
    f.type = mime
    f.size_in_bytes = file_path.stat().st_size
    f.contents = temp_file.read()  # type: ignore
    f.modified = datetime.datetime.now(datetime.timezone.utc)


def create_file_in_db(
    # can't type it as NamedTemporaryFile since that's a function returning a private object
    temp_file: Any,
    external_file_name: str,
    description: str,
) -> orm.File:
    result = orm.File(
        type="placeholder",
        size_in_bytes=0,
        modified=datetime.datetime.now(datetime.timezone.utc),
        file_name=external_file_name,
        original_path=None,
        description=description,
        sha256="",
        contents=b"",
    )
    update_file_with_contents(result, temp_file)
    return result


def create_new_user_configuration(
    user_configuration: orm.UserConfiguration,
) -> orm.UserConfiguration:
    return orm.UserConfiguration(
        beamtime_id=user_configuration.beamtime_id,
        created=datetime.datetime.now(datetime.timezone.utc),
        auto_pilot=user_configuration.auto_pilot,
        use_online_crystfel=user_configuration.use_online_crystfel,
        current_experiment_type_id=user_configuration.current_experiment_type_id,
        current_online_indexing_parameters_id=user_configuration.current_online_indexing_parameters_id,
    )


async def duplicate_file(f: orm.File, new_file_name: str) -> orm.File:
    return orm.File(
        type=f.type,
        file_name=new_file_name,
        size_in_bytes=f.size_in_bytes,
        original_path=f.original_path,
        sha256=f.sha256,
        modified=datetime.datetime.now(datetime.timezone.utc),
        contents=await f.awaitable_attrs.contents,
        description=f.description,
    )


def orm_entity_has_attributo_value_to_attributo_value(
    chav: (
        orm.ChemicalHasAttributoValue
        | orm.DataSetHasAttributoValue
        | orm.RunHasAttributoValue
    ),
) -> AttributoValue:
    return (
        chav.bool_value
        if chav.bool_value is not None
        else (
            chav.string_value
            if chav.string_value is not None
            else (
                chav.integer_value
                if chav.integer_value is not None
                else (
                    chav.float_value
                    if chav.float_value is not None
                    else (
                        chav.datetime_value
                        if chav.datetime_value is not None
                        else (
                            chav.list_value
                            if chav.list_value is not None
                            else (
                                chav.chemical_value
                                if not isinstance(chav, orm.ChemicalHasAttributoValue)
                                and chav.chemical_value is not None
                                else None
                            )
                        )
                    )
                )
            )
        )
    )


def update_orm_entity_has_attributo_value(
    chav: (
        orm.ChemicalHasAttributoValue
        | orm.RunHasAttributoValue
        | orm.DataSetHasAttributoValue
    ),
    type_: AttributoType,
    v: AttributoValue,
) -> None:
    # First, set all value types to None...
    chav.integer_value = None
    chav.string_value = None
    chav.bool_value = None
    chav.float_value = None
    chav.datetime_value = None
    chav.list_value = None
    if not isinstance(chav, orm.ChemicalHasAttributoValue):
        chav.chemical_value = None
    # ...then re-add the converted one
    if isinstance(type_, AttributoTypeInt):
        assert isinstance(v, int), f"expected an integer, got {v}"
        chav.integer_value = v
    elif isinstance(type_, AttributoTypeBoolean):
        assert isinstance(v, bool), f"expected a boolean, got {v}"
        chav.bool_value = v
    elif isinstance(type_, AttributoTypeString):
        assert isinstance(v, str), f"expected a string, got {v}"
        chav.string_value = v
    elif isinstance(type_, AttributoTypeChoice):
        assert isinstance(v, str), f"expected a string (choice), got {v}"
        chav.string_value = v
    elif isinstance(type_, AttributoTypeDecimal):
        assert isinstance(v, (int, float)), f"expected a number, got {v}"
        chav.float_value = v
    elif isinstance(type_, AttributoTypeDateTime):
        assert isinstance(v, datetime.datetime), f"expected a datetime value, got {v}"
        chav.datetime_value = v
    elif isinstance(type_, AttributoTypeChemical) and not isinstance(
        chav, orm.ChemicalHasAttributoValue
    ):
        assert isinstance(v, int), f"expected an int value (chemical), got {v}"
        chav.chemical_value = v
    else:
        assert isinstance(v, list), f"expected a list, got {v}"
        chav.list_value = v


async def migrate(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(upgrade_to_head_connection)


def validate_json_attributo_return_error(
    a: JsonAttributoValue, atype_raw: orm.Attributo
) -> None | str:
    atype = schema_dict_to_attributo_type(atype_raw.json_schema)

    if isinstance(atype, AttributoTypeInt):
        if a.attributo_value_str is not None:
            return f"attributo {a.attributo_id}: has string value {a.attributo_value_str}, but is type int"
        if a.attributo_value_bool is not None:
            return f"attributo {a.attributo_id}: has boolean value {a.attributo_value_bool}, but is type int"
        if a.attributo_value_float is not None:
            return f"attributo {a.attributo_id}: has float value {a.attributo_value_float}, but is type int"
        if a.attributo_value_chemical is not None:
            return f"attributo {a.attributo_id}: has chemical value {a.attributo_value_chemical}, but is type int"
        if a.attributo_value_datetime is not None:
            return f"attributo {a.attributo_id}: has datetime value {a.attributo_value_datetime}, but is type int"
        if a.attributo_value_list_str is not None:
            return f"attributo {a.attributo_id}: has list value, but is type int"
        if a.attributo_value_list_bool is not None:
            return f"attributo {a.attributo_id}: has list value, but is type int"
        if a.attributo_value_list_float is not None:
            return f"attributo {a.attributo_id}: has list value, but is type int"
        return None

    if isinstance(atype, AttributoTypeChemical):
        if a.attributo_value_int is not None:
            return f"attributo {a.attributo_id}: has int value {a.attributo_value_str}, but is type chemical"
        if a.attributo_value_str is not None:
            return f"attributo {a.attributo_id}: has string value {a.attributo_value_str}, but is type chemical"
        if a.attributo_value_bool is not None:
            return f"attributo {a.attributo_id}: has boolean value {a.attributo_value_bool}, but is type chemical"
        if a.attributo_value_float is not None:
            return f"attributo {a.attributo_id}: has float value {a.attributo_value_float}, but is type chemical"
        if a.attributo_value_datetime is not None:
            return f"attributo {a.attributo_id}: has datetime value {a.attributo_value_datetime}, but is type chemical"
        if a.attributo_value_list_str is not None:
            return f"attributo {a.attributo_id}: has list value, but is type chemical"
        if a.attributo_value_list_bool is not None:
            return f"attributo {a.attributo_id}: has list value, but is type chemical"
        if a.attributo_value_list_float is not None:
            return f"attributo {a.attributo_id}: has list value, but is type chemical"
        return None

    if isinstance(atype, AttributoTypeString):
        if a.attributo_value_int is not None:
            return f"attributo {a.attributo_id}: has string value {a.attributo_value_int}, but is type string"
        if a.attributo_value_bool is not None:
            return f"attributo {a.attributo_id}: has boolean value {a.attributo_value_bool}, but is type string"
        if a.attributo_value_float is not None:
            return f"attributo {a.attributo_id}: has float value {a.attributo_value_float}, but is type string"
        if a.attributo_value_chemical is not None:
            return f"attributo {a.attributo_id}: has chemical value {a.attributo_value_chemical}, but is type string"
        if a.attributo_value_datetime is not None:
            return f"attributo {a.attributo_id}: has datetime value {a.attributo_value_datetime}, but is type string"
        if a.attributo_value_list_str is not None:
            return f"attributo {a.attributo_id}: has list value, but is type string"
        if a.attributo_value_list_bool is not None:
            return f"attributo {a.attributo_id}: has list value, but is type string"
        if a.attributo_value_list_float is not None:
            return f"attributo {a.attributo_id}: has list value, but is type string"
        return None

    if isinstance(atype, AttributoTypeBoolean):
        if a.attributo_value_int is not None:
            return f"attributo {a.attributo_id}: has string value {a.attributo_value_int}, but is type bool"
        if a.attributo_value_str is not None:
            return f"attributo {a.attributo_id}: has string value {a.attributo_value_int}, but is type bool"
        if a.attributo_value_float is not None:
            return f"attributo {a.attributo_id}: has float value {a.attributo_value_float}, but is type bool"
        if a.attributo_value_chemical is not None:
            return f"attributo {a.attributo_id}: has chemical value {a.attributo_value_chemical}, but is type bool"
        if a.attributo_value_datetime is not None:
            return f"attributo {a.attributo_id}: has datetime value {a.attributo_value_datetime}, but is type bool"
        if a.attributo_value_list_str is not None:
            return f"attributo {a.attributo_id}: has list value, but is type bool"
        if a.attributo_value_list_bool is not None:
            return f"attributo {a.attributo_id}: has list value, but is type bool"
        if a.attributo_value_list_float is not None:
            return f"attributo {a.attributo_id}: has list value, but is type bool"
        return None

    if isinstance(atype, AttributoTypeDateTime):
        if a.attributo_value_int is not None:
            return f"attributo {a.attributo_id}: has string value {a.attributo_value_int}, but is type datetime"
        if a.attributo_value_str is not None:
            return f"attributo {a.attributo_id}: has string value {a.attributo_value_int}, but is type datetime"
        if a.attributo_value_float is not None:
            return f"attributo {a.attributo_id}: has float value {a.attributo_value_float}, but is type datetime"
        if a.attributo_value_chemical is not None:
            return f"attributo {a.attributo_id}: has chemical value {a.attributo_value_chemical}, but is type datetime"
        if a.attributo_value_list_str is not None:
            return f"attributo {a.attributo_id}: has list value, but is type datetime"
        if a.attributo_value_list_bool is not None:
            return f"attributo {a.attributo_id}: has list value, but is type datetime"
        if a.attributo_value_list_float is not None:
            return f"attributo {a.attributo_id}: has list value, but is type datetime"
        return None

    if isinstance(atype, AttributoTypeChoice):
        if a.attributo_value_int is not None:
            return f"attributo {a.attributo_id}: has string value {a.attributo_value_int}, but is type string"
        if a.attributo_value_bool is not None:
            return f"attributo {a.attributo_id}: has boolean value {a.attributo_value_bool}, but is type string"
        if a.attributo_value_float is not None:
            return f"attributo {a.attributo_id}: has float value {a.attributo_value_float}, but is type string"
        if a.attributo_value_chemical is not None:
            return f"attributo {a.attributo_id}: has chemical value {a.attributo_value_chemical}, but is type string"
        if a.attributo_value_datetime is not None:
            return f"attributo {a.attributo_id}: has datetime value {a.attributo_value_datetime}, but is type string"
        if a.attributo_value_list_str is not None:
            return f"attributo {a.attributo_id}: has list value, but is type string"
        if a.attributo_value_list_bool is not None:
            return f"attributo {a.attributo_id}: has list value, but is type string"
        if a.attributo_value_list_float is not None:
            return f"attributo {a.attributo_id}: has list value, but is type string"
        if a.attributo_value_str is None:
            return None
        return (
            None
            if a.attributo_value_str in atype.values
            else f'attributo {a.attributo_id}: has string value "{a.attributo_value_str}" which is not in the available choices: '
            + ", ".join(atype.values)
        )
    if isinstance(atype, AttributoTypeDecimal):
        if a.attributo_value_str is not None:
            return f"attributo {a.attributo_id}: has string value {a.attributo_value_str}, but is type decimal"
        if a.attributo_value_bool is not None:
            return f"attributo {a.attributo_id}: has boolean value {a.attributo_value_bool}, but is type decimal"
        if a.attributo_value_chemical is not None:
            return f"attributo {a.attributo_id}: has chemical value {a.attributo_value_chemical}, but is type decimal"
        if a.attributo_value_datetime is not None:
            return f"attributo {a.attributo_id}: has datetime value {a.attributo_value_datetime}, but is type decimal"
        if a.attributo_value_list_str is not None:
            return f"attributo {a.attributo_id}: has list value, but is type decimal"
        if a.attributo_value_list_bool is not None:
            return f"attributo {a.attributo_id}: has list value, but is type decimal"
        if a.attributo_value_list_float is not None:
            return f"attributo {a.attributo_id}: has list value, but is type decimal"
        v = a.attributo_value_float
        if v is None:
            return None
        if atype.range is not None and not atype.range.value_is_inside(v):
            return (
                f"attributo {a.attributo_id}: out of range; range is {atype.range}, "
                + f"value is {v}"
            )
    return None


@dataclass(frozen=True)
class RunIndexingMetadata:
    point_group: None | str
    cell_description: None | CrystFELCellFile
    chemical: orm.Chemical
    log_messages: list[str]


async def determine_run_indexing_metadata(
    session: AsyncSession,
    r: orm.Run,
) -> str | RunIndexingMetadata:
    point_group: None | str = None
    cell_description_str: None | str = None
    channel_chemical: None | orm.Chemical = None
    # For indexing, we need to provide one chemical ID that serves as _the_ chemical ID for the indexing job
    # (kind of a bug right now). So, if we don't find any chemicals with cell information, we just use the first
    # one which is of type "crystal". Since it's totally valid to leave out cell information for crystals, for
    # example in the case where you actually don't know that and want to find out.
    crystal_chemicals: list[orm.Chemical] = []
    # "protein" is for old beamtimes and acts as a fallback for now. Not a good solution, we know.
    crystal_attributo_names = [
        f"channel_{channel}_chemical_id"
        for channel in range(1, ELVEFLOW_OB1_MAX_NUMBER_OF_CHANNELS + 1)
    ] + ["protein"]
    async for this_channel_chemical in (
        (
            await session.scalars(
                select(orm.Chemical)
                .where(orm.Chemical.id == attributo_value.chemical_value)
                .options(
                    selectinload(orm.Chemical.attributo_values).selectinload(
                        orm.ChemicalHasAttributoValue.attributo
                    )
                )
            )
        ).one()
        for attributo_value in r.attributo_values
        if attributo_value.chemical_value is not None
        and attributo_value.attributo.name in crystal_attributo_names
    ):
        if this_channel_chemical.type == ChemicalType.CRYSTAL:
            crystal_chemicals.append(this_channel_chemical)
        this_point_group = next(
            iter(
                attributo_value.string_value
                for attributo_value in this_channel_chemical.attributo_values
                if attributo_value.attributo.name == POINT_GROUP_ATTRIBUTO
            ),
            None,
        )
        this_cell_description = next(
            iter(
                attributo_value.string_value
                for attributo_value in this_channel_chemical.attributo_values
                if attributo_value.attributo.name == CELL_DESCRIPTION_ATTRIBUTO
            ),
            None,
        )
        if this_point_group is not None and this_cell_description is not None:
            point_group = this_point_group
            cell_description_str = this_cell_description
            channel_chemical = this_channel_chemical
            break

    log_messages: list[str] = []
    if channel_chemical is None:
        if not crystal_chemicals:
            return 'no chemicals with cell information and none of type "crystal" in run detected'
        channel_chemical = crystal_chemicals[0]
        log_messages.append(
            "no chemicals with cell information found, taking the first chemical of type "
            + f' "crystal": {channel_chemical.name} (id {channel_chemical.id})'
        )

    cell_description: None | CrystFELCellFile
    if cell_description_str is not None and cell_description_str:
        cell_description = parse_cell_description(cell_description_str)
        if cell_description is None:
            return f"cell description is invalid: {cell_description_str}"
    else:
        cell_description = None

    return RunIndexingMetadata(
        point_group=point_group if point_group else None,
        cell_description=cell_description,
        chemical=channel_chemical,
        log_messages=log_messages,
    )


def encode_beamtime(bt: orm.Beamtime) -> JsonBeamtime:
    return JsonBeamtime(
        id=bt.id,
        external_id=bt.external_id,
        proposal=bt.proposal,
        beamline=bt.beamline,
        title=bt.title,
        comment=bt.comment,
        start=datetime_to_attributo_int(bt.start),
        end=datetime_to_attributo_int(bt.end),
        chemical_names=[chemical.name for chemical in bt.chemicals],
    )
