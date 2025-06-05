import datetime
import zlib
from dataclasses import dataclass
from enum import Enum
from pathlib import Path
from typing import Any
from typing import AsyncGenerator

import magic
from sqlalchemy.ext.asyncio import AsyncEngine
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import selectinload
from sqlalchemy.sql import select

from amarcord.cli.crystfel_index import CrystFELCellFile
from amarcord.cli.crystfel_index import parse_cell_description
from amarcord.db import orm
from amarcord.db.attributi import parse_schema_type
from amarcord.db.attributi import schema_dict_to_attributo_type
from amarcord.db.attributi import schema_union_to_attributo_type
from amarcord.db.attributi import utc_datetime_to_local_int
from amarcord.db.attributi import utc_datetime_to_utc_int
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.constants import CELL_DESCRIPTION_ATTRIBUTO
from amarcord.db.constants import POINT_GROUP_ATTRIBUTO
from amarcord.db.constants import SPACE_GROUP_ATTRIBUTO
from amarcord.db.migrations.alembic_utilities import upgrade_to_head_connection
from amarcord.util import sha256_file
from amarcord.web.json_models import JsonAttributoValue
from amarcord.web.json_models import JsonBeamtimeOutput
from amarcord.web.json_models import JsonGeometryMetadata

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
        geometry_id=None,
        command_line="--peaks=peakfinder8 --min-snr=5 --min-res=50 --threshold=4 --min-pix-count=2"
        + " --max-pix-count=50 --peakfinder8-fast --min-peaks=10 --local-bg-radius=3"
        + " --int-radius=4,5,7 --indexing=asdf --asdf-fast --no-retry",
        # source is empty for online indexing, since then it can be determined by the daemon
        source="",
    )


async def retrieve_latest_config(
    session: AsyncSession,
    beamtime_id: BeamtimeId,
) -> orm.UserConfiguration:
    result = (
        await session.scalars(
            select(orm.UserConfiguration)
            .where(orm.UserConfiguration.beamtime_id == beamtime_id)
            .order_by(orm.UserConfiguration.id.desc()),
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
    session: AsyncSession,
    beamtime_id: BeamtimeId,
) -> None | orm.Run:
    return (
        await session.scalars(
            select(orm.Run)
            .where(orm.Run.beamtime_id == beamtime_id)
            .order_by(orm.Run.started.desc()),
        )
    ).first()


class CompressionMode(Enum):
    COMPRESS_AUTO = "auto"
    COMPRESS_ON = "on"
    COMPRESS_OFF = "off"


def update_file_with_contents(
    f: orm.File, temp_file: Any, compress: CompressionMode
) -> None:
    file_path = Path(temp_file.name)
    f.sha256 = sha256_file(file_path)

    mime = magic.from_file(str(file_path), mime=True)  # type: ignore
    assert isinstance(mime, str), f"mime type is not a string: {mime}"
    f.type = mime

    file_size = file_path.stat().st_size
    f.size_in_bytes = file_size

    do_compress = (
        compress == CompressionMode.COMPRESS_AUTO and file_size >= 1000
    ) or CompressionMode.COMPRESS_ON

    if do_compress:
        new_contents = zlib.compress(temp_file.read())
        f.contents = new_contents
        f.size_in_bytes_compressed = len(new_contents)
    else:
        f.contents = temp_file.read()
    f.modified = datetime.datetime.now(datetime.timezone.utc)


def create_file_in_db(
    # can't type it as NamedTemporaryFile since that's a function returning a private object
    temp_file: Any,
    external_file_name: str,
    description: str,
    compression_mode: CompressionMode,
) -> orm.File:
    result = orm.File(
        type="placeholder",
        size_in_bytes=0,
        size_in_bytes_compressed=None,
        modified=datetime.datetime.now(datetime.timezone.utc),
        file_name=external_file_name,
        original_path=None,
        description=description,
        sha256="",
        contents=b"",
    )
    update_file_with_contents(result, temp_file, compression_mode)
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
        size_in_bytes_compressed=f.size_in_bytes_compressed,
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
        assert isinstance(v, float | int), f"expected a number, got {v}"
        chav.float_value = v
    elif isinstance(type_, AttributoTypeDateTime):
        assert isinstance(v, datetime.datetime), f"expected a datetime value, got {v}"
        chav.datetime_value = v
    elif isinstance(type_, AttributoTypeChemical) and not isinstance(
        chav,
        orm.ChemicalHasAttributoValue,
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
    a: JsonAttributoValue,
    atype_raw: orm.Attributo,
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
    async for this_channel_chemical in (
        (
            await session.scalars(
                select(orm.Chemical)
                .where(orm.Chemical.id == attributo_value.chemical_value)
                .options(
                    selectinload(orm.Chemical.attributo_values).selectinload(
                        orm.ChemicalHasAttributoValue.attributo,
                    ),
                ),
            )
        ).one()
        for attributo_value in r.attributo_values
        if attributo_value.chemical_value is not None
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
            + f' "crystal": {channel_chemical.name} (id {channel_chemical.id})',
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


def encode_beamtime(bt: orm.Beamtime, with_chemicals: bool) -> JsonBeamtimeOutput:
    return JsonBeamtimeOutput(
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
        chemical_names=(
            [chemical.name for chemical in bt.chemicals] if with_chemicals else []
        ),
        analysis_output_path=bt.analysis_output_path,
    )


def run_has_attributo_to_data_set_has_attributo(
    r: orm.RunHasAttributoValue,
) -> orm.DataSetHasAttributoValue:
    return orm.DataSetHasAttributoValue(
        attributo_id=r.attributo_id,
        integer_value=r.integer_value,
        float_value=r.float_value,
        string_value=r.string_value,
        bool_value=r.bool_value,
        datetime_value=r.datetime_value,
        list_value=r.list_value,
        chemical_value=r.chemical_value,
    )


async def determine_string_attributo_from_runs(
    session: AsyncSession, beamtime_id: int, run_ids: list[int], attributo_name: str
) -> set[str]:
    # get all chemicals in all runs related to the indexing results (attributo ID is not even important)
    chemical_ids_in_runs = select(orm.RunHasAttributoValue.chemical_value).where(
        (orm.RunHasAttributoValue.run_id.in_(run_ids))
        & (orm.RunHasAttributoValue.chemical_value.is_not(None)),
    )
    # attributi, plural, but there should be only one since names are hopefully unique
    string_chemical_attributi = (
        select(orm.Attributo.id)
        .where(
            (orm.Attributo.name == attributo_name)
            & (orm.Attributo.beamtime_id == beamtime_id),
        )
        .scalar_subquery()
    )
    select_all_strings = select(orm.ChemicalHasAttributoValue.string_value).where(
        (orm.ChemicalHasAttributoValue.attributo_id == string_chemical_attributi)
        & (orm.ChemicalHasAttributoValue.chemical_id.in_(chemical_ids_in_runs)),
    )
    return set(
        s.strip()
        for s in (await session.scalars(select_all_strings.distinct()))
        if s is not None and s.strip()
    )


async def determine_point_group_from_runs(
    session: AsyncSession,
    beamtime_id: int,
    run_ids: list[int],
) -> str:
    point_groups = await determine_string_attributo_from_runs(
        session, beamtime_id, run_ids, POINT_GROUP_ATTRIBUTO
    )

    if len(point_groups) > 1:
        raise ValueError(
            "Found more than one point group! The runs I chose have (internal) IDs "
            + ", ".join(str(run_id) for run_id in run_ids)
            + ", which results in the following point groups (determined by going through all chemicals in the runs): "
            + ", ".join(point_groups)
            + ". To correct this, you have to either specify a separate point group while merging, or (better choice, probably) take care of the point groups for your chemicals: you should have exactly one point group for all chemicals for all runs.",
        )
    if not point_groups:
        raise ValueError(
            "found no point groups at all! The runs I chose have (internal) IDs "
            + ", ".join(str(run_id) for run_id in run_ids)
            + ", which either have no chemicals attached, or the chemicals have no point group inside them.",
        )
    return next(iter(point_groups))


async def determine_space_group_from_runs(
    session: AsyncSession,
    beamtime_id: int,
    run_ids: list[int],
) -> str:
    space_groups = await determine_string_attributo_from_runs(
        session, beamtime_id, run_ids, SPACE_GROUP_ATTRIBUTO
    )

    if len(space_groups) > 1:
        raise ValueError(
            "Found more than one space group! The runs I chose have (internal) IDs "
            + ", ".join(str(run_id) for run_id in run_ids)
            + ", which results in the following space groups (determined by going through all chemicals in the runs): "
            + ", ".join(space_groups)
            + ". To correct this, you have to either specify a separate space group while merging, or (better choice, probably) take care of the space groups for your chemicals: you should have exactly one space group for all chemicals for all runs.",
        )
    if not space_groups:
        raise ValueError(
            "found no space groups at all! The runs I chose have (internal) IDs "
            + ", ".join(str(run_id) for run_id in run_ids)
            + ", which either have no chemicals attached, or the chemicals have no space group inside them.",
        )
    return next(iter(space_groups))


async def determine_cell_description_from_runs(
    session: AsyncSession,
    beamtime_id: int,
    run_ids: list[int],
) -> str:
    cell_descriptions = await determine_string_attributo_from_runs(
        session, beamtime_id, run_ids, CELL_DESCRIPTION_ATTRIBUTO
    )

    if len(cell_descriptions) > 1:
        raise ValueError(
            "Found more than one cell description! The runs I chose have (internal) IDs "
            + ", ".join(str(run_id) for run_id in run_ids)
            + ", which results in the following cell descriptions (determined by going through all chemicals in the runs): "
            + ", ".join(cell_descriptions)
            + ". To correct this, you have to take care of the point groups for your chemicals: you should have exactly one point group for all chemicals for all runs.",
        )
    if not cell_descriptions:
        raise ValueError(
            "found no cell_description at all! The runs I chose have (internal) IDs "
            + ", ".join(str(run_id) for run_id in run_ids)
            + ", which either have no chemicals attached, or the chemicals have no cell description inside them.",
        )
    return next(iter(cell_descriptions))


async def determine_point_group_from_indexing_results(
    session: AsyncSession,
    beamtime_id: int,
    indexing_results_matching_params: list[orm.IndexingResult],
) -> str:
    return await determine_point_group_from_runs(
        session, beamtime_id, [ir.run_id for ir in indexing_results_matching_params]
    )


def data_sets_are_equal(a: orm.DataSet, b: orm.DataSet) -> bool:
    if a.experiment_type_id != b.experiment_type_id:
        return False

    a_attributi: dict[int, orm.DataSetHasAttributoValue] = {
        av.attributo_id: av for av in a.attributo_values
    }
    b_attributi: dict[int, orm.DataSetHasAttributoValue] = {
        av.attributo_id: av for av in b.attributo_values
    }

    if a_attributi.keys() != b_attributi.keys():
        return False

    for aid, av in a_attributi.items():
        bv = b_attributi[aid]

        if not av.is_value_equal(bv):
            return False
    return True


async def all_geometry_metadatas(
    session: AsyncSession, beamtime_id: BeamtimeId
) -> list[JsonGeometryMetadata]:
    return [
        JsonGeometryMetadata(
            id=geom.id,
            name=geom.name,
            created_local=utc_datetime_to_local_int(geom.created),
        )
        for geom in await session.scalars(
            select(orm.Geometry).where(orm.Geometry.beamtime_id == beamtime_id)
        )
    ]


async def run_attributo_value_to_template_replacement(
    attributo: orm.Attributo, av: orm.RunHasAttributoValue
) -> str:
    attributo_type = schema_union_to_attributo_type(
        parse_schema_type(attributo.json_schema)
    )

    match attributo_type:
        case AttributoTypeInt():
            return str(av.integer_value) if av.integer_value is not None else ""
        case AttributoTypeBoolean():
            return "true" if av.bool_value is not None else "false"
        case AttributoTypeString() | AttributoTypeChoice():
            return av.string_value if av.string_value is not None else ""
        case AttributoTypeChemical():
            return (
                (await av.awaitable_attrs.chemical).name
                if av.chemical_value is not None
                else ""
            )
        case AttributoTypeDecimal():
            return str(av.float_value) if av.float_value is not None else ""
        case AttributoTypeDateTime():
            return str(av.datetime_value) if av.datetime_value is not None else ""
        case AttributoTypeList():
            return (
                ",".join(
                    str(v) for v in (av.list_value if av.list_value is not None else [])
                )
                if av.datetime_value is not None
                else ""
            )


async def generate_geometry_replacements(
    run: orm.Run, geometry: orm.Geometry
) -> AsyncGenerator[orm.GeometryTemplateReplacement, None]:
    for attributo in await geometry.awaitable_attrs.attributi:
        replacement_found = False
        for run_attributo_value in run.attributo_values:
            if run_attributo_value.attributo_id == attributo.id:
                yield orm.GeometryTemplateReplacement(
                    attributo_id=attributo.id,
                    replacement=await run_attributo_value_to_template_replacement(
                        attributo, run_attributo_value
                    ),
                )
                replacement_found = True
                break
        # This could happen if we have an Attributo in the geometry which is unset in the run.
        if not replacement_found:
            yield orm.GeometryTemplateReplacement(
                attributo_id=attributo.id,
                replacement="",
            )


async def add_geometry_template_replacements_to_ir(
    ir: orm.IndexingResult, run: orm.Run, geometry: orm.Geometry
) -> None:
    async for replacement in generate_geometry_replacements(run, geometry):
        ir.template_replacements.append(replacement)
