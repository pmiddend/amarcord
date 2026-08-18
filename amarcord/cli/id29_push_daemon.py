import asyncio
import datetime
import json
import re
import time
from dataclasses import dataclass
from dataclasses import replace
from enum import Enum
from pathlib import Path

import aiohttp
import structlog
from typed_argparse import Parser
from typed_argparse import TypedArgs
from typed_argparse import arg

from amarcord.cli.crystfel_index import convert_to_cell_description
from amarcord.db.attributi import utc_datetime_to_utc_int
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.json_schema import JSONSchemaBoolean
from amarcord.json_schema import JSONSchemaInteger
from amarcord.json_schema import JSONSchemaNumber
from amarcord.json_schema import JSONSchemaString
from amarcord.logging_util import setup_structlog
from amarcord.web.json_models import JsonAttributo
from amarcord.web.json_models import JsonAttributoValue
from amarcord.web.json_models import JsonChemical
from amarcord.web.json_models import JsonCreateAttributiFromSchemaInput
from amarcord.web.json_models import JsonCreateAttributiFromSchemaSingleAttributo
from amarcord.web.json_models import JsonCreateOrUpdateRun
from amarcord.web.json_models import JsonEventInput
from amarcord.web.json_models import JsonEventTopLevelInput
from amarcord.web.json_models import JsonImportFinishedIndexingJobInput
from amarcord.web.json_models import JsonImportFinishedIndexingJobOutput
from amarcord.web.json_models import JsonReadAttributi
from amarcord.web.json_models import JsonReadChemicals
from amarcord.web.json_models import JsonReadRuns
from amarcord.web.json_models import JsonRun

setup_structlog()

logger = structlog.stdlib.get_logger(__name__)

ID29_MAGIC_DIRECTORY_ATTRIBUTO = "MX_directory"
ID29_MAGIC_STREAM_FILE_NAME = "alltogether.stream"
_STREAM_FILE_MIN_AGE_S = 5 * 60


class ID29AttributoType(Enum):
    ATTRIBUTO_TYPE_NUMBER = "number"
    ATTRIBUTO_TYPE_INTEGER = "integer"
    ATTRIBUTO_TYPE_STRING = "string"
    ATTRIBUTO_TYPE_BOOLEAN = "boolean"


@dataclass
class ID29AttributoConfig:
    beamline_attributo_name: str
    attributo_id: int
    attributo_name: str
    attributo_type: ID29AttributoType
    attributo_unit: str | None


@dataclass
class ID29AttributoConfigFile:
    attributi: list[ID29AttributoConfig]


@dataclass
class ID29ErrorCache:
    unknown_chemicals: set[str]


def id29_parse_attributo_config_file(p: Path) -> ID29AttributoConfigFile:
    with p.open("r", encoding="utf-8") as f:
        json_content = json.load(f)

        assert isinstance(json_content, list)

        attributi: list[ID29AttributoConfig] = []
        for item in json_content:
            attributi.append(
                ID29AttributoConfig(
                    beamline_attributo_name=item["beamline-name"],
                    attributo_id=0,
                    attributo_name=item["attributo-name"],
                    attributo_type=ID29AttributoType(item["attributo-type"]),
                    attributo_unit=item.get("attributo-unit"),
                )
            )
        return ID29AttributoConfigFile(attributi)


class Arguments(TypedArgs):
    amarcord_beamtime_id: int = arg(help="Which beamtime to push the runs to")
    raw_data_path: Path = arg(help="here to start looking for metadata.json files")
    run_id_file: Path = arg(
        help="here to store the last chosen run ID (to generate a new one)"
    )
    metadata_visited_file: Path = arg(
        help="here to store which metadata.json was already processed"
    )
    stream_visited_file: Path = arg(
        help="Where to store which stream file was already processed"
    )
    attributo_config_file: Path = arg(help="Where to store which attributi to send")
    create_attributi: bool = arg(
        help="hether to use the config file to actually create all attributi"
    )
    amarcord_url: str = arg(help="URL to the AMARCORD API (without /api suffix)")
    amarcord_user: str = arg(help="HTTP user name to access the AMARCORD API")
    amarcord_password: str = arg(help="HTTP password to access the AMARCORD API")
    sample_attributo: str = arg(help="Name of the sample attributo to use")
    tag_attributo: str = arg(help="Name of the tag attributo to use")
    compare_stream_file_age: bool = arg(
        help="Do not ingest new stream files immediately, but test for their age and give some grace time",
        default=True,
    )
    simulate: bool = arg(
        "Do not modify the file system or database, only simulate what would be done",
        default=False,
    )
    raw_data_prefix: str | None = arg(
        "Prefix to replace for the raw data paths", default=None
    )
    raw_data_prefix_replacement: str | None = arg(
        "Replacement for the prefix in --raw-data-prefix", default=None
    )


def _retrieve_next_run_id(args: Arguments) -> int:
    try:
        with args.run_id_file.open("r", encoding="utf-8") as f:
            run_id = int(f.read().strip())
        new_run_id = run_id + 1
        with args.run_id_file.open("w+", encoding="utf-8") as f:
            f.write(f"{new_run_id}")
        return new_run_id
    except:
        new_run_id = 1
        with args.run_id_file.open("w+", encoding="utf-8") as f:
            f.write(f"{new_run_id}")
        return new_run_id


def _mark_metadata_as_completed(args: Arguments, metadata_file: Path) -> None:
    if args.simulate:
        logger.info(f"simulation: would mark {metadata_file} as completed")
        return
    try:
        logger.info(f"marking {metadata_file} as completed")
        with args.metadata_visited_file.open("a+") as f:
            f.write(f"{metadata_file}\n")
    except Exception as e:
        logger.error(f"{metadata_file}: cannot mark as completed: {e}")


def _mark_stream_as_completed(args: Arguments, stream_file: Path) -> None:
    logger.info(f"marking stream {stream_file} as completed")
    with args.stream_visited_file.open("a+") as f:
        f.write(f"{stream_file}\n")


def _retrieve_new_metadata_files(args: Arguments) -> list[Path]:
    try:
        with args.metadata_visited_file.open("r", encoding="utf-8") as f:
            metadata_files = set(line.strip() for line in f.read().split("\n"))
    except:
        metadata_files = set()

    result: list[Path] = []
    for metadata_json in args.raw_data_path.rglob("metadata.json"):
        absolute_path_str = str(metadata_json.absolute())

        if absolute_path_str not in metadata_files:
            result.append(metadata_json)
    return result


def _retrieve_new_stream_files(args: Arguments) -> list[Path]:
    try:
        with args.stream_visited_file.open("r", encoding="utf-8") as f:
            stream_files = set(line.strip() for line in f.read().split("\n"))
    except:
        stream_files = set()

    result: list[Path] = []
    now = time.time()
    for stream_file in args.raw_data_path.rglob(ID29_MAGIC_STREAM_FILE_NAME):
        age = now - stream_file.stat().st_mtime
        if args.compare_stream_file_age and age < _STREAM_FILE_MIN_AGE_S:
            logger.info(f"file {stream_file} too young (age {age / 60}min)")
            continue
        absolute_path_str = str(stream_file.absolute())

        if absolute_path_str not in stream_files:
            result.append(stream_file)
    return result


def _oldest_and_newest_file_in_dir(
    dir_to_search: Path,
) -> tuple[datetime.datetime, datetime.datetime] | None:
    mintime: datetime.datetime | None = None
    maxtime: datetime.datetime | None = None
    try:
        for p in dir_to_search.iterdir():
            if not p.is_file():
                continue
            t = datetime.datetime.fromtimestamp(p.stat().st_mtime, tz=datetime.UTC)
            if mintime is None or t < mintime:
                mintime = t
            if maxtime is None or t > maxtime:
                maxtime = t
        if mintime is not None and maxtime is not None:
            return mintime, maxtime
    except Exception as e:
        logger.error(f"couldn't read some file in directory: {e}")
    return None


async def _ingest_new_metadata_file(
    args: Arguments,
    session: aiohttp.ClientSession,
    sample_attribute_id: int,
    tag_attribute_id: int,
    directory_attribute_id: int,
    chemicals: list[JsonChemical],
    attributo_config_file: ID29AttributoConfigFile,
    metadata_file: Path,
    error_cache: ID29ErrorCache,
) -> None:
    try:
        with metadata_file.open("r", encoding="utf-8") as f:
            file_dict = json.load(f)

        assert isinstance(file_dict, dict)
    except Exception as e:
        logger.error(
            f"{metadata_file}: error reading previous metadata files from AMARCORD: {e}"
        )
        return

    try:
        chemical_name = file_dict["Sample_name"]
        assert isinstance(chemical_name, str)
        chemical_in_amarcord = next(
            iter(x for x in chemicals if x.name == chemical_name), None
        )
        if chemical_in_amarcord is None:
            if chemical_name in error_cache.unknown_chemicals:
                return
            await _try_send_event(
                args,
                session,
                "warning",
                f"couldn't find chemical **{chemical_name}** in AMARCORD, I will try again soon",
            )
            error_cache.unknown_chemicals.add(chemical_name)
            return
        if chemical_name in error_cache.unknown_chemicals:
            await _try_send_event(
                args,
                session,
                "info",
                f"chemical **{chemical_name}** found in AMARCORD now, processing...",
            )
            error_cache.unknown_chemicals.remove(chemical_name)
        files_path = file_dict[ID29_MAGIC_DIRECTORY_ATTRIBUTO]
        assert isinstance(files_path, str)
        try:
            start_date_str = file_dict["startDate"]
            assert isinstance(start_date_str, str)
            started = datetime.datetime.fromisoformat(start_date_str)
            end_date_str = file_dict["endDate"]
            assert isinstance(end_date_str, str)
            stopped = datetime.datetime.fromisoformat(end_date_str)
        except:
            logger.warning(f"{metadata_file}: no start date given, synthesizing one")
            min_and_max_mtime = _oldest_and_newest_file_in_dir(Path(files_path))
            if min_and_max_mtime is not None:
                started, stopped = min_and_max_mtime
            else:
                started = datetime.datetime.now(datetime.UTC) - datetime.timedelta(
                    seconds=60
                )
                stopped = datetime.datetime.now(datetime.UTC)

    except Exception as e:
        await _try_send_event(
            args,
            session,
            "error",
            f"`{metadata_file}`: error parsing metadata file: {e}, will not try again",
        )
        _mark_metadata_as_completed(args, metadata_file)
        return

    attributi: list[JsonAttributoValue] = [
        JsonAttributoValue(
            attributo_id=sample_attribute_id,
            attributo_value_chemical=chemical_in_amarcord.id,
        ),
        JsonAttributoValue(
            attributo_id=tag_attribute_id,
            attributo_value_str=Path(files_path).parent.name,
        ),
    ]
    for a in attributo_config_file.attributi:
        value = file_dict.get(a.beamline_attributo_name)
        if value is None:
            continue

        str_value: str | None
        if not isinstance(value, str):
            str_value = None
        elif a.attributo_id != directory_attribute_id:
            str_value = value
        elif (
            args.raw_data_prefix is not None
            and args.raw_data_prefix_replacement is not None
        ):
            str_value = value.replace(
                args.raw_data_prefix, args.raw_data_prefix_replacement
            )
        else:
            str_value = value
        attributi.append(
            JsonAttributoValue(
                attributo_id=a.attributo_id,
                attributo_value_str=str_value,
                attributo_value_int=(
                    value
                    if isinstance(value, int)
                    and a.attributo_type == ID29AttributoType.ATTRIBUTO_TYPE_INTEGER
                    else None
                ),
                attributo_value_float=(
                    value
                    if isinstance(value, float | int)
                    and a.attributo_type == ID29AttributoType.ATTRIBUTO_TYPE_NUMBER
                    else None
                ),
                attributo_value_bool=(value if isinstance(value, bool) else None),
                attributo_value_list_str=None,
                attributo_value_list_float=None,
                attributo_value_list_bool=None,
            )
        )

    request_content = JsonCreateOrUpdateRun(
        beamtime_id=BeamtimeId(args.amarcord_beamtime_id),
        attributi=attributi,
        files=None,
        create_data_set=True,
        started=utc_datetime_to_utc_int(started),
        stopped=utc_datetime_to_utc_int(stopped),
    )
    run_id = _retrieve_next_run_id(args) if not args.simulate else 1

    if args.simulate:
        logger.info(f"simulation: would send run {run_id}: {request_content}")
    else:
        try:
            async with session.post(
                f"{args.amarcord_url}/api/runs/{run_id}",
                json=request_content.model_dump(),
            ):
                logger.info("run creation request successful")
        except Exception as e:
            logger.error(
                f"`{metadata_file}`: error sending run creation request: {e}, will retry soon"
            )
            return

    await _try_send_event(
        args,
        session,
        "info",
        f"new run: {run_id}, directory `{files_path}`",
    )
    _mark_metadata_as_completed(args, metadata_file)


@dataclass(frozen=True)
class _StreamFileData:
    command_line: str
    cell: str | None
    geometry: str
    program_version: str
    frames: int
    hits: int
    indexed: int
    first_image_filename: Path | None


def _parse_crystfel_cli(s: str) -> str:
    parts = s.split(" ")
    replacements = [
        (r"-i [^ ]*", ""),
        (r"--input=[^ ]*", ""),
        (r"-o [^ ]*", ""),
        (r"--output=[^ ]*", ""),
        (r"-g [^ ]*", ""),
        (r"--geometry=[^ ]*", ""),
        (r"--mille-dir=[^ ]*", ""),
        (r"--serial-start=[^ ]*", ""),
        (r"--temp-dir=[^ ]*", ""),
        (r"-p [^ ]*", ""),
        (r"--pdb=[^ ]*", ""),
        (r"-j [^ ]*", ""),
        (r"--no-non-hits-in-stream", ""),
        (r"--wait-for-file=[^ ]*", ""),
        (
            r"--xgandalf-fast-execution",
            "--xgandalf-sampling-pitch=2 --xgandalf-grad-desc-iterations=3",
        ),
    ]
    result = " ".join(parts[1:])
    for needle, replacement in replacements:
        result = re.sub(needle, replacement, result)
    return result


def _read_stream_file_data(p: Path) -> _StreamFileData:
    with p.open("r", encoding="utf-8") as f:
        crystfel_version: str | None = None
        indexamajig_cli: str | None = None
        in_geometry = False
        in_cell = False
        geometry: str | None = None
        cell: str | None = None
        frames = 0
        hits = 0
        indexed = 0
        first_image_filename: Path | None = None
        for line_with_nl in f:
            line = line_with_nl.strip()

            if line.startswith("Generated by CrystFEL "):
                crystfel_version = line[22:]
            elif "indexamajig" in line:
                indexamajig_cli = _parse_crystfel_cli(line)
            elif line.startswith("----- Begin geometry") and geometry is None:
                in_geometry = True
                geometry = ""
            elif in_geometry and line.startswith("----- End geometry"):
                in_geometry = False
            elif in_geometry and geometry is not None:
                geometry += line + "\n"
            elif line.startswith("----- Begin unit") and cell is None:
                in_cell = True
                cell = ""
            elif in_cell and line.startswith("----- End unit"):
                in_cell = False
            elif in_cell and cell is not None:
                cell += line + "\n"
            elif line.startswith("----- Begin chunk"):
                frames += 1
            elif line.startswith("hit = 1"):
                hits += 1
            elif line.startswith("indexed_by = ") and line != "indexed_by = none":
                indexed += 1
            elif line.startswith("Image filename: ") and first_image_filename is None:
                first_image_filename = Path(line[16:])

        if indexamajig_cli is None:
            raise Exception(f"{p}: no indexamajig line found")
        if crystfel_version is None:
            raise Exception(f"{p}: no crystfel version line found")
        if geometry is None or not geometry.strip():
            raise Exception(f"{p}: no geometry section found")
        return _StreamFileData(
            command_line=indexamajig_cli,
            cell=cell,
            geometry=geometry,
            program_version=crystfel_version,
            frames=frames,
            hits=hits,
            indexed=indexed,
            first_image_filename=first_image_filename,
        )


async def _try_send_event(
    args: Arguments, session: aiohttp.ClientSession, level: str, error_message: str
) -> None:
    if args.simulate:
        logger.info(f"simulation: would send level {level}, message {error_message}")
        return
    if level == "warning":
        logger.warning(error_message)
    elif level == "error":
        logger.error(error_message)
    elif level == "info":
        logger.info(error_message)
    try:
        async with session.post(
            f"{args.amarcord_url}/api/events",
            json=JsonEventTopLevelInput(
                beamtime_id=BeamtimeId(args.amarcord_beamtime_id),
                event=JsonEventInput(
                    source="push", text=error_message, level=level, file_ids=[]
                ),
                with_live_stream=False,
            ).model_dump(),
        ):
            logger.info("sent event")
    except Exception as e:
        logger.error(f"couldn't send event: {e}")


async def _ingest_new_stream_file(
    args: Arguments, session: aiohttp.ClientSession, stream_file: Path
) -> None:
    try:
        logger.info(f"reading stream file {stream_file} (this will take a while)")
        stream_file_data = _read_stream_file_data(stream_file)
    except Exception as e:
        # The read function raises either because of an I/O error or
        # because the stream file is not complete (no geometry, ...)
        # Either of them could be recoverable, but for now, let's mark it as done.
        await _try_send_event(
            args,
            session,
            "warning",
            f"error parsing stream file `{stream_file}`: {e}, marking as done",
        )
        _mark_stream_as_completed(args, stream_file)
        return

    if (
        args.raw_data_prefix is not None
        and args.raw_data_prefix_replacement is not None
        and stream_file_data.first_image_filename is not None
    ):
        stream_file_data = replace(
            stream_file_data,
            first_image_filename=Path(
                str(stream_file_data.first_image_filename).replace(
                    args.raw_data_prefix, args.raw_data_prefix_replacement
                )
            ),
        )

    if stream_file_data.first_image_filename is None:
        # The stream file might complete later, but we set it up so this doesn't happen. Mark as completed.
        await _try_send_event(
            args,
            session,
            "warning",
            f"{stream_file}: has no images, cannot determine the run it belongs to, marking as done",
        )
        _mark_stream_as_completed(args, stream_file)
        return

    try:
        cell_description = (
            convert_to_cell_description(stream_file_data.cell)
            if stream_file_data.cell is not None
            else None
        )
    except Exception as e:
        await _try_send_event(
            args,
            session,
            "error",
            f"error parsing cell description from `{stream_file}`: {e} marking as done",
        )
        _mark_stream_as_completed(args, stream_file)
        return

    try:
        async with session.get(
            f"{args.amarcord_url}/api/runs/{args.amarcord_beamtime_id}",
        ) as resp:
            runs = JsonReadRuns(**await resp.json())
    except Exception as e:
        await _try_send_event(
            args,
            session,
            "warning",
            f"retrieving runs for beamtime {args.amarcord_beamtime_id} failed, skipping for now: {e}",
        )
        return

    directory_attributo = next(
        iter(a for a in runs.attributi if a.name == ID29_MAGIC_DIRECTORY_ATTRIBUTO),
        None,
    )
    if directory_attributo is None:
        await _try_send_event(
            args,
            session,
            "error",
            f"have no attributo named `{ID29_MAGIC_DIRECTORY_ATTRIBUTO}` in beamtime, cannot ingest stream files",
        )
        return
    found_run: JsonRun | None = None
    for run in runs.runs:
        run_directory_attributo = next(
            iter(
                ra for ra in run.attributi if ra.attributo_id == directory_attributo.id
            ),
            None,
        )
        if (
            run_directory_attributo is not None
            and run_directory_attributo.attributo_value_str is not None
            and Path(run_directory_attributo.attributo_value_str)
            == stream_file_data.first_image_filename.parent
        ):
            found_run = run
            break
    if found_run is None:
        await _try_send_event(
            args,
            session,
            "warning",
            f"no run found for stream file with images like `{stream_file_data.first_image_filename}`",
        )
        return

    logger.info(
        f"{stream_file} belongs to run {found_run.id}, importing indexing result"
    )
    try:
        async with session.post(
            f"{args.amarcord_url}/api/indexing/import",
            json=JsonImportFinishedIndexingJobInput(
                is_online=False,
                cell_description=cell_description
                if cell_description is not None
                else "",
                command_line=stream_file_data.command_line,
                source="raw",
                run_internal_id=found_run.id,
                stream_file=str(stream_file),
                program_version=stream_file_data.program_version,
                frames=stream_file_data.frames,
                hits=stream_file_data.hits,
                indexed_frames=stream_file_data.indexed,
                align_detector_groups=[],
                geometry_contents=stream_file_data.geometry,
                generated_geometry_file=None,
                job_log="",
            ).model_dump(),
        ) as resp:
            indexing_result = JsonImportFinishedIndexingJobOutput(**await resp.json())
            await _try_send_event(
                args,
                session,
                "info",
                f"indexing result {indexing_result.indexing_result_id} created succesfully",
            )
            _mark_stream_as_completed(args, stream_file)
    except Exception as e:
        await _try_send_event(
            args, session, "error", f"indexing result creation failed: {e}"
        )
        _mark_stream_as_completed(args, stream_file)
        return


async def _main_loop_iteration(
    args: Arguments,
    attributo_config_file: ID29AttributoConfigFile,
    session: aiohttp.ClientSession,
    error_cache: ID29ErrorCache,
) -> None:
    logger.info("=> iterating over new files")
    try:
        async with session.get(
            f"{args.amarcord_url}/api/attributi/{args.amarcord_beamtime_id}"
        ) as resp:
            attributi_list = JsonReadAttributi(**await resp.json())

        attributi_by_name: dict[str, JsonAttributo] = {
            a.name: a for a in attributi_list.attributi
        }

        sample_attribute = attributi_by_name.get(args.sample_attributo)
        if sample_attribute is None:
            logger.info(f"cannot get ID for sample attribute {args.sample_attributo}")
            return
        tag_attribute = attributi_by_name.get(args.tag_attributo)
        if tag_attribute is None:
            logger.info(f"cannot get ID for tag attribute {args.tag_attributo}")
            return
        directory_attribute = attributi_by_name.get(ID29_MAGIC_DIRECTORY_ATTRIBUTO)
        if directory_attribute is None:
            logger.info(
                f"cannot get ID for directory attribute {ID29_MAGIC_DIRECTORY_ATTRIBUTO}"
            )
            return

        for a_in_config in attributo_config_file.attributi:
            a = attributi_by_name.get(a_in_config.attributo_name)
            if a is None:
                logger.error(
                    f"couldn't get attributo ID for name '{a_in_config.attributo_name}', check AMARCORD and the daemon config"
                )
                return
            a_in_config.attributo_id = a.id
    except Exception as e:
        logger.error(f"error reading attributi from AMARCORD: {e}")
        return

    try:
        async with session.get(
            f"{args.amarcord_url}/api/chemicals/{args.amarcord_beamtime_id}"
        ) as resp:
            chemicals_result = JsonReadChemicals(**await resp.json())
    except Exception as e:
        logger.info(f"error reading chemicals from AMARCORD: {e}, leaving this one out")
        return

    try:
        for new_metadata_file in _retrieve_new_metadata_files(args):
            await _ingest_new_metadata_file(
                args,
                session,
                sample_attribute.id,
                tag_attribute.id,
                directory_attribute.id,
                chemicals_result.chemicals,
                metadata_file=new_metadata_file,
                attributo_config_file=attributo_config_file,
                error_cache=error_cache,
            )
    except Exception as e:
        logger.info(f"unexpected exception retrieving metadata files: {e}")
        return

    try:
        for new_stream_file in _retrieve_new_stream_files(args):
            await _ingest_new_stream_file(args, session, new_stream_file)
    except Exception as e:
        logger.info(f"unexpected exception ingesting stream files: {e}")
        return

    logger.info("<= iterating over new files")


async def _create_attributi(
    args: Arguments,
    session: aiohttp.ClientSession,
    attributo_config_file: ID29AttributoConfigFile,
) -> None:
    attributi_schema: list[JsonCreateAttributiFromSchemaSingleAttributo] = []
    for a in attributo_config_file.attributi:
        attributo_type = (
            JSONSchemaNumber(
                type="number",
                format=None if a.attributo_unit is None else "standard-unit",
                suffix=a.attributo_unit,
            )
            if a.attributo_type == ID29AttributoType.ATTRIBUTO_TYPE_NUMBER
            else JSONSchemaString(type="string")
            if a.attributo_type == ID29AttributoType.ATTRIBUTO_TYPE_STRING
            else JSONSchemaBoolean(type="boolean")
            if a.attributo_type == ID29AttributoType.ATTRIBUTO_TYPE_BOOLEAN
            else JSONSchemaInteger(type="integer")
        )
        attributi_schema.append(
            JsonCreateAttributiFromSchemaSingleAttributo(
                attributo_name=a.attributo_name,
                attributo_type=attributo_type,
                description="",
            )
        )
    attributi_schema.append(
        JsonCreateAttributiFromSchemaSingleAttributo(
            attributo_name=args.sample_attributo,
            attributo_type=JSONSchemaInteger(type="integer", format="chemical-id"),
            description="",
        )
    )
    attributi_schema.append(
        JsonCreateAttributiFromSchemaSingleAttributo(
            attributo_name=args.tag_attributo,
            attributo_type=JSONSchemaString(type="string"),
            description="",
        )
    )
    async with session.post(
        f"{args.amarcord_url}/api/attributi/schema",
        json=JsonCreateAttributiFromSchemaInput(
            attributi_schema=attributi_schema,
            beamtime_id=BeamtimeId(args.amarcord_beamtime_id),
        ).model_dump(),
    ):
        logger.info(f"created all {len(attributo_config_file.attributi)} attributi")


async def _main_loop(args: Arguments) -> None:
    config_file = id29_parse_attributo_config_file(args.attributo_config_file)
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(5),
        auth=aiohttp.BasicAuth(
            login=args.amarcord_user, password=args.amarcord_password
        ),
        raise_for_status=True,
    ) as session:
        if args.create_attributi:
            await _create_attributi(args, session, config_file)
        error_cache = ID29ErrorCache(unknown_chemicals=set())
        while True:
            await _main_loop_iteration(args, config_file, session, error_cache)
            await asyncio.sleep(120)


def main() -> None:
    def runner(args: Arguments) -> None:
        asyncio.run(_main_loop(args))

    Parser(Arguments).bind(runner).run()


if __name__ == "__main__":  # pragma: no cover
    main()
