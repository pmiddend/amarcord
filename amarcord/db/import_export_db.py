import datetime
import time
from collections.abc import Awaitable
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from typing import BinaryIO
from typing import Final

import anyio
import structlog
from sqlalchemy import func
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import make_transient
from sqlalchemy.orm import selectinload
from structlog.stdlib import BoundLogger

from amarcord.cli.crystfel_index import sha256_bytes
from amarcord.db import orm
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.orm_utils import migrate
from amarcord.util import bz2_compress_async
from amarcord.util import bz2_decompress_async
from amarcord.util import rmdir_async
from amarcord.util import zip_compress_async
from amarcord.util import zip_decompress_async
from amarcord.web.fastapi_utils import get_orm_sessionmaker_with_url

logger = structlog.stdlib.get_logger(__name__)


async def import_db(
    import_beamtime_id: BeamtimeId,
    import_into_session: AsyncSession,
    import_from_session: AsyncSession,
    new_title: str | None,
    new_output_path: str | None,
) -> int:
    # Get the beamtime without any attached objects, just to see if
    # it's there, and then use it as a basis for the new beamtime in
    # the other DB.
    beamtime_sparse = (
        await import_from_session.scalars(
            select(orm.Beamtime).where(orm.Beamtime.id == import_beamtime_id)
        )
    ).one_or_none()

    if beamtime_sparse is None:
        raise Exception(f"Cannot find beamtime {import_beamtime_id} in source DB.")

    if (
        await import_into_session.scalars(
            select(func.count())
            .select_from(orm.Beamtime)
            .where(
                orm.Beamtime.title
                == (beamtime_sparse.title if new_title is None else new_title)
            )
        )
    ).one() >= 1:
        raise Exception(
            'The target DB already contains a beamtime with title "'
            + (beamtime_sparse.title if new_title is None else new_title)
            + '"'
        )

    # 1. add the sparse beamtime so we have something to add attributi to
    make_transient(beamtime_sparse)
    if new_title is not None:
        logger.info(f'Title of new beamtime will be "{new_title}".')
        beamtime_sparse.title = new_title
    if new_output_path is not None:
        logger.info(
            f'Analysis output path of new beamtime will be "{new_output_path}".'
        )
        beamtime_sparse.analysis_output_path = new_output_path
    beamtime_sparse.id = None  # ty:ignore[invalid-assignment]
    import_into_session.add(beamtime_sparse)
    await import_into_session.flush()

    new_beamtime_id = beamtime_sparse.id
    logger.info(f"Added beamtime to target DB. New ID is {new_beamtime_id}.")

    # From here on out the principle of this function is,
    # unfortunately, a bit laborious and manual, but it works:
    #
    # - Take an object (file, run, ...) from the source DB
    # - Remember its original ID
    # - Make it transient (meaning decouple it from the source DB ORM)
    # - Add it to the target DB
    # - Remember the new ID as well
    #
    # If you encounter the original object ID in some other ORM
    # entities, replace it with the new one.

    # Here we copy all files, which might be too much, but it's
    # annoying to pull files out of events, indexing results etc.
    files_old_to_new_id: dict[int, int] = {}
    for o in (await import_from_session.scalars(select(orm.File))).all():
        old_id = o.id
        # contents are deferred, so we load them here. Probably not
        # the right way to do it, to be fixed later.
        await o.awaitable_attrs.contents
        make_transient(o)
        o.id = None  # ty:ignore[invalid-assignment]
        import_into_session.add(o)
        await import_into_session.flush()
        files_old_to_new_id[old_id] = o.id

    if files_old_to_new_id:
        logger.info(f"Added {len(files_old_to_new_id)} file(s) to new DB.")

    beamtime = (
        await import_from_session.scalars(
            select(orm.Beamtime)
            .where(
                orm.Beamtime.id == import_beamtime_id,
            )
            .options(selectinload(orm.Beamtime.attributi))
            .options(
                selectinload(orm.Beamtime.geometries).selectinload(
                    orm.Geometry.attributi
                )
            )
            .options(
                selectinload(orm.Beamtime.experiment_types).selectinload(
                    orm.ExperimentType.data_sets
                )
            )
            .options(selectinload(orm.Beamtime.schedules))
            .options(
                selectinload(orm.Beamtime.chemicals).selectinload(orm.Chemical.files)
            )
            .options(selectinload(orm.Beamtime.events).selectinload(orm.EventLog.files))
            .options(selectinload(orm.Beamtime.runs).selectinload(orm.Run.files))
            .options(
                selectinload(orm.Beamtime.runs).selectinload(orm.Run.indexing_results)
            )
        )
    ).one()

    attributi_old_to_new_id: dict[int, int] = {}
    for o in beamtime.attributi:
        old_id = o.id
        make_transient(o)
        o.id = None  # ty:ignore[invalid-assignment]
        o.beamtime_id = new_beamtime_id
        import_into_session.add(o)
        await import_into_session.flush()
        attributi_old_to_new_id[old_id] = o.id

    logger.info(f"Added {len(attributi_old_to_new_id)} attributi to new DB.")

    chemicals_old_to_new_id: dict[int, int] = {}
    for o in beamtime.chemicals:
        old_id = o.id
        make_transient(o)
        o.id = None  # ty:ignore[invalid-assignment]
        new_o = orm.Chemical(
            beamtime_id=new_beamtime_id,
            name=o.name,
            responsible_person=o.responsible_person,
            modified=o.modified,
            type=o.type,
        )
        for f in o.files:
            new_file = await import_into_session.get(
                orm.File, files_old_to_new_id[f.id]
            )
            assert new_file is not None
            new_o.files.append(new_file)
        for chav in o.attributo_values:
            make_transient(chav)
            chav.attributo_id = attributi_old_to_new_id[chav.attributo_id]
            new_o.attributo_values.append(chav)
        import_into_session.add(new_o)
        await import_into_session.flush()
        chemicals_old_to_new_id[old_id] = new_o.id

    logger.info(f"Added {len(chemicals_old_to_new_id)} chemicals to new DB.")

    geometries_old_to_new_id: dict[int, int] = {}
    for o in beamtime.geometries:
        old_id = o.id
        new_o = orm.Geometry(
            beamtime_id=new_beamtime_id,
            content=o.content,
            geometry_type=o.geometry_type,
            hash=o.hash,
            name=o.name,
            created=o.created,
        )
        for a in o.attributi:
            new_attributo = await import_into_session.get(
                orm.Attributo, attributi_old_to_new_id[a.id]
            )
            assert new_attributo is not None
            new_o.attributi.append(new_attributo)
        import_into_session.add(new_o)
        await import_into_session.flush()
        geometries_old_to_new_id[old_id] = new_o.id

    logger.info(f"Added {len(geometries_old_to_new_id)} geometries to new DB.")

    events_old_to_new_id: dict[int, int] = {}
    for o in beamtime.events:
        old_id = o.id
        new_o = orm.EventLog(
            beamtime_id=new_beamtime_id,
            created=o.created,
            level=o.level,
            source=o.source,
            text=o.text,
        )
        for f in o.files:
            new_file = await import_into_session.get(
                orm.File, files_old_to_new_id[f.id]
            )
            assert new_file is not None
            new_o.files.append(new_file)
        import_into_session.add(new_o)
        await import_into_session.flush()
        events_old_to_new_id[old_id] = new_o.id

    logger.info(f"Added {len(events_old_to_new_id)} events to new DB.")

    experiment_types_old_to_new_id: dict[int, int] = {}
    for o in beamtime.experiment_types:
        old_id = o.id
        new_o = orm.ExperimentType(beamtime_id=new_beamtime_id, name=o.name)
        for a in o.attributi:
            new_attributo = await import_into_session.get(
                orm.Attributo, attributi_old_to_new_id[a.attributo_id]
            )
            assert new_attributo is not None
            new_o.attributi.append(
                orm.ExperimentHasAttributo(
                    attributo_id=attributi_old_to_new_id[a.attributo_id],
                    chemical_role=a.chemical_role,
                )
            )
        import_into_session.add(new_o)
        await import_into_session.flush()
        experiment_types_old_to_new_id[old_id] = new_o.id

    logger.info(
        f"Added {len(experiment_types_old_to_new_id)} experiment types to new DB."
    )

    data_sets_old_to_new_id: dict[int, int] = {}
    for o in (
        await import_from_session.scalars(
            select(orm.DataSet).where(
                orm.DataSet.experiment_type_id.in_(
                    experiment_types_old_to_new_id.keys()
                )
            )
        )
    ).all():
        old_id = o.id
        new_o = orm.DataSet(
            experiment_type_id=experiment_types_old_to_new_id[o.experiment_type_id]
        )
        for dhav in o.attributo_values:
            make_transient(dhav)
            # Not sure what ty's problem is here
            dhav.attributo_id = attributi_old_to_new_id[dhav.attributo_id]  # ty:ignore[invalid-assignment]
            if dhav.chemical_value is not None:
                logger.info(
                    f"chemical value {dhav.chemical_value}, new {chemicals_old_to_new_id[dhav.chemical_value]}"
                )
                dhav.chemical_value = chemicals_old_to_new_id[dhav.chemical_value]
            new_o.attributo_values.append(dhav)
        import_into_session.add(new_o)
        await import_into_session.flush()
        data_sets_old_to_new_id[old_id] = new_o.id

    logger.info(f"Added {len(data_sets_old_to_new_id)} experiment types to new DB.")

    old_indexing_result_ids: list[int] = []
    runs_old_to_new_id: dict[int, int] = {}
    for o in beamtime.runs:
        old_id = o.id
        old_indexing_result_ids.extend(ir.id for ir in o.indexing_results)
        make_transient(o)
        o.id = None  # ty:ignore[invalid-assignment]
        new_o = orm.Run(
            beamtime_id=new_beamtime_id,
            external_id=o.external_id,
            modified=o.modified,
            started=o.started,
            stopped=o.stopped,
            experiment_type_id=experiment_types_old_to_new_id[o.experiment_type_id],
        )
        for rhav in o.attributo_values:
            make_transient(rhav)
            # Not sure what ty's problem is here
            rhav.attributo_id = attributi_old_to_new_id[rhav.attributo_id]  # ty:ignore[invalid-assignment]
            if rhav.chemical_value is not None:
                rhav.chemical_value = chemicals_old_to_new_id[rhav.chemical_value]
            new_o.attributo_values.append(rhav)
        for f in o.files:
            new_o.files.append(orm.RunHasFiles(glob=f.glob, source=f.source))
        import_into_session.add(new_o)
        await import_into_session.flush()
        runs_old_to_new_id[old_id] = new_o.id

    logger.info(f"Added {len(runs_old_to_new_id)} runs to new DB.")

    indexing_results_old_to_new_id: dict[int, int] = {}
    indexing_parameters_old_to_new_id: dict[int, int] = {}
    merge_results_old_to_new_id: dict[int, int] = {}
    for o in (
        await import_from_session.scalars(
            select(orm.IndexingResult)
            .where(orm.IndexingResult.id.in_(old_indexing_result_ids))
            .options(
                selectinload(orm.IndexingResult.merge_results).selectinload(
                    orm.MergeResult.refinement_results
                )
            )
            .options(selectinload(orm.IndexingResult.statistics))
            .options(selectinload(orm.IndexingResult.template_replacements))
            .options(selectinload(orm.IndexingResult.align_detector_groups))
            .options(selectinload(orm.IndexingResult.indexing_parameters))
        )
    ).all():
        # Multiple indexing results can share a parameters object, so
        # don't break that mechanism while copying
        if o.indexing_parameters_id in indexing_parameters_old_to_new_id:
            ip_id = indexing_parameters_old_to_new_id[o.indexing_parameters.id]
        else:
            ip = o.indexing_parameters
            old_id = ip.id
            make_transient(ip)
            ip.id = None  # ty:ignore[invalid-assignment]
            ip.geometry_id = geometries_old_to_new_id.get(ip.geometry_id)
            import_into_session.add(ip)
            await import_into_session.flush()
            ip_id = ip.id
            indexing_parameters_old_to_new_id[old_id] = ip_id

        old_id = o.id
        new_o = orm.IndexingResult(
            created=o.created,
            run_id=orm.RunInternalId(runs_old_to_new_id[o.run_id]),
            stream_file=o.stream_file,
            program_version=o.program_version,
            frames=o.frames,
            hits=o.hits,
            indexed_frames=o.indexed_frames,
            generated_geometry_id=geometries_old_to_new_id.get(o.generated_geometry_id),
            job_id=o.job_id,
            job_status=o.job_status,
            job_error=o.job_error,
            job_latest_log=o.job_latest_log,
            job_started=o.job_started,
            job_stopped=o.job_stopped,
            indexing_parameters_id=ip_id,
            unit_cell_histograms_file_id=files_old_to_new_id.get(
                o.unit_cell_histograms_file_id
            ),
        )
        import_into_session.add(new_o)
        for adg in o.align_detector_groups:
            make_transient(adg)
            adg.id = None  # ty: ignore[invalid-assignment]
            adg.indexing_result_id = None  # ty: ignore[invalid-assignment]
            new_o.align_detector_groups.append(adg)
        for stat in o.statistics:
            make_transient(stat)
            stat.indexing_result_id = None  # ty: ignore[invalid-assignment]
            new_o.statistics.append(stat)
        for tr in o.template_replacements:
            make_transient(tr)
            tr.id = None  # ty: ignore[invalid-assignment]
            tr.indexing_result_id = None  # ty: ignore[invalid-assignment]
            tr.attributo_id = attributi_old_to_new_id[tr.attributo_id]
            new_o.template_replacements.append(tr)
        import_into_session.add(new_o)
        await import_into_session.flush()
        indexing_results_old_to_new_id[old_id] = new_o.id

        for mr in o.merge_results:
            logger.info("Iterating over merge result.")
            make_transient(mr)
            mr.id = None  # ty: ignore[invalid-assignment]
            mr.mtz_file_id = files_old_to_new_id.get(mr.mtz_file_id)
            for mrsf in mr.shell_foms:
                make_transient(mrsf)
                mrsf.id = None  # ty: ignore[invalid-assignment]
            for rr in mr.refinement_results:
                mtz_file_id = files_old_to_new_id[rr.mtz_file_id]
                pdb_file_id = files_old_to_new_id[rr.pdb_file_id]
                make_transient(rr)
                rr.id = None  # ty: ignore[invalid-assignment]
                rr.mtz_file_id = mtz_file_id
                rr.pdb_file_id = pdb_file_id
            mr.indexing_results.append(new_o)
            mr.mtz_file_id = files_old_to_new_id.get(mr.mtz_file_id)
            import_into_session.add(mr)
            await import_into_session.flush()
            merge_results_old_to_new_id[old_id] = mr.id
    logger.info(
        f"Added {len(indexing_results_old_to_new_id)} indexing results to new DB."
    )

    for o in beamtime.schedules:
        new_o = orm.BeamtimeSchedule(
            beamtime_id=new_beamtime_id,
            users=o.users,
            td_support=o.td_support,
            comment=o.comment,
            shift=o.shift,
            date=o.date,
        )
        for c in await o.awaitable_attrs.chemicals:
            new_chem = await import_into_session.get(
                orm.Chemical, chemicals_old_to_new_id[c.id]
            )
            assert new_chem is not None
            new_o.chemicals.append(new_chem)
        import_into_session.add(new_o)
        await import_into_session.flush()

    configurations_old_to_new_id: dict[int, int] = {}
    for o in await beamtime.awaitable_attrs.configurations:
        old_id = o.id
        online_indexing_params: (
            orm.IndexingParameters | None
        ) = await o.awaitable_attrs.current_online_indexing_parameters
        new_coipid = (
            indexing_parameters_old_to_new_id.get(online_indexing_params.id)
            if online_indexing_params is not None
            else None
        )
        new_o = orm.UserConfiguration(
            beamtime_id=new_beamtime_id,
            created=o.created,
            auto_pilot=o.auto_pilot,
            use_online_crystfel=o.use_online_crystfel,
            current_experiment_type_id=experiment_types_old_to_new_id.get(
                o.current_experiment_type_id
            ),
            current_online_indexing_parameters_id=new_coipid,
        )
        import_into_session.add(new_o)
        await import_into_session.flush()
        configurations_old_to_new_id[old_id] = new_o.id
    logger.info(f"Added {len(configurations_old_to_new_id)} configurations to new DB.")

    await import_into_session.flush()
    logger.info(f"Merged beamtime, new ID: {new_beamtime_id}.")
    return new_beamtime_id


_EXPORT_WORKING_DIR: Final = "amarcord-export"
_EXPORT_DB_FILE_NAME: Final = "db.sqlite"
_EXPORT_STREAM_FILE_DIRECTORY: Final = "stream-files"


@dataclass(frozen=True)
class _CopyStreamFileResult:
    stream_files: int
    size_mib: float


def _format_eta(t: datetime.timedelta) -> str:
    hours, remainder = divmod(t.seconds, 3600)
    minutes, _ = divmod(remainder, 60)
    return f"{hours}h{minutes}min"


async def _copy_stream_files(
    bound_logger: BoundLogger,
    import_into_url: str,
    working_dir: anyio.Path,
    update_status: Callable[[str, int | None], Awaitable[None]],
) -> _CopyStreamFileResult:
    bound_logger.info("Importing done, now reading stream files.")

    # Find out which indexing results are to be archived. Then close
    # the session so we don't have a long transaction while copying
    # and can update the copy status dynamically.
    stream_files: list[anyio.Path] = []
    async with get_orm_sessionmaker_with_url(import_into_url)() as import_into_session:
        stream_files.extend(
            anyio.Path(ir.stream_file)
            for ir in await import_into_session.scalars(
                select(orm.IndexingResult).where(
                    (orm.IndexingResult.stream_file.is_not(None))
                    & (orm.IndexingResult.stream_file != "")
                )
            )
            # Redundant check, only there for type-checking.
            if ir.stream_file is not None
        )

    # We treat nonexisting stream files in a benign way, so we have to
    # ignore the file size of these as well as ignore them further
    # down
    async def file_size_or_zero(x: anyio.Path) -> int:
        try:
            return (await x.stat()).st_size
        except:
            return 0

    # Used for progress and the ETA display.
    file_sizes: list[int] = [await file_size_or_zero(x) for x in stream_files]
    total_uncompressed_size_bytes = sum(file_sizes)
    remaining_uncompressed_size_bytes = total_uncompressed_size_bytes

    # Actually copy the stream files now.
    stream_file_dir = working_dir / _EXPORT_STREAM_FILE_DIRECTORY
    await stream_file_dir.mkdir()
    bound_logger.info(
        f"{len(stream_files)} stream files assembled, now copying and compressing them."
    )

    total_size_mib = 0
    total_files = len(stream_files)
    compression_mib_per_second: float | None = None
    for file_no, original_path in enumerate(stream_files, start=1):
        stream_bound_logger = bound_logger.bind(stream_file=original_path.name)
        if not await original_path.is_file():
            stream_bound_logger.warning(
                f'Stream file "{original_path}" does not exist, skipping.'
            )
            continue
        path_hash = sha256_bytes(str(original_path).encode("utf-8"))
        stream_bound_logger.info(f'Compressing "{original_path}".')
        status_to_send = f"Compressing {file_no}/{total_files} ({file_no / total_files * 100:.2f}%). {total_size_mib:.2f}MiB so far."
        if compression_mib_per_second is not None:
            remaining_uncompressed_size_mib = (
                remaining_uncompressed_size_bytes / 1024 / 1024
            )
            remaining = datetime.timedelta(
                seconds=remaining_uncompressed_size_mib / compression_mib_per_second
            )
            status_to_send += f". ETA: {_format_eta(remaining)}."
        await update_status(status_to_send, None)
        compression_start_s = time.time()
        await bz2_compress_async(original_path, (stream_file_dir / path_hash))
        compression_end_s = time.time()
        file_size_before_bytes = (await original_path.stat()).st_size
        file_size_before_mib = file_size_before_bytes / 1024 / 1024
        compression_mib_per_second = file_size_before_mib / (
            compression_end_s - compression_start_s
        )
        remaining_uncompressed_size_bytes -= file_size_before_bytes
        file_size_after_mib = (
            (await (stream_file_dir / path_hash).stat()).st_size / 1024 / 1024
        )
        stream_bound_logger.info(
            f"{file_size_before_mib:.2f}MiB -> {file_size_after_mib:.2f}MiB"
        )
        total_size_mib += file_size_after_mib

    return _CopyStreamFileResult(
        stream_files=len(stream_files), size_mib=total_size_mib
    )


async def _export_db_inner(
    bound_logger: BoundLogger,
    export_from_url: str,
    working_dir_top: anyio.Path,
    beamtime_id: BeamtimeId,
    zip_file_name: anyio.Path,
    with_stream_files: bool,
    update_status: Callable[[str, int | None], Awaitable[None]],
) -> None:
    working_dir_name = _EXPORT_WORKING_DIR
    working_dir = working_dir_top / working_dir_name
    await working_dir.mkdir(parents=True)
    import_into_url = f"sqlite+aiosqlite:///{working_dir}/{_EXPORT_DB_FILE_NAME}"
    bound_logger.info(f'Importing DB to "{import_into_url}".')
    engine = create_async_engine(import_into_url)
    # First step is to create ("migrate") target DB (which will only
    # be used for this zip file, so it will not exist beforehand)
    await migrate(engine)

    # Next, fill this new DB so we can forget about the original DB from here on out
    async with (
        get_orm_sessionmaker_with_url(export_from_url)() as export_from_session,
        get_orm_sessionmaker_with_url(import_into_url)() as import_into_session,
    ):
        await import_db(
            beamtime_id,
            import_into_session,
            export_from_session,
            new_title=None,
            new_output_path=None,
        )
        await import_into_session.commit()

    stream_copy: _CopyStreamFileResult | None
    if with_stream_files:
        stream_copy = await _copy_stream_files(
            bound_logger, import_into_url, working_dir, update_status
        )
    else:
        stream_copy = None

    await update_status("Writing zip file...", None)
    await zip_compress_async(
        zip_file_name,
        root_dir=anyio.Path(working_dir_top),
        base_dir_relative=working_dir_name,
    )
    # Remove the working dir again - it might contain huge stream files in the end.
    try:
        await rmdir_async(working_dir)
    except:
        logger.exception(
            f'Error removing working directory "{working_dir}", continuing export still.'
        )
    await update_status(
        f"Done. {stream_copy.stream_files} stream files, {stream_copy.size_mib:.2f}MiB compressed."
        if stream_copy is not None
        else "Done.",
        max(1, (await zip_file_name.stat()).st_size // 1024 // 1024),
    )
    bound_logger.info(f'Writing zip file "{zip_file_name}" done.')


async def export_db(export_from_url: str, export_job_id: int) -> None:
    bound_logger = logger.bind(export_job_id=export_job_id)

    async def with_job(f: Callable[[orm.ExportJob], None]) -> None:
        async with get_orm_sessionmaker_with_url(
            export_from_url
        )() as export_from_session:
            export_job = await export_from_session.get(orm.ExportJob, export_job_id)
            assert export_job is not None
            f(export_job)
            await export_from_session.commit()

    # Step 0: Get information about the export to be done
    async with get_orm_sessionmaker_with_url(export_from_url)() as session:
        export_job = await session.get(orm.ExportJob, export_job_id)
        assert export_job is not None
        bound_logger.info('Setting "started" time stamp.')
        export_job.started = datetime.datetime.now(datetime.UTC)
        await session.commit()
        session.expunge(export_job)

    async def update_status(status: str, size_in_mebibytes: int | None) -> None:
        bound_logger.info(status)

        def set_status(x: orm.ExportJob) -> None:
            x.status_message = status
            if size_in_mebibytes is not None:
                bound_logger.info(f"Final is size: {size_in_mebibytes}MiB.")
                x.size_in_mebibytes = size_in_mebibytes

        await with_job(set_status)

    try:
        output_path_parent = anyio.Path(export_job.output_path).parent
        if not await output_path_parent.is_dir():
            raise Exception(
                f'Directory to put the exported file into "{output_path_parent}" does not exist. Please create it before starting to export.'
            )
        working_dir_top = output_path_parent / f"{export_job_id}-working-directory"
        # Call an inner function to not have a deeply nested
        # indent block with an exception at the very end.
        await _export_db_inner(
            logger.bind(export_job_id=export_job_id),
            export_from_url,
            anyio.Path(working_dir_top),
            export_job.beamtime_id,
            anyio.Path(export_job.output_path),
            with_stream_files=export_job.contains_stream_files,
            update_status=update_status,
        )
    except Exception as e:
        bound_logger.exception("Error exporting")
        await update_status(f"Error: {e}", None)
        raise
    finally:

        def set_stopped(job: orm.ExportJob) -> None:
            bound_logger.info('Setting "stopped" time stamp.')
            job.stopped = datetime.datetime.now(datetime.UTC)

        await with_job(set_stopped)


async def import_db_from_zip_file(
    file_name: Path, stream_file_dir: anyio.Path, import_to_db_connection_url: str
) -> None:
    with file_name.open("rb") as zip_obj:
        await import_db_from_zip(
            zip_obj,
            stream_file_dir,
            import_to_db_connection_url,
            file_name=file_name.name,
            new_title=None,
            new_output_path=None,
        )


async def import_db_from_zip(
    file_obj: BinaryIO,
    stream_file_dir: anyio.Path,
    import_to_db_connection_url: str,
    file_name: str | None,
    new_title: str | None,
    new_output_path: str | None,
) -> int:
    extracted_stream_files: list[anyio.Path] = []
    try:
        return await import_db_from_zip_inner(
            file_obj,
            stream_file_dir,
            import_to_db_connection_url,
            file_name,
            new_title,
            new_output_path,
            extracted_stream_files,
        )
    except:
        logger.info("Cleaning extracted stream files after exception.")
        for f in extracted_stream_files:
            logger.info(f'Removing "{f}".')
            await f.unlink()
        logger.info("Now reraising exception after cleanup.")
        raise


async def import_db_from_zip_inner(
    file_obj: BinaryIO,
    stream_file_dir: anyio.Path,
    import_to_db_connection_url: str,
    file_name: str | None,
    new_title: str | None,
    new_output_path: str | None,
    extracted_stream_files: list[anyio.Path],
) -> int:
    async with anyio.TemporaryDirectory() as working_dir_top:
        await zip_decompress_async(file_obj, anyio.Path(working_dir_top))
        extracted_zip_file_base_dir = anyio.Path(working_dir_top) / _EXPORT_WORKING_DIR
        if not await extracted_zip_file_base_dir.is_dir():
            raise Exception(
                f"cannot find directory {_EXPORT_WORKING_DIR} in zip file {file_name}"
            )
        import_from_db_path = extracted_zip_file_base_dir / _EXPORT_DB_FILE_NAME
        if not await import_from_db_path.is_file():
            raise Exception(
                f"cannot find DB file {_EXPORT_DB_FILE_NAME} in directory {_EXPORT_WORKING_DIR} in zip file {file_name}"
            )
        async with (
            get_orm_sessionmaker_with_url(
                import_to_db_connection_url
            )() as import_into_session,
            get_orm_sessionmaker_with_url(
                f"sqlite+aiosqlite:///{import_from_db_path}"
            )() as import_from_session,
        ):
            try:
                single_beamtime = (
                    await import_from_session.scalars(select(orm.Beamtime))
                ).one_or_none()
                if single_beamtime is None:
                    raise Exception(
                        "Expected at least one beamtime in the imported DB, but found none"
                    )
                import_from_beamtime_id = BeamtimeId(single_beamtime.id)
            except:
                raise Exception(
                    "Found more than one beamtime in the exported database. This is not supported yet."
                )
            logger.info(
                f"Found beamtime {import_from_beamtime_id} in zip file, importing."
            )
            import_into_beamtime_id = await import_db(
                import_from_beamtime_id,
                import_into_session,
                import_from_session,
                new_title,
                new_output_path,
            )

            logger.info(
                "Import complete, now decompressing indexing result stream files."
            )
            number_of_indexing_results = 0
            for ir in (
                await import_into_session.scalars(
                    select(orm.IndexingResult)
                    .join(orm.IndexingResult.run)
                    .where(orm.Run.beamtime_id == import_into_beamtime_id)
                )
            ).all():
                bound_logger = logger.bind(ir_id=ir.id)
                if ir.stream_file is None:
                    bound_logger.info(
                        "Skipping indexing result, no stream file present."
                    )
                    continue
                number_of_indexing_results += 1
                source_path = Path(ir.stream_file)
                source_path_hash = sha256_bytes(str(source_path).encode("utf-8"))
                hashed_file_path = (
                    extracted_zip_file_base_dir
                    / _EXPORT_STREAM_FILE_DIRECTORY
                    / source_path_hash
                )
                if not await hashed_file_path.is_file():
                    bound_logger.info(
                        f'Should have a file "{source_path_hash}" for "{source_path}" in the "{_EXPORT_STREAM_FILE_DIRECTORY}" directory in the zip file, but it doesn\'t. Was this not exported for some reason? Skipping.'
                    )
                    continue
                await stream_file_dir.mkdir(exist_ok=True, parents=True)
                target_file_name = stream_file_dir / source_path.name
                duplicate_counter = 1
                while await target_file_name.is_file():
                    target_file_name = (
                        stream_file_dir
                        / f"{source_path.stem}-{duplicate_counter}{source_path.suffix}"
                    )
                    bound_logger.info(
                        f'Target file already present, trying new name: "{target_file_name}".'
                    )
                    duplicate_counter += 1
                bound_logger.info(
                    f'Moving and extracting "{hashed_file_path}" => "{target_file_name}".'
                )
                await bz2_decompress_async(hashed_file_path, target_file_name)
                # Remember these files so we can delete them if there is an exception!
                extracted_stream_files.append(target_file_name)
                bound_logger.info(
                    f'Changing stream file DB attribute from "{ir.stream_file}" to "{target_file_name}".'
                )
                ir.stream_file = str(target_file_name)
            logger.info("Decompression finished - import from zip file finished.")

            await import_into_session.commit()
            return number_of_indexing_results
