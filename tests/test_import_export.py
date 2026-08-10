import datetime
from pathlib import Path
from typing import Final

import anyio
import pytest
import structlog
from sqlalchemy import select
from sqlalchemy.ext.asyncio import create_async_engine
from sqlalchemy.orm import selectinload

from amarcord.db import orm
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.geometry_type import GeometryType
from amarcord.db.import_export_db import export_db
from amarcord.db.import_export_db import import_db
from amarcord.db.import_export_db import import_db_from_zip_file
from amarcord.db.merge_model import MergeModel
from amarcord.db.orm_utils import migrate
from amarcord.db.run_external_id import RunExternalId
from amarcord.db.scale_intensities import ScaleIntensities
from amarcord.web.fastapi_utils import get_orm_sessionmaker_with_url

logger = structlog.stdlib.get_logger(__name__)

_TEST_CRYSTFEL_STREAM: Final = anyio.Path("tests") / "crystfel" / "test.stream"
_TEST_CRYSTFEL_STREAM_2: Final = (
    anyio.Path("tests") / "crystfel" / "second-stream" / "test.stream"
)


async def test_import_db_duplicate_title(tmp_path: Path) -> None:
    import_from_url = f"sqlite+aiosqlite:///{tmp_path}/from-db"
    import_to_url = f"sqlite+aiosqlite:///{tmp_path}/to-db"

    for url in (import_from_url, import_to_url):
        engine = create_async_engine(url)
        await migrate(engine)

    async with (
        get_orm_sessionmaker_with_url(import_from_url)() as import_into_session,
        get_orm_sessionmaker_with_url(import_to_url)() as import_from_session,
    ):
        start = datetime.datetime.now(datetime.UTC)
        end = start + datetime.timedelta(days=1)
        original_beamtime = orm.Beamtime(
            external_id="1",
            proposal="proposal",
            beamline="beamline",
            title="title",
            comment="comment",
            start=start,
            end=end,
            analysis_output_path="path",
        )
        import_from_session.add(original_beamtime)

        await import_from_session.commit()

        logger.info(f"added beamtime {original_beamtime.id}")

        await import_db(
            BeamtimeId(original_beamtime.id),
            import_into_session,
            import_from_session,
            new_title=None,
            new_output_path=None,
        )
        await import_into_session.commit()

        with pytest.raises(Exception):
            # This should raise, because the title is already present now
            await import_db(
                BeamtimeId(original_beamtime.id),
                import_into_session,
                import_from_session,
                new_title=None,
                new_output_path=None,
            )


async def test_import_db_change_title_and_output_path(tmp_path: Path) -> None:
    import_from_url = f"sqlite+aiosqlite:///{tmp_path}/from-db"
    import_to_url = f"sqlite+aiosqlite:///{tmp_path}/to-db"

    for url in (import_from_url, import_to_url):
        engine = create_async_engine(url)
        await migrate(engine)

    async with (
        get_orm_sessionmaker_with_url(import_from_url)() as import_into_session,
        get_orm_sessionmaker_with_url(import_to_url)() as import_from_session,
    ):
        start = datetime.datetime.now(datetime.UTC)
        end = start + datetime.timedelta(days=1)
        original_beamtime = orm.Beamtime(
            external_id="1",
            proposal="proposal",
            beamline="beamline",
            title="title",
            comment="comment",
            start=start,
            end=end,
            analysis_output_path="path",
        )
        import_from_session.add(original_beamtime)

        await import_from_session.commit()

        logger.info(f"added beamtime {original_beamtime.id}")

        # Import without changes
        beamtime_id_no_changes = await import_db(
            BeamtimeId(original_beamtime.id),
            import_into_session,
            import_from_session,
            new_title=None,
            new_output_path=None,
        )
        # Import with title and output path change
        beamtime_id_changes = await import_db(
            BeamtimeId(original_beamtime.id),
            import_into_session,
            import_from_session,
            new_title="newtitle",
            new_output_path="newpath",
        )
        await import_into_session.commit()

    async with get_orm_sessionmaker_with_url(import_from_url)() as import_into_session:
        beamtime_no_changes = await import_into_session.get(
            orm.Beamtime, beamtime_id_no_changes
        )
        beamtime_changes = await import_into_session.get(
            orm.Beamtime, beamtime_id_changes
        )

        assert beamtime_no_changes is not None
        assert beamtime_changes is not None

        assert beamtime_no_changes.title == "title"
        assert beamtime_no_changes.analysis_output_path == "path"
        assert beamtime_changes.title == "newtitle"
        assert beamtime_changes.analysis_output_path == "newpath"


_BEAMTIME_START = datetime.datetime(2020, 1, 1, 15, 0, 0, tzinfo=datetime.UTC)
_BEAMTIME_END = datetime.datetime(2020, 1, 2, 15, 0, 0, tzinfo=datetime.UTC)


async def _create_test_db(
    url: str, stream_file_1: anyio.Path, stream_file_2: anyio.Path
) -> BeamtimeId:
    async with get_orm_sessionmaker_with_url(url)() as import_from_session:
        original_beamtime = orm.Beamtime(
            external_id="1",
            proposal="proposal",
            beamline="beamline",
            title="title",
            comment="comment",
            start=_BEAMTIME_START,
            end=_BEAMTIME_END,
            analysis_output_path="path",
        )
        import_from_session.add(original_beamtime)

        await import_from_session.flush()

        logger.info(f"added beamtime {original_beamtime.id}")

        run_attributo = orm.Attributo(
            beamtime_id=original_beamtime.id,
            name="1",
            description="description",
            group="group",
            associated_table=AssociatedTable.RUN,
            json_schema={"type": "integer", "format": "chemical-id"},
        )
        original_beamtime.attributi.append(run_attributo)
        import_from_session.add(run_attributo)

        chemical_attributo = orm.Attributo(
            beamtime_id=original_beamtime.id,
            name="2",
            description="description",
            group="group",
            associated_table=AssociatedTable.CHEMICAL,
            json_schema={"type": "integer"},
        )
        original_beamtime.attributi.append(chemical_attributo)
        import_from_session.add(chemical_attributo)
        await import_from_session.flush()

        chemical = orm.Chemical(
            beamtime_id=original_beamtime.id,
            name="c1",
            responsible_person="p",
            modified=_BEAMTIME_START,
            type=ChemicalType.CRYSTAL,
        )
        chemical.attributo_values.append(
            orm.ChemicalHasAttributoValue(
                attributo_id=chemical_attributo.id,
                integer_value=1337,
                float_value=None,
                string_value=None,
                bool_value=None,
                datetime_value=None,
                list_value=None,
            )
        )
        chemical.files.append(
            orm.File(
                type="text/plain",
                file_name="test.txt",
                description="just a test",
                size_in_bytes=4,
                size_in_bytes_compressed=4,
                original_path="test.txt",
                sha256="",
                modified=_BEAMTIME_START,
                contents=b"foob",
            )
        )
        import_from_session.add(chemical)

        geometry = orm.Geometry(
            beamtime_id=original_beamtime.id,
            content="foo bar baz {{1}} qux",
            geometry_type=GeometryType.CRYSTFEL_STRING,
            hash="hash",
            name="first geometry",
            created=_BEAMTIME_START,
        )
        geometry.attributi.append(run_attributo)
        import_from_session.add(geometry)

        experiment_type = orm.ExperimentType(
            beamtime_id=original_beamtime.id,
            name="et",
            attributi=[
                orm.ExperimentHasAttributo(
                    attributo_id=run_attributo.id, chemical_role=ChemicalType.CRYSTAL
                )
            ],
        )
        import_from_session.add(experiment_type)
        await import_from_session.flush()

        run = orm.Run(
            external_id=RunExternalId(1),
            beamtime_id=original_beamtime.id,
            modified=_BEAMTIME_START,
            started=_BEAMTIME_START,
            stopped=_BEAMTIME_END,
            experiment_type_id=experiment_type.id,
            attributo_values=[
                orm.RunHasAttributoValue(
                    attributo_id=run_attributo.id,
                    integer_value=None,
                    chemical_value=chemical.id,
                    float_value=None,
                    string_value=None,
                    bool_value=None,
                    datetime_value=None,
                    list_value=None,
                )
            ],
            files=[orm.RunHasFiles(glob="*.h5", source="raw")],
        )

        import_from_session.add(run)

        data_set = orm.DataSet(experiment_type_id=experiment_type.id)
        data_set.attributo_values.append(
            orm.DataSetHasAttributoValue(
                attributo_id=run_attributo.id,
                integer_value=None,
                float_value=None,
                string_value=None,
                bool_value=None,
                datetime_value=None,
                list_value=None,
                chemical_value=chemical.id,
            )
        )
        import_from_session.add(data_set)
        await import_from_session.flush()

        indexing_parameters = orm.IndexingParameters(
            is_online=True,
            cell_description="",
            command_line="cli",
            geometry_id=geometry.id,
            source="source",
        )

        import_from_session.add(indexing_parameters)
        await import_from_session.flush()

        indexing_result_1 = orm.IndexingResult(
            created=_BEAMTIME_START,
            run_id=run.id,
            stream_file=str(stream_file_1),
            program_version="",
            frames=0,
            hits=0,
            indexed_frames=0,
            generated_geometry_id=None,
            unit_cell_histograms_file_id=None,
            job_id=None,
            job_status=DBJobStatus.QUEUED,
            job_error=None,
            job_latest_log="",
            job_started=None,
            job_stopped=None,
            indexing_parameters_id=indexing_parameters.id,
        )
        indexing_result_1.align_detector_groups.append(
            orm.AlignDetectorGroup(
                group="a",
                x_translation_mm=1.0,
                y_translation_mm=1.0,
                z_translation_mm=1.0,
                x_rotation_deg=1.0,
                y_rotation_deg=1.0,
            )
        )
        indexing_result_1.statistics.append(
            orm.IndexingResultHasStatistic(
                time=_BEAMTIME_END,
                frames=10,
                hits=5,
                indexed_crystals=6,
                indexed_frames=7,
            )
        )
        indexing_result_1.template_replacements.append(
            orm.GeometryTemplateReplacement(
                attributo_id=run_attributo.id, replacement="test"
            )
        )
        indexing_result_2 = orm.IndexingResult(
            created=_BEAMTIME_START,
            run_id=run.id,
            stream_file=str(stream_file_2),
            program_version="",
            frames=0,
            hits=0,
            indexed_frames=0,
            generated_geometry_id=None,
            unit_cell_histograms_file_id=None,
            job_id=None,
            job_status=DBJobStatus.QUEUED,
            job_error=None,
            job_latest_log="",
            job_started=None,
            job_stopped=None,
            indexing_parameters_id=indexing_parameters.id,
        )
        import_from_session.add(indexing_result_1)
        import_from_session.add(indexing_result_2)

        event = orm.EventLog(
            beamtime_id=BeamtimeId(original_beamtime.id),
            created=_BEAMTIME_START,
            level=EventLogLevel.INFO,
            source="mysource",
            text="hello world",
        )
        event.files.append(
            orm.File(
                type="text/plain",
                file_name="test.txt",
                description="just a test",
                size_in_bytes=4,
                size_in_bytes_compressed=4,
                original_path="test.txt",
                sha256="",
                modified=_BEAMTIME_START,
                contents=b"foobar",
            )
        )
        import_from_session.add(event)
        await import_from_session.flush()

        merge_result = orm.MergeResult(
            created=_BEAMTIME_START,
            recent_log="log",
            negative_handling=None,
            job_status=DBJobStatus.DONE,
            started=_BEAMTIME_START,
            stopped=_BEAMTIME_START,
            point_group="mmm",
            space_group="P1",
            cell_description="hexagonal F c (30 30 30) (40 40 40)",
            custom_split=None,
            dataset="",
            job_id=None,
            job_error=None,
            mtz_file_id=None,
            input_merge_model=MergeModel.UNITY,
            input_scale_intensities=ScaleIntensities.OFF,
            input_post_refinement=True,
            input_iterations=3,
            input_polarisation_angle=None,
            input_polarisation_percent=None,
            input_start_after=None,
            input_stop_after=None,
            input_rel_b=1.0,
            input_no_pr=False,
            input_force_bandwidth=None,
            input_force_radius=None,
            input_force_lambda=None,
            input_no_delta_cc_half=True,
            input_max_adu=None,
            input_min_measurements=1,
            input_logs=False,
            input_min_res=None,
            input_push_res=None,
            input_w=None,
            ambigator_fg_graph_file_id=None,
            ambigator_command_line=None,
            cutoff_lowres=None,
            cutoff_highres=None,
        )
        merge_result.mtz_file = orm.File(
            type="text/plain",
            file_name="test.txt",
            description="just a test",
            size_in_bytes=4,
            size_in_bytes_compressed=4,
            original_path="test.txt",
            sha256="",
            modified=_BEAMTIME_START,
            contents=b"rr_mtz_file",
        )
        pdb_file_refined = orm.File(
            type="text/plain",
            file_name="test.txt",
            description="just a test",
            size_in_bytes=4,
            size_in_bytes_compressed=4,
            original_path="test.txt",
            sha256="",
            modified=_BEAMTIME_START,
            contents=b"merge_result_pdb_file",
        )
        mtz_file_refined = orm.File(
            type="text/plain",
            file_name="test.txt",
            description="just a test",
            size_in_bytes=4,
            size_in_bytes_compressed=4,
            original_path="test.txt",
            sha256="",
            modified=_BEAMTIME_START,
            contents=b"rr_mtz_file",
        )
        import_from_session.add(pdb_file_refined)
        import_from_session.add(mtz_file_refined)
        await import_from_session.flush()

        rr = orm.RefinementResult(
            r_free=1.0,
            r_work=0.5,
            rms_bond_angle=90,
            rms_bond_length=10,
            pdb_file_id=pdb_file_refined.id,
            mtz_file_id=mtz_file_refined.id,
        )
        merge_result.refinement_results.append(rr)
        import_from_session.add(merge_result)

        merge_result.indexing_results.append(indexing_result_1)
        merge_result.shell_foms.append(
            orm.MergeResultShellFom(
                one_over_d_centre=1.0,
                nref=1,
                d_over_a=1.0,
                min_res=1.0,
                max_res=1.0,
                cc=1.0,
                ccstar=1.0,
                r_split=1.0,
                reflections_possible=1,
                completeness=1.0,
                measurements=1,
                redundancy=1.0,
                snr=1.0,
                mean_i=1.0,
            )
        )

        schedule = orm.BeamtimeSchedule(
            beamtime_id=original_beamtime.id,
            users="users",
            td_support="",
            comment="comment",
            shift="15:00-18:00",
            date="2026-07-31",
            chemicals=[chemical],
        )
        import_from_session.add(schedule)

        configuration = orm.UserConfiguration(
            beamtime_id=original_beamtime.id,
            created=_BEAMTIME_START,
            auto_pilot=False,
            use_online_crystfel=True,
            current_experiment_type_id=experiment_type.id,
            current_online_indexing_parameters_id=indexing_parameters.id,
        )
        import_from_session.add(configuration)

        await import_from_session.commit()
        return original_beamtime.id


async def test_import_db(tmp_path: Path) -> None:
    import_from_url = f"sqlite+aiosqlite:///{tmp_path}/from-db"
    import_to_url = f"sqlite+aiosqlite:///{tmp_path}/to-db"

    for url in (import_from_url, import_to_url):
        engine = create_async_engine(url)
        await migrate(engine)

    original_beamtime_id = await _create_test_db(
        import_from_url, _TEST_CRYSTFEL_STREAM, _TEST_CRYSTFEL_STREAM_2
    )

    async with (
        get_orm_sessionmaker_with_url(import_from_url)() as import_from_session,
        get_orm_sessionmaker_with_url(import_to_url)() as import_to_session,
    ):
        await import_db(
            original_beamtime_id,
            import_to_session,
            import_from_session,
            new_title=None,
            new_output_path=None,
        )
        await import_to_session.commit()

    async with get_orm_sessionmaker_with_url(import_to_url)() as import_into_session:
        for beamtime_copy in (
            await import_into_session.scalars(select(orm.Beamtime))
        ).all():
            assert beamtime_copy.title == "title"
            assert beamtime_copy.external_id == "1"
            assert beamtime_copy.proposal == "proposal"
            assert beamtime_copy.title == "title"
            assert beamtime_copy.comment == "comment"
            assert (await beamtime_copy.awaitable_attrs.start).replace(
                tzinfo=datetime.UTC
            ) == _BEAMTIME_START
            assert (await beamtime_copy.awaitable_attrs.end).replace(
                tzinfo=datetime.UTC
            ) == _BEAMTIME_END
            assert beamtime_copy.analysis_output_path == "path"

            assert len(list(await beamtime_copy.awaitable_attrs.attributi)) == 2

        for attributo in (
            await import_into_session.scalars(select(orm.Attributo))
        ).all():
            logger.info("comparing attributo name")
            assert attributo.name in ("1", "2")
            assert attributo.beamtime_id == 1

        chemicals = (
            await import_into_session.scalars(
                select(orm.Chemical).options(selectinload(orm.Chemical.files))
            )
        ).all()
        assert len(chemicals) == 1
        for chemical in chemicals:
            assert len(chemical.files) == 1

        for attributo in (
            await import_into_session.scalars(select(orm.Attributo))
        ).all():
            logger.info("comparing attributo name")
            assert attributo.name in ("1", "2")
            assert attributo.beamtime_id == 1
        geometries = list(
            (
                await import_into_session.scalars(
                    select(orm.Geometry).options(selectinload(orm.Geometry.attributi))
                )
            ).all()
        )
        assert len(geometries) == 1
        assert geometries[0].content == "foo bar baz {{1}} qux"
        assert len(geometries[0].attributi) == 1

        events = list((await import_into_session.scalars(select(orm.EventLog))).all())
        assert len(events) == 1
        assert len(await events[0].awaitable_attrs.files) == 1
        assert events[0].text == "hello world"

        mrs = list((await import_into_session.scalars(select(orm.MergeResult))).all())
        assert len(mrs) == 1
        assert len(await mrs[0].awaitable_attrs.indexing_results) == 1
        assert len(await mrs[0].awaitable_attrs.refinement_results) == 1
        assert len(await mrs[0].awaitable_attrs.shell_foms) == 1

        schedules = list(
            (await import_into_session.scalars(select(orm.BeamtimeSchedule))).all()
        )
        assert len(schedules) == 1
        assert len(await schedules[0].awaitable_attrs.chemicals) == 1

        configurations = list(
            (await import_into_session.scalars(select(orm.UserConfiguration))).all()
        )
        assert len(configurations) == 1
        assert configurations[0].current_experiment_type_id is not None
        assert (
            await configurations[
                0
            ].awaitable_attrs.current_online_indexing_parameters_id
            is not None
        )

        indexing_results = list(
            (await import_into_session.scalars(select(orm.IndexingResult))).all()
        )
        assert len(indexing_results) == 2
        assert len(await indexing_results[0].awaitable_attrs.align_detector_groups) == 1
        assert len(await indexing_results[0].awaitable_attrs.statistics) == 1
        assert len(await indexing_results[0].awaitable_attrs.template_replacements) == 1

        runs = list(
            (
                await import_into_session.scalars(
                    select(orm.Run).options(selectinload(orm.Run.files))
                )
            ).all()
        )
        assert len(runs) == 1
        assert len(runs[0].files) == 1
        assert runs[0].files[0].glob == "*.h5"


async def test_export_db_with_nonexistant_parent_dir(tmp_path: Path) -> None:
    original_db_url = f"sqlite+aiosqlite:///{tmp_path}/db"

    original_engine = create_async_engine(original_db_url)
    await migrate(original_engine)

    beamtime_id = await _create_test_db(
        original_db_url, _TEST_CRYSTFEL_STREAM, _TEST_CRYSTFEL_STREAM_2
    )
    zip_path = tmp_path / "nonexistant-directory" / "test.zip"

    async with get_orm_sessionmaker_with_url(original_db_url)() as session:
        job = orm.ExportJob(
            beamtime_id=beamtime_id,
            created=_BEAMTIME_START,
            size_in_mebibytes=0,
            output_path=str(zip_path),
            status_message="",
            contains_stream_files=True,
            started=None,
            stopped=None,
        )
        session.add(job)
        await session.commit()
        export_job_id = job.id

    with pytest.raises(Exception):
        await export_db(original_db_url, export_job_id)


async def test_export_db_with_stream_files(tmp_path: Path) -> None:
    original_db_url = f"sqlite+aiosqlite:///{tmp_path}/db"

    original_engine = create_async_engine(original_db_url)
    await migrate(original_engine)

    beamtime_id = await _create_test_db(
        original_db_url, _TEST_CRYSTFEL_STREAM, _TEST_CRYSTFEL_STREAM_2
    )
    zip_path = tmp_path / "test.zip"

    async with get_orm_sessionmaker_with_url(original_db_url)() as session:
        job = orm.ExportJob(
            beamtime_id=beamtime_id,
            created=_BEAMTIME_START,
            size_in_mebibytes=0,
            output_path=str(zip_path),
            status_message="",
            contains_stream_files=True,
            started=None,
            stopped=None,
        )
        session.add(job)
        await session.commit()
        export_job_id = job.id

    await export_db(original_db_url, export_job_id)

    assert await anyio.Path(zip_path).is_file()

    logger.info(
        f"Export complete, zip file {zip_path} was written. Now checking if the export job was updated."
    )
    async with get_orm_sessionmaker_with_url(original_db_url)() as session:
        logger.info("Reading DB.", export_from_url=original_db_url)
        export_job = await session.get(orm.ExportJob, export_job_id)
        assert export_job is not None
        assert export_job.started is not None
        assert export_job.stopped is not None
        assert export_job.status_message != ""
        assert export_job.size_in_mebibytes != 0

    new_db_url = f"sqlite+aiosqlite:///{tmp_path}/new-db"
    new_engine = create_async_engine(new_db_url)
    await migrate(new_engine)

    # Add this after adding the indexing results
    extracted_stream_file_dir = anyio.Path(tmp_path / "extracted-stream-files")
    await extracted_stream_file_dir.mkdir(parents=True)
    await import_db_from_zip_file(
        file_name=zip_path,
        stream_file_dir=extracted_stream_file_dir,
        import_to_db_connection_url=new_db_url,
    )

    assert await (extracted_stream_file_dir / _TEST_CRYSTFEL_STREAM.name).is_file()

    logger.info(
        "Importing the ZIP file completed, checking if the streams file we have in there have the right (new) path."
    )
    async with get_orm_sessionmaker_with_url(new_db_url)() as session:
        indexing_results = (await session.scalars(select(orm.IndexingResult))).all()
        assert {
            str(indexing_result.stream_file) for indexing_result in indexing_results
        } == {
            str(extracted_stream_file_dir / _TEST_CRYSTFEL_STREAM.name),
            str(extracted_stream_file_dir / "test-1.stream"),
        }


async def test_export_db_without_stream_files(tmp_path: Path) -> None:
    original_db_url = f"sqlite+aiosqlite:///{tmp_path}/db"

    original_engine = create_async_engine(original_db_url)
    await migrate(original_engine)

    beamtime_id = await _create_test_db(
        original_db_url, _TEST_CRYSTFEL_STREAM, _TEST_CRYSTFEL_STREAM_2
    )
    zip_path = tmp_path / "test.zip"

    async with get_orm_sessionmaker_with_url(original_db_url)() as session:
        job = orm.ExportJob(
            beamtime_id=beamtime_id,
            created=_BEAMTIME_START,
            size_in_mebibytes=0,
            output_path=str(zip_path),
            status_message="",
            contains_stream_files=False,
            started=None,
            stopped=None,
        )
        session.add(job)
        await session.commit()
        export_job_id = job.id

    await export_db(original_db_url, export_job_id)

    assert await anyio.Path(zip_path).is_file()

    new_db_url = f"sqlite+aiosqlite:///{tmp_path}/new-db"
    new_engine = create_async_engine(new_db_url)
    await migrate(new_engine)

    # Add this after adding the indexing results
    extracted_stream_file_dir = anyio.Path(tmp_path / "extracted-stream-files")
    await extracted_stream_file_dir.mkdir(parents=True)
    await import_db_from_zip_file(
        file_name=zip_path,
        stream_file_dir=extracted_stream_file_dir,
        import_to_db_connection_url=new_db_url,
    )

    assert not (
        await (extracted_stream_file_dir / _TEST_CRYSTFEL_STREAM.name).is_file()
    )


async def test_export_db_with_nonexistant_stream_files(tmp_path: Path) -> None:
    original_db_url = f"sqlite+aiosqlite:///{tmp_path}/db"

    original_engine = create_async_engine(original_db_url)
    await migrate(original_engine)

    beamtime_id = await _create_test_db(
        original_db_url,
        _TEST_CRYSTFEL_STREAM / "doesnotexist",
        _TEST_CRYSTFEL_STREAM_2 / "doesnotexist",
    )
    zip_path = tmp_path / "test.zip"

    async with get_orm_sessionmaker_with_url(original_db_url)() as session:
        job = orm.ExportJob(
            beamtime_id=beamtime_id,
            created=_BEAMTIME_START,
            size_in_mebibytes=0,
            output_path=str(zip_path),
            status_message="",
            contains_stream_files=True,
            started=None,
            stopped=None,
        )
        session.add(job)
        await session.commit()
        export_job_id = job.id

    await export_db(original_db_url, export_job_id)

    assert await anyio.Path(zip_path).is_file()

    logger.info(
        f"Export complete, zip file {zip_path} was written. Now checking if the export job was updated."
    )
    async with get_orm_sessionmaker_with_url(original_db_url)() as session:
        logger.info("Reading DB.", export_from_url=original_db_url)
        export_job = await session.get(orm.ExportJob, export_job_id)
        assert export_job is not None
        assert export_job.started is not None
        assert export_job.stopped is not None
        assert export_job.status_message != ""
        assert export_job.size_in_mebibytes != 0

    new_db_url = f"sqlite+aiosqlite:///{tmp_path}/new-db"
    new_engine = create_async_engine(new_db_url)
    await migrate(new_engine)

    # Add this after adding the indexing results
    extracted_stream_file_dir = anyio.Path(tmp_path / "extracted-stream-files")
    await extracted_stream_file_dir.mkdir(parents=True)
    await import_db_from_zip_file(
        file_name=zip_path,
        stream_file_dir=extracted_stream_file_dir,
        import_to_db_connection_url=new_db_url,
    )

    assert not await (extracted_stream_file_dir / _TEST_CRYSTFEL_STREAM.name).is_file()
