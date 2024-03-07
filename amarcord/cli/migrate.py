import argparse
import datetime
import itertools
import sys
from dataclasses import dataclass
from typing import Any
from typing import cast

from alembic import command
from alembic.config import Config
from sqlalchemy import MetaData
from sqlalchemy import create_engine
from sqlalchemy import insert
from sqlalchemy import select
from sqlalchemy.engine import URL
from sqlalchemy.engine import Connection

from amarcord.db import tables
from amarcord.db import tables_old
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.asyncdb import attributo_value_to_db_row
from amarcord.db.asyncdb import live_stream_image_name
from amarcord.db.attributi import datetime_from_attributo_int
from amarcord.db.attributi import schema_union_to_attributo_type
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.dbattributo import DBAttributo

_DEBUG_LOG = False
_SLOW_MIGRATION = True


def debug_print(s: str) -> None:
    if _DEBUG_LOG:
        print(s)


def migrate_attributi(
    attributi: dict[str, Any],
    chemical_id_conversion: dict[None | int, int],
    attributo_name_to_id: dict[str, int],
    chemical_attributo_ids: set[int],
) -> dict[str, Any]:
    debug_print(f"run attributi {attributi}")

    def nonchemical_or_existing_chemical(name: str, value: Any) -> bool:
        aid = attributo_name_to_id[name]
        return aid not in chemical_attributo_ids or (value != 0 and value is not None)

    def migrate_single_attributo(name: str, value: Any) -> Any:
        aid = attributo_name_to_id[name]
        if aid in chemical_attributo_ids:
            return chemical_id_conversion[value]
        return value

    return {
        str(attributo_name_to_id[name]): migrate_single_attributo(name, value)
        for name, value in attributi.items()
        if nonchemical_or_existing_chemical(name, value)
    }


@dataclass(frozen=True)
class _Beamtime:
    external_id: str
    proposal: str
    beamline: str
    title: str
    comment: str
    start: datetime.datetime
    end: datetime.datetime
    url: URL


def main_inner(
    delete_old: bool,
    bt: _Beamtime,
    old_conn: Connection,
    old_tables: tables_old.DBTables,
    new_conn: Connection,
    new_tables: tables.DBTables,
) -> None:
    if delete_old:
        print("removing old entries from table")
        new_conn.execute(new_tables.refinement_result.delete())
        new_conn.execute(new_tables.merge_result_shell_fom.delete())
        new_conn.execute(new_tables.merge_result_has_indexing_result.delete())
        new_conn.execute(new_tables.merge_result.delete())
        new_conn.execute(new_tables.indexing_result_has_statistic.delete())
        new_conn.execute(new_tables.indexing_result.delete())
        new_conn.execute(new_tables.run.delete())
        new_conn.execute(new_tables.event_has_file.delete())
        new_conn.execute(new_tables.event_log.delete())
        new_conn.execute(new_tables.beamtime_schedule_has_chemical.delete())
        new_conn.execute(new_tables.chemical_has_file.delete())
        new_conn.execute(new_tables.chemical.delete())
        new_conn.execute(new_tables.data_set.delete())
        new_conn.execute(new_tables.experiment_has_attributo.delete())
        new_conn.execute(new_tables.experiment_type.delete())
        new_conn.execute(new_tables.attributo.delete())
        new_conn.execute(new_tables.file.delete())
        new_conn.execute(new_tables.beamtime_schedule.delete())
        new_conn.execute(new_tables.beamtime.delete())

    beamtime_id = new_conn.execute(
        insert(new_tables.beamtime).values(
            external_id=bt.external_id,
            proposal=bt.proposal,
            beamline=bt.beamline,
            title=bt.title,
            comment=bt.comment,
            start=bt.start,
            end=bt.end,
        )
    ).lastrowid

    print(f"New beamtime has ID {beamtime_id}")

    old_to_new_experiment_type_id: dict[int, int] = {}
    for experiment_type in old_conn.execute(
        select(old_tables.experiment_type.c.id, old_tables.experiment_type.c.name)
    ):
        old_to_new_experiment_type_id[experiment_type.id] = new_conn.execute(
            insert(new_tables.experiment_type).values(
                beamtime_id=beamtime_id, name=experiment_type.name
            )
        ).lastrowid

    debug_print("experiment type IDs:\n")
    for old_id, new_id in old_to_new_experiment_type_id.items():
        debug_print(f"old {old_id} => new {new_id}")

    attributo_name_to_id: dict[str, int] = {}
    attributo_types: list[DBAttributo] = []
    chemical_attributo_ids: set[int] = set()
    for attributo in old_conn.execute(select(old_tables.attributo)):
        if (
            attributo.name in ("started", "stopped")
            and attributo.associated_table == AssociatedTable.RUN
        ):
            print(f"skipping run attributo {attributo.name}")
            continue
        print(
            f"inserting {attributo.associated_table.value} attribute {attributo.name}"
        )
        aid = new_conn.execute(
            insert(new_tables.attributo).values(
                beamtime_id=beamtime_id,
                name=attributo.name,
                description=attributo.description,
                group=attributo.group,
                associated_table=attributo.associated_table,
                json_schema=attributo.json_schema,
            )
        ).lastrowid
        attributo_name_to_id[attributo.name] = aid
        if (
            "format" in attributo.json_schema
            and attributo.json_schema["format"] == "chemical-id"
        ):
            chemical_attributo_ids.add(aid)
        attributo_types.append(
            DBAttributo(
                id=AttributoId(aid),
                beamtime_id=BeamtimeId(beamtime_id),
                name=attributo.name,
                description="",
                group="",
                associated_table=attributo.associated_table,
                attributo_type=schema_union_to_attributo_type(attributo.json_schema),
            )
        )

    debug_print("attributo IDs:\n")
    for name, aid in attributo_name_to_id.items():
        debug_print(f"{name} => new {aid}")

    old_to_new_file_id: dict[int, int] = {}
    for f in old_conn.execute(select(old_tables.file)):
        # We used to treat "live-stream-image" as *the* live stream
        # image for the whole database. Now that we have more than one
        # beamtime in the DB, we have to adapt the name at least.
        new_file_name = (
            live_stream_image_name(beamtime_id)
            if f.file_name == "live-stream-image"
            else f.file_name
        )
        old_to_new_file_id[f.id] = new_conn.execute(
            insert(new_tables.file).values(
                type=f.type,
                file_name=new_file_name,
                size_in_bytes=f.size_in_bytes,
                original_path=f.original_path,
                sha256=f.sha256,
                modified=f.modified,
                contents=b"" if not _SLOW_MIGRATION else f.contents,
                description=f.description,
            )
        ).lastrowid

    debug_print("file IDs:\n")
    for old_id, new_id in old_to_new_file_id.items():
        debug_print(f"old {old_id} => new {new_id}")

    old_to_new_beamtime_schedule_id: dict[int, int] = {}
    for bts in old_conn.execute(select(old_tables.beamtime_schedule)):
        old_to_new_beamtime_schedule_id[bts.id] = new_conn.execute(
            insert(new_tables.beamtime_schedule).values(
                beamtime_id=beamtime_id,
                users=bts.users,
                td_support=bts.td_support,
                comment=bts.comment,
                shift=bts.shift,
                date=bts.date,
            )
        ).lastrowid

    debug_print("beamtime schedule IDs:\n")
    for old_id, new_id in old_to_new_beamtime_schedule_id.items():
        debug_print(f"old {old_id} => new {new_id}")

    debug_print("=> inserting experiment has attributo rows")
    for eha in old_conn.execute(select(old_tables.experiment_has_attributo)):
        new_conn.execute(
            insert(new_tables.experiment_has_attributo).values(
                experiment_type_id=old_to_new_experiment_type_id[
                    eha.experiment_type_id
                ],
                attributo_id=attributo_name_to_id[eha.attributo_name],
                chemical_role=eha.chemical_role,
            )
        )
    debug_print("<= inserting experiment has attributo rows done")

    # Special ID 0 stays at 0, and we also have "None" in older DBs
    # old_to_new_chemical_id: dict[None | int, int] = {0: 0, None: 0}
    old_to_new_chemical_id: dict[None | int, int] = {}
    for f in old_conn.execute(select(old_tables.chemical)):
        new_chemical_attributi = {
            str(attributo_name_to_id[name]): value
            for name, value in f.attributi.items()
        }
        new_chemical_id = new_conn.execute(
            insert(new_tables.chemical).values(
                name=f.name,
                beamtime_id=beamtime_id,
                responsible_person=f.responsible_person,
                modified=f.modified,
                type=f.type,
            )
        ).lastrowid
        old_to_new_chemical_id[f.id] = new_chemical_id
        amap = AttributiMap.from_types_and_json_dict(
            types=attributo_types, json_dict=new_chemical_attributi
        )
        for aid, v in amap.items():
            if v is None:
                continue
            new_conn.execute(
                insert(new_tables.chemical_has_attributo_value).values(
                    {
                        "attributo_id": aid,
                        "chemical_id": new_chemical_id,
                    }
                    | attributo_value_to_db_row(
                        v, cast(DBAttributo, amap.retrieve_type(aid)).attributo_type
                    )
                )
            )

    debug_print("chemical IDs:\n")
    for old_id, new_id in old_to_new_chemical_id.items():
        debug_print(f"old {old_id} => new {new_id}")

    old_to_new_data_set_id: dict[int, int] = {}
    for f in old_conn.execute(select(old_tables.data_set)):
        new_data_set_id = new_conn.execute(
            insert(new_tables.data_set).values(
                experiment_type_id=old_to_new_experiment_type_id[f.experiment_type_id],
            )
        ).lastrowid
        new_attributi_json = migrate_attributi(
            f.attributi,
            old_to_new_chemical_id,
            attributo_name_to_id,
            chemical_attributo_ids,
        )
        old_to_new_data_set_id[f.id] = new_data_set_id
        amap = AttributiMap.from_types_and_json_dict(
            types=attributo_types,
            json_dict=new_attributi_json,
        )
        for aid, v in amap.items():
            if v is None:
                continue
            new_conn.execute(
                insert(new_tables.data_set_has_attributo_value).values(
                    {
                        "attributo_id": aid,
                        "data_set_id": new_data_set_id,
                    }
                    | attributo_value_to_db_row(
                        v, cast(DBAttributo, amap.retrieve_type(aid)).attributo_type
                    )
                )
            )

    debug_print("data set IDs:\n")
    for old_id, new_id in old_to_new_data_set_id.items():
        debug_print(f"old {old_id} => new {new_id}")

    debug_print("=> inserting chemical has file rows")
    for eha in old_conn.execute(select(old_tables.chemical_has_file)):
        new_conn.execute(
            insert(new_tables.chemical_has_file).values(
                chemical_id=old_to_new_chemical_id[eha.chemical_id],
                file_id=old_to_new_file_id[eha.file_id],
            )
        )
    debug_print("<= inserting chemical has file rows done")

    debug_print("=> inserting beamtime schedule has chemical rows")
    for eha in old_conn.execute(select(old_tables.beamtime_schedule_has_chemical)):
        new_conn.execute(
            insert(new_tables.beamtime_schedule_has_chemical).values(
                beamtime_schedule_id=old_to_new_beamtime_schedule_id[
                    eha.beamtime_schedule_id
                ],
                chemical_id=old_to_new_chemical_id[eha.chemical_id],
            )
        )
    debug_print("<= inserting beamtime schedule has chemical rows done")

    old_to_new_run_id: dict[int, int] = {}
    for f in old_conn.execute(select(old_tables.run)):
        debug_print(f"migrating run id {f.id}")
        new_run_attributi_json = migrate_attributi(
            {
                key: value
                for key, value in f.attributi.items()
                if key not in ("started", "stopped")
            },
            old_to_new_chemical_id,
            attributo_name_to_id,
            chemical_attributo_ids,
        )
        new_run_id = new_conn.execute(
            insert(new_tables.run).values(
                external_id=f.id,
                beamtime_id=beamtime_id,
                modified=f.modified,
                experiment_type_id=old_to_new_experiment_type_id[f.experiment_type_id],
                started=datetime_from_attributo_int(f.attributi["started"]),
                stopped=(
                    datetime_from_attributo_int(f.attributi["stopped"])
                    if "stopped" in f.attributi
                    else None
                ),
            )
        ).lastrowid
        old_to_new_run_id[f.id] = new_run_id
        amap = AttributiMap.from_types_and_json_dict(
            types=attributo_types,
            json_dict=new_run_attributi_json,
        )
        for aid, v in amap.items():
            if v is None:
                continue
            new_conn.execute(
                insert(new_tables.run_has_attributo_value).values(
                    {
                        "attributo_id": aid,
                        "run_id": new_run_id,
                    }
                    | attributo_value_to_db_row(
                        v, cast(DBAttributo, amap.retrieve_type(aid)).attributo_type
                    )
                )
            )

    debug_print("run IDs:\n")
    for old_id, new_id in old_to_new_run_id.items():
        debug_print(f"old {old_id} => new {new_id}")

    old_to_new_event_id: dict[int, int] = {}
    for f in old_conn.execute(select(old_tables.event_log)):
        old_to_new_event_id[f.id] = new_conn.execute(
            insert(new_tables.event_log).values(
                beamtime_id=beamtime_id,
                created=f.created,
                level="USER",
                source=f.source,
                text=f.text,
            )
        ).lastrowid

    debug_print("event IDs:\n")
    for old_id, new_id in old_to_new_event_id.items():
        debug_print(f"old {old_id} => new {new_id}")

    old_to_new_indexing_result_id: dict[int, int] = {}
    for f in old_conn.execute(select(old_tables.indexing_result)):
        old_to_new_indexing_result_id[f.id] = new_conn.execute(
            insert(new_tables.indexing_result).values(
                created=f.created,
                run_id=old_to_new_run_id[f.run_id],
                stream_file=f.stream_file,
                cell_description=f.cell_description,
                point_group=f.point_group,
                chemical_id=old_to_new_chemical_id[f.chemical_id],
                frames=f.frames,
                hits=f.hits,
                not_indexed_frames=f.not_indexed_frames,
                hit_rate=f.hit_rate,
                indexing_rate=f.indexing_rate,
                indexed_frames=f.indexed_frames,
                detector_shift_x_mm=f.detector_shift_x_mm,
                detector_shift_y_mm=f.detector_shift_y_mm,
                job_id=f.job_id,
                job_status=f.job_status,
                job_error=f.job_error,
            )
        ).lastrowid

    debug_print("indexing result IDs:\n")
    for old_id, new_id in old_to_new_indexing_result_id.items():
        debug_print(f"old {old_id} => new {new_id}")

    debug_print("=> inserting event has file rows")
    for eha in old_conn.execute(select(old_tables.event_has_file)):
        new_conn.execute(
            insert(new_tables.event_has_file).values(
                event_id=old_to_new_event_id[eha.event_id],
                file_id=old_to_new_file_id[eha.file_id],
            )
        )
    debug_print("<= inserting event has file rows done")

    debug_print("=> inserting indexing result statistics rows")
    if _SLOW_MIGRATION:
        for eha in old_conn.execute(select(old_tables.indexing_result_has_statistic)):
            new_conn.execute(
                insert(new_tables.indexing_result_has_statistic).values(
                    indexing_result_id=old_to_new_indexing_result_id[
                        eha.indexing_result_id
                    ],
                    time=eha.time,
                    frames=eha.frames,
                    hits=eha.hits,
                    indexed_frames=eha.indexed_frames,
                    indexed_crystals=eha.indexed_crystals,
                )
            )
    debug_print("<= inserting indexing result statistics done")

    old_to_new_merge_result_id: dict[int, int] = {}
    for f in old_conn.execute(select(old_tables.merge_result)):
        debug_print(f"merge result: {f}")
        old_to_new_merge_result_id[f.id] = new_conn.execute(
            insert(new_tables.merge_result).values(
                created=f.created,
                recent_log=f.recent_log,
                negative_handling=f.negative_handling,
                job_status=f.job_status,
                started=f.started,
                stopped=f.stopped,
                point_group=f.point_group,
                cell_description=f.cell_description,
                job_id=f.job_id,
                job_error=f.job_error,
                mtz_file_id=(
                    old_to_new_file_id[f.mtz_file_id]
                    if f.mtz_file_id is not None
                    else None
                ),
                input_merge_model=f.input_merge_model,
                input_scale_intensities=f.input_scale_intensities,
                input_post_refinement=f.input_post_refinement,
                input_iterations=f.input_iterations,
                input_polarisation_angle=f.input_polarisation_angle,
                input_polarisation_percent=f.input_polarisation_percent,
                input_start_after=f.input_start_after,
                input_stop_after=f.input_stop_after,
                input_rel_b=f.input_rel_b,
                input_no_pr=f.input_no_pr,
                input_force_bandwidth=f.input_force_bandwidth,
                input_force_radius=f.input_force_radius,
                input_force_lambda=f.input_force_lambda,
                input_no_delta_cc_half=f.input_no_delta_cc_half,
                input_max_adu=f.input_max_adu,
                input_min_measurements=f.input_min_measurements,
                input_logs=f.input_logs,
                input_min_res=f.input_min_res,
                input_push_res=f.input_push_res,
                input_w=f.input_w,
                fom_snr=f.fom_snr,
                fom_wilson=f.fom_wilson,
                fom_ln_k=f.fom_ln_k,
                fom_discarded_reflections=f.fom_discarded_reflections,
                fom_one_over_d_from=f.fom_one_over_d_from,
                fom_one_over_d_to=f.fom_one_over_d_to,
                fom_redundancy=f.fom_redundancy,
                fom_completeness=f.fom_completeness,
                fom_measurements_total=f.fom_measurements_total,
                fom_reflections_total=f.fom_reflections_total,
                fom_reflections_possible=f.fom_reflections_possible,
                fom_r_split=f.fom_r_split,
                fom_r1i=f.fom_r1i,
                fom_2=f.fom_2,
                fom_cc=f.fom_cc,
                fom_ccstar=f.fom_ccstar,
                fom_ccano=f.fom_ccano,
                fom_crdano=f.fom_crdano,
                fom_rano=f.fom_rano,
                fom_rano_over_r_split=f.fom_rano_over_r_split,
                fom_d1sig=f.fom_d1sig,
                fom_d2sig=f.fom_d2sig,
                fom_outer_resolution=f.fom_outer_resolution,
                fom_outer_ccstar=f.fom_outer_ccstar,
                fom_outer_r_split=f.fom_outer_r_split,
                fom_outer_cc=f.fom_outer_cc,
                fom_outer_unique_reflections=f.fom_outer_unique_reflections,
                fom_outer_completeness=f.fom_outer_completeness,
                fom_outer_redundancy=f.fom_outer_redundancy,
                fom_outer_snr=f.fom_outer_snr,
                fom_outer_min_res=f.fom_outer_min_res,
                fom_outer_max_res=f.fom_outer_max_res,
            )
        ).lastrowid

    debug_print("merge result IDs:\n")
    for old_id, new_id in old_to_new_merge_result_id.items():
        debug_print(f"old {old_id} => new {new_id}")

    debug_print("=> inserting merge result shell fom rows")
    for eha in old_conn.execute(select(old_tables.merge_result_shell_fom)):
        new_conn.execute(
            insert(new_tables.merge_result_shell_fom).values(
                merge_result_id=old_to_new_merge_result_id[eha.merge_result_id],
                one_over_d_centre=eha.one_over_d_centre,
                nref=eha.nref,
                d_over_a=eha.d_over_a,
                min_res=eha.min_res,
                max_res=eha.max_res,
                cc=eha.cc,
                ccstar=eha.ccstar,
                r_split=eha.r_split,
                reflections_possible=eha.reflections_possible,
                completeness=eha.completeness,
                measurements=eha.measurements,
                redundancy=eha.redundancy,
                snr=eha.snr,
                mean_i=eha.mean_i,
            )
        )
    debug_print("<= inserting merge result shell fom done")

    debug_print("=> inserting merge result has indexing result rows")
    for eha in old_conn.execute(select(old_tables.merge_result_has_indexing_result)):
        new_conn.execute(
            insert(new_tables.merge_result_has_indexing_result).values(
                merge_result_id=old_to_new_merge_result_id[eha.merge_result_id],
                indexing_result_id=old_to_new_indexing_result_id[
                    eha.indexing_result_id
                ],
            )
        )
    debug_print("<= inserting merge result has indexing result done")

    debug_print("=> inserting refinement result rows")
    for eha in old_conn.execute(select(old_tables.refinement_result)):
        new_conn.execute(
            insert(new_tables.refinement_result).values(
                merge_result_id=old_to_new_merge_result_id[eha.merge_result_id],
                pdb_file_id=old_to_new_file_id[eha.pdb_file_id],
                mtz_file_id=old_to_new_file_id[eha.mtz_file_id],
                r_free=eha.r_free,
                r_work=eha.r_work,
                rms_bond_angle=eha.rms_bond_angle,
                rms_bond_length=eha.rms_bond_length,
            )
        )
    debug_print("<= inserting refinement results done")


def upgrade_to_connection(
    conn: Any, version: str, additional_args: dict[str, str] | None = None
) -> None:
    alembic_cfg = Config()
    # see https://alembic.sqlalchemy.org/en/latest/cookbook.html#programmatic-api-use-connection-sharing-with-asyncio
    # pylint: disable=unsupported-assignment-operation
    alembic_cfg.attributes["connection"] = conn
    alembic_cfg.set_main_option("script_location", "amarcord:db/migrations")
    argparser = argparse.ArgumentParser()
    argparser.add_argument("-x", action="append")
    # Horribly unsafe and badly written, but please somebody show me how to do this right? How do I
    # pass -x parameters programmatically to alembic?
    alembic_cfg.cmd_opts = argparser.parse_args(
        list(
            itertools.chain.from_iterable(
                [["-x", f"{key}={value}"] for key, value in additional_args.items()]
            )
        )
        if additional_args is not None
        else []
    )
    command.upgrade(alembic_cfg, version)


def upgrade_to_head_connection(
    connection: Any, additional_args: dict[str, str] | None = None
) -> None:
    upgrade_to_connection(connection, "head", additional_args)


_MYSQL_DRIVER_NAME = "mysql+pymysql"


def _create_vm05_db_url(db_name: str, main_db_pw: str) -> URL:
    return URL.create(
        drivername=_MYSQL_DRIVER_NAME,
        username="tapedrive_user",
        password=main_db_pw,
        host="cfeld-vm05",
        port=3306,
        database=db_name,
    )


def _create_local_db_url(db_name: str) -> URL:
    return URL.create(
        drivername=_MYSQL_DRIVER_NAME,
        username="root",
        password="password",
        host="localhost",
        port=3307,
        database=db_name,
    )


def old_beamtimes() -> list[_Beamtime]:
    return [
        _Beamtime(
            external_id="11014381",
            proposal="II-20200001",
            beamline="P11",
            title="Alessa's Birthday Beamtime 🥳",
            comment="Title: Developing serial synchrotron crystallography towards a multidimensional crystallography approach for structural dynamics of biological macromolecules",
            start=datetime.datetime(2022, 4, 4, 9, 0, 0),
            end=datetime.datetime(2022, 4, 10, 9, 0, 0),
            url=_create_local_db_url("tapedrive_2022_04"),
        ),
        _Beamtime(
            external_id="11014380",
            proposal="II-20200001",
            beamline="P11",
            title="May the fourth beam with you",
            comment="Title: Developing serial synchrotron crystallography towards a multidimensional crystallography approach for structural dynamics of biological macromolecules",
            start=datetime.datetime(2022, 5, 5, 9, 0, 0),
            end=datetime.datetime(2022, 5, 10, 9, 0, 0),
            url=_create_local_db_url("tapedrive_2022_05"),
        ),
        _Beamtime(
            external_id="11014376",
            proposal="II-20200001",
            beamline="P11",
            title="Last beamtime before summer 🏖️🌞",
            comment="Title: Developing serial synchrotron crystallography towards a multidimensional crystallography approach for structural dynamics of biological macromolecules",
            start=datetime.datetime(2022, 7, 7, 9, 0, 0),
            end=datetime.datetime(2022, 7, 11, 9, 0, 0),
            url=_create_local_db_url("tapedrive_2022_07"),
        ),
        _Beamtime(
            external_id="11015430",
            proposal="I-20220495",
            beamline="P11",
            title="Wake me up when the beamtime ends 🤘🏼🎸",
            comment="Title: Combining pH, temperature and time resolved studies for multidimensional SSX to investigate structure and dynamics of SARS-CoV-2 main protease inhibition.\n\nFirst beam time with CrystFEL online.",
            start=datetime.datetime(2022, 9, 23, 9, 0, 0),
            end=datetime.datetime(2022, 9, 28, 9, 0, 0),
            url=_create_local_db_url("tapedrive_2022_09"),
        ),
        _Beamtime(
            external_id="11015490",
            proposal="I-20220495",
            beamline="P11",
            title="Twinkle, twinkle, CC* 🌟",
            comment="Title: Combining pH, temperature and time resolved studies for multidimensional SSX to investigate structure and dynamics of SARS-CoV-2 main protease inhibition.",
            start=datetime.datetime(2022, 11, 4, 9, 0, 0),
            end=datetime.datetime(2022, 11, 9, 9, 0, 0),
            url=_create_local_db_url("tapedrive_2022_11"),
        ),
        _Beamtime(
            external_id="11016853",
            proposal="BAG-20220737",
            beamline="P11",
            title="BAG to the Future ⏰🚗⚡️",
            comment="Title: Multi-dimensional serial synchrotron crystallography to unravel protein dynamics in various sample environments\n\nIn AMARCORD, this beamtime also contains the cancelled April 2023 beam time (run time April 13th to April 17th; it has no runs, and no discernible P11 beamtime ID, though).\n\nAlso, this beam time has two beam time IDs. The main one is mentioned here, the other one is 11016855 and is just for testing.",
            start=datetime.datetime(2023, 6, 1, 9, 0, 0),
            end=datetime.datetime(2023, 6, 5, 15, 0, 0),
            url=_create_local_db_url("tapedrive_2023_04"),
        ),
        _Beamtime(
            external_id="11016848",
            proposal="BAG-20220737",
            beamline="P11",
            title="Bragg'n BAG",
            comment="Title: Multi-dimensional serial synchrotron crystallography to unravel protein dynamics in various sample environments",
            start=datetime.datetime(2023, 9, 20, 9, 0, 0),
            end=datetime.datetime(2023, 9, 26, 9, 0, 0),
            url=_create_local_db_url("tapedrive_2023_09"),
        ),
        _Beamtime(
            external_id="",
            proposal="BAG-20220737",
            beamline="P11",
            title="Jingle BAGs🎄🔔",
            comment="Cancelled due to chopper issue.",
            start=datetime.datetime(2023, 11, 16, 9, 0, 0),
            end=datetime.datetime(2023, 11, 20, 20, 0, 0),
            url=_create_local_db_url("tapedrive_2023_11"),
        ),
        _Beamtime(
            external_id="",
            proposal="BAG-20220737",
            beamline="P11",
            title="BAG to Work 🫵🏼💪🏼",
            comment="",
            start=datetime.datetime(2024, 3, 7, 17, 0, 0),
            end=datetime.datetime(2023, 3, 11, 9, 0, 0),
            url=_create_local_db_url("tapedrive_2024_03"),
        ),
    ]


def new_url(main_db_pw: str) -> URL:
    return _create_vm05_db_url("tapedrive", main_db_pw)


def main(main_db_pw: str) -> None:
    new_engine = create_engine(
        new_url(main_db_pw),
        echo=False,
    )

    old_tables = tables_old.create_tables_from_metadata(MetaData())
    new_metadata = MetaData()
    new_tables = tables.create_tables_from_metadata(new_metadata)

    first_migration = True
    with new_engine.connect() as new_conn:
        for bt in old_beamtimes():
            print(f"migrating {bt}")
            old_engine = create_engine(bt.url)

            with old_engine.connect() as old_conn:
                upgrade_to_head_connection(new_conn)
                if first_migration:
                    print("deleting old entries")
                main_inner(
                    first_migration, bt, old_conn, old_tables, new_conn, new_tables
                )

            first_migration = False
            new_conn.commit()


if __name__ == "__main__":
    main(sys.argv[1])
