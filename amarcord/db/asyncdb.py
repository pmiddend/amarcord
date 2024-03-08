import datetime
import hashlib
import itertools
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Iterable
from typing import cast

import numpy as np
import sqlalchemy as sa
import structlog.stdlib
from pint import UnitRegistry
from sqlalchemy.sql import select

from amarcord import magic
from amarcord.amici.crystfel.util import CrystFELCellFile
from amarcord.amici.crystfel.util import coparse_cell_description
from amarcord.amici.crystfel.util import parse_cell_description
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.async_dbcontext import Connection
from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import convert_attributo_value
from amarcord.db.attributi import schema_json_to_attributo_type
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_name_and_role import AttributoIdAndRole
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
from amarcord.db.data_set import DBDataSet
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.db_merge_result import DBMergeResultInput
from amarcord.db.db_merge_result import DBMergeResultOutput
from amarcord.db.db_merge_result import DBMergeRuntimeStatus
from amarcord.db.db_merge_result import DBMergeRuntimeStatusDone
from amarcord.db.db_merge_result import DBMergeRuntimeStatusError
from amarcord.db.db_merge_result import DBMergeRuntimeStatusRunning
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.experiment_type import DBExperimentType
from amarcord.db.indexing_result import DBIndexingFOM
from amarcord.db.indexing_result import DBIndexingResultDone
from amarcord.db.indexing_result import DBIndexingResultInput
from amarcord.db.indexing_result import DBIndexingResultOutput
from amarcord.db.indexing_result import DBIndexingResultRunning
from amarcord.db.indexing_result import DBIndexingResultRuntimeStatus
from amarcord.db.indexing_result import DBIndexingResultStatistic
from amarcord.db.merge_model import MergeModel
from amarcord.db.merge_parameters import DBMergeParameters
from amarcord.db.merge_result import MergeResult
from amarcord.db.merge_result import MergeResultFom
from amarcord.db.merge_result import MergeResultOuterShell
from amarcord.db.merge_result import MergeResultShell
from amarcord.db.merge_result import RefinementResult
from amarcord.db.migrations.alembic_utilities import upgrade_to_head_connection
from amarcord.db.polarisation import Polarisation
from amarcord.db.refinement_result import DBRefinementResultOutput
from amarcord.db.run_external_id import RunExternalId
from amarcord.db.run_internal_id import RunInternalId
from amarcord.db.scale_intensities import ScaleIntensities
from amarcord.db.schedule_entry import BeamtimeScheduleEntry
from amarcord.db.table_classes import BeamtimeInput
from amarcord.db.table_classes import BeamtimeOutput
from amarcord.db.table_classes import DBChemical
from amarcord.db.table_classes import DBEvent
from amarcord.db.table_classes import DBFile
from amarcord.db.table_classes import DBRunOutput
from amarcord.db.tables import DBTables
from amarcord.db.user_configuration import UserConfiguration
from amarcord.db.user_configuration import initial_user_configuration
from amarcord.pint_util import valid_pint_unit
from amarcord.util import group_by
from amarcord.util import sha256_file

_UNIT_REGISTRY = UnitRegistry()


class NotFoundError(Exception):
    """
    Sometimes, we want to raise an exception because some DB row or something is missing. We could return None, but the cases are usually
    exceedingly rare, so an exception seems nice.

    This way, we can catch those types of errors and return a 404 in the web server, for example (saving the 500 requests for "real" server
    errors)
    """

    def __init__(self, missing_id: int) -> None:
        self.missing_id = missing_id


def attributo_value_for_db_row(row: Any) -> AttributoValue:
    """
    Convert a DB row containing all necessary columns to describe an attributo value (so the columns for the different attributo types (int, string)) into an actual value.

    In essence, this resolves tuple[None | int, None | str, ...] to int | str | ...
    """
    if row.integer_value is not None:
        return row.integer_value
    if row.float_value is not None:
        return row.float_value
    if row.string_value is not None:
        return row.string_value
    if row.bool_value is not None:
        return row.bool_value
    if row.datetime_value is not None:
        return row.datetime_value
    if row.chemical_value is not None:
        return row.chemical_value
    assert row.list_value, f"DB row has no value column non-None: {row}"
    return row.list_value


def attributo_value_to_db_row(a: AttributoValue, type_: AttributoType) -> Any:
    """
    Convert an attributo value to a dictionary with the corresponding database column. This is essentially the opposite of the function above this.

    It needs the type of the attributo, because from the value alone it cannot determine which column to use. The number '3' could be int or float, for example.
    """
    assert a is not None
    if isinstance(type_, AttributoTypeInt):
        return {"integer_value": a}
    if isinstance(type_, AttributoTypeBoolean):
        return {"bool_value": a}
    if isinstance(type_, AttributoTypeString):
        return {"string_value": a}
    if isinstance(type_, AttributoTypeChemical):
        return {"chemical_value": a}
    if isinstance(type_, AttributoTypeChoice):
        return {"string_value": a}
    if isinstance(type_, AttributoTypeDecimal):
        # pyright cannot assume any type information here
        if np.isnan(a):  # type: ignore
            raise Exception("NaN values not allowed in attributo!")
        return {"float_value": a}
    if isinstance(type_, AttributoTypeDateTime):
        return {"datetime_value": a}
    assert isinstance(type_, AttributoTypeList)
    return {"list_value": a}


def live_stream_image_name(beamtime_id: int) -> str:
    return f"live-stream-image-{beamtime_id}"


logger = structlog.stdlib.get_logger(__name__)


class MergeResultError(str):
    pass


@dataclass(frozen=True)
class CreateFileResult:
    id: int
    type_: str
    size_in_bytes: int


ATTRIBUTO_GROUP_MANUAL = "manual"


def _evaluate_indexing_result_runtime_status(
    db_row: Any,
) -> DBIndexingResultRuntimeStatus:
    return (
        DBIndexingResultDone(
            stream_file=Path(db_row.stream_file),
            job_error=db_row.job_error,
            fom=DBIndexingFOM(
                db_row.hit_rate,
                db_row.indexing_rate,
                db_row.indexed_frames,
                db_row.detector_shift_x_mm,
                db_row.detector_shift_y_mm,
            ),
        )
        if db_row.job_status == DBJobStatus.DONE
        else DBIndexingResultRunning(
            stream_file=Path(db_row.stream_file),
            job_id=db_row.job_id,
            fom=DBIndexingFOM(
                db_row.hit_rate,
                db_row.indexing_rate,
                db_row.indexed_frames,
                db_row.detector_shift_x_mm,
                db_row.detector_shift_y_mm,
            ),
        )
        if db_row.job_status == DBJobStatus.RUNNING
        else None
    )


def _values_from_shell_foms(
    mrd: MergeResultShell, merge_result_id: int
) -> dict[str, float | int]:
    return {
        "merge_result_id": merge_result_id,
        "one_over_d_centre": mrd.one_over_d_centre,
        "nref": mrd.nref,
        "d_over_a": mrd.d_over_a,
        "min_res": mrd.min_res,
        "max_res": mrd.max_res,
        "cc": mrd.cc,
        "ccstar": mrd.ccstar,
        "r_split": mrd.r_split,
        "reflections_possible": mrd.reflections_possible,
        "completeness": mrd.completeness,
        "measurements": mrd.measurements,
        "redundancy": mrd.redundancy,
        "snr": mrd.snr,
        "mean_i": mrd.mean_i,
    }


def _runtime_status_sql_values(rs: DBMergeRuntimeStatus) -> dict[str, Any]:
    match rs:
        case None:
            return {"recent_log": "", "job_status": DBJobStatus.QUEUED}
        case DBMergeRuntimeStatusRunning(job_id, started, recent_log):
            return {
                "started": started,
                "job_id": job_id,
                "recent_log": recent_log,
                "job_status": DBJobStatus.RUNNING,
            }
        case DBMergeRuntimeStatusError(error, started, stopped, recent_log):
            return {
                "job_error": error,
                "started": started,
                "stopped": stopped,
                "recent_log": recent_log,
                "job_status": DBJobStatus.DONE,
            }
        case DBMergeRuntimeStatusDone(started, stopped, result, recent_log):
            return {
                "job_status": DBJobStatus.DONE,
                "recent_log": recent_log,
                "started": started,
                "stopped": stopped,
                "mtz_file_id": result.mtz_file_id,
                "fom_snr": result.fom.snr,
                "fom_wilson": result.fom.wilson,
                "fom_ln_k": result.fom.ln_k,
                "fom_discarded_reflections": result.fom.discarded_reflections,
                "fom_one_over_d_from": result.fom.one_over_d_from,
                "fom_one_over_d_to": result.fom.one_over_d_to,
                "fom_redundancy": result.fom.redundancy,
                "fom_completeness": result.fom.completeness,
                "fom_measurements_total": result.fom.measurements_total,
                "fom_reflections_total": result.fom.reflections_total,
                "fom_reflections_possible": result.fom.reflections_possible,
                "fom_r_split": result.fom.r_split,
                "fom_r1i": result.fom.r1i,
                "fom_2": result.fom.r2,
                "fom_cc": result.fom.cc,
                "fom_ccstar": result.fom.ccstar,
                "fom_ccano": result.fom.ccano,
                "fom_crdano": result.fom.crdano,
                "fom_rano": result.fom.rano,
                "fom_rano_over_r_split": result.fom.rano_over_r_split,
                "fom_d1sig": result.fom.d1sig,
                "fom_d2sig": result.fom.d2sig,
                "fom_outer_resolution": result.fom.outer_shell.resolution,
                "fom_outer_ccstar": result.fom.outer_shell.ccstar,
                "fom_outer_r_split": result.fom.outer_shell.r_split,
                "fom_outer_cc": result.fom.outer_shell.cc,
                "fom_outer_unique_reflections": result.fom.outer_shell.unique_reflections,
                "fom_outer_completeness": result.fom.outer_shell.completeness,
                "fom_outer_redundancy": result.fom.outer_shell.redundancy,
                "fom_outer_snr": result.fom.outer_shell.snr,
                "fom_outer_min_res": result.fom.outer_shell.min_res,
                "fom_outer_max_res": result.fom.outer_shell.max_res,
            }


class AsyncDB:
    def __init__(self, dbcontext: AsyncDBContext, tables: DBTables) -> None:
        self.dbcontext = dbcontext
        self.tables = tables

    async def migrate(self) -> None:
        async with self.begin() as conn:
            await conn.run_sync(upgrade_to_head_connection)

    def read_only_connection(self) -> Connection:
        return self.dbcontext.read_only_connection()

    def begin(self) -> Connection:
        return self.dbcontext.begin()

    async def dispose(self) -> None:
        await self.dbcontext.dispose()

    async def retrieve_beamtimes(self, conn: Connection) -> list[BeamtimeOutput]:
        btc = self.tables.beamtime.c
        beamtimes_and_chemicals = list(
            await conn.execute(
                select(
                    btc.id,
                    btc.external_id,
                    btc.proposal,
                    btc.beamline,
                    btc.title,
                    btc.comment,
                    btc.start,
                    btc.end,
                    self.tables.chemical.c.name,
                ).select_from(
                    self.tables.beamtime.outerjoin(
                        self.tables.chemical,
                        self.tables.chemical.c.beamtime_id == btc.id,
                    )
                )
            )
        )
        if not beamtimes_and_chemicals:
            return []
        chemical_names_for_beamtime: dict[int, list[str]] = {
            bt.id: [] for bt in beamtimes_and_chemicals
        }
        beamtime_ids_so_far: set[int] = set()
        for bt_and_c in beamtimes_and_chemicals:
            if bt_and_c.name is not None:
                chemical_names_for_beamtime[bt_and_c.id].append(bt_and_c.name)
        result: list[BeamtimeOutput] = []
        for o in beamtimes_and_chemicals:
            if o.id in beamtime_ids_so_far:
                continue
            beamtime_ids_so_far.add(o.id)
            result.append(
                BeamtimeOutput(
                    id=BeamtimeId(o.id),
                    external_id=o.external_id,
                    proposal=o.proposal,
                    beamline=o.beamline,
                    title=o.title,
                    comment=o.comment,
                    start=o.start,
                    end=o.end,
                    chemical_names=chemical_names_for_beamtime[o.id],
                )
            )
        return result

    async def retrieve_beamtime(
        self, conn: Connection, bt: BeamtimeId
    ) -> BeamtimeOutput:
        o = (
            await conn.execute(
                self.tables.beamtime.select().where(self.tables.beamtime.c.id == bt)
            )
        ).fetchone()
        return BeamtimeOutput(
            id=BeamtimeId(o.id),
            external_id=o.external_id,
            proposal=o.proposal,
            beamline=o.beamline,
            title=o.title,
            comment=o.comment,
            start=o.start,
            end=o.end,
            chemical_names=None,
        )

    async def create_beamtime(self, conn: Connection, bt: BeamtimeInput) -> BeamtimeId:
        return BeamtimeId(
            (
                await conn.execute(
                    self.tables.beamtime.insert().values(
                        external_id=bt.external_id,
                        proposal=bt.proposal,
                        beamline=bt.beamline,
                        title=bt.title,
                        comment=bt.comment,
                        start=bt.start,
                        end=bt.end,
                    )
                )
            ).inserted_primary_key[0]
        )

    async def update_beamtime(
        self,
        conn: Connection,
        beamtime_id: BeamtimeId,
        external_id: str,
        proposal: str,
        beamline: str,
        title: str,
        comment: str,
        start: datetime.datetime,
        end: datetime.datetime,
    ) -> None:
        await conn.execute(
            self.tables.beamtime.update()
            .values(
                external_id=external_id,
                proposal=proposal,
                beamline=beamline,
                title=title,
                comment=comment,
                start=start,
                end=end,
            )
            .where(self.tables.beamtime.c.id == beamtime_id)
        )

    async def replace_beamtime_schedule(
        self,
        conn: Connection,
        beamtime_id: BeamtimeId,
        schedule: list[BeamtimeScheduleEntry],
    ) -> None:
        await conn.execute(
            self.tables.beamtime_schedule.delete().where(
                self.tables.beamtime_schedule.c.beamtime_id == beamtime_id
            )
        )
        for entry in schedule:
            chemical_ids = entry.chemicals
            inserted_schedule_id = (
                await conn.execute(
                    self.tables.beamtime_schedule.insert().values(
                        beamtime_id=beamtime_id,
                        users=entry.users,
                        date=entry.date,
                        shift=entry.shift,
                        td_support=entry.td_support,
                        comment=entry.comment,
                    )
                )
            ).inserted_primary_key[0]

            for chemical_id in chemical_ids:
                await conn.execute(
                    self.tables.beamtime_schedule_has_chemical.insert().values(
                        beamtime_schedule_id=inserted_schedule_id,
                        chemical_id=chemical_id,
                    )
                )

    async def retrieve_beamtime_schedule(
        self, conn: Connection, beamtime_id: BeamtimeId
    ) -> list[BeamtimeScheduleEntry]:
        schedule_chemicals_select = await conn.execute(
            sa.select(
                self.tables.beamtime_schedule_has_chemical.c.beamtime_schedule_id,
                self.tables.beamtime_schedule_has_chemical.c.chemical_id,
            )
            .join(self.tables.beamtime_schedule)
            .where(self.tables.beamtime_schedule.c.beamtime_id == beamtime_id)
        )

        schedule_chemicals_fetched: list[
            tuple[int, int]
        ] = schedule_chemicals_select.fetchall()
        schedule_chemicals_by_chemical: dict[int, list[int]] = {
            k: [*map(lambda v: v[1], values)]
            for k, values in itertools.groupby(
                sorted(schedule_chemicals_fetched, key=lambda x: x[0]),
                lambda x: x[0],
            )
        }

        schedule = await conn.execute(
            self.tables.beamtime_schedule.select().where(
                self.tables.beamtime_schedule.c.beamtime_id == beamtime_id
            )
        )
        return [
            BeamtimeScheduleEntry(
                users=se.users,
                comment=se.comment,
                date=se.date,
                shift=se.shift,
                td_support=se.td_support,
                chemicals=schedule_chemicals_by_chemical.get(se.id, []),
            )
            for se in schedule.fetchall()
        ]

    async def retrieve_file_id_by_name(
        self, conn: Connection, file_name: str
    ) -> int | None:
        result = await conn.execute(
            sa.select(
                self.tables.file.c.id,
            ).where(self.tables.file.c.file_name == file_name)
        )

        db_result = result.fetchone()

        return db_result[0] if db_result else None

    async def retrieve_file(
        self, conn: Connection, file_id: int, with_contents: bool
    ) -> DBFile:
        result = await conn.execute(
            sa.select(
                *(
                    [
                        self.tables.file.c.file_name,
                        self.tables.file.c.description,
                        self.tables.file.c.original_path,
                        self.tables.file.c.type,
                        self.tables.file.c.size_in_bytes,
                    ]
                    + ([self.tables.file.c.contents] if with_contents else [])
                )
            ).where(self.tables.file.c.id == file_id)
        )

        result_row = result.fetchone()

        if result_row is None:
            raise NotFoundError(missing_id=file_id)

        return DBFile(
            id=file_id,
            description=result_row.description,
            type_=result_row.type,
            original_path=result_row.original_path,
            file_name=result_row.file_name,
            size_in_bytes=result_row.size_in_bytes,
            contents=result_row.contents if with_contents else None,
        )

    async def retrieve_configuration(
        self, conn: Connection, beamtime_id: BeamtimeId
    ) -> UserConfiguration:
        cc = self.tables.configuration.c
        result = (
            await conn.execute(
                sa.select(
                    cc.auto_pilot,
                    cc.use_online_crystfel,
                    cc.current_experiment_type_id,
                )
                .order_by(cc.id.desc())
                .where(cc.beamtime_id == beamtime_id)
            )
        ).fetchone()

        return (
            UserConfiguration(
                auto_pilot=result[0],
                use_online_crystfel=result[1] if result[1] is not None else False,
                current_experiment_type_id=result[2],
            )
            if result is not None
            else initial_user_configuration()
        )

    async def update_configuration(
        self,
        conn: Connection,
        beamtime_id: BeamtimeId,
        configuration: UserConfiguration,
    ) -> None:
        await conn.execute(
            self.tables.configuration.insert().values(
                auto_pilot=configuration.auto_pilot,
                use_online_crystfel=configuration.use_online_crystfel,
                current_experiment_type_id=configuration.current_experiment_type_id,
                created=datetime.datetime.utcnow(),
                beamtime_id=beamtime_id,
            )
        )

    async def retrieve_attributi(
        self,
        conn: Connection,
        beamtime_id: None | BeamtimeId,
        associated_table: AssociatedTable | None,
    ) -> list[DBAttributo]:
        ac = self.tables.attributo.c
        select_stmt = sa.select(
            ac.id,
            ac.name,
            ac.beamtime_id,
            ac.description,
            ac.group,
            ac.json_schema,
            ac.associated_table,
        ).order_by(ac.associated_table)

        if beamtime_id is not None:
            select_stmt = select_stmt.where(ac.beamtime_id == beamtime_id)

        if associated_table is not None:
            select_stmt = select_stmt.where(ac.associated_table == associated_table)

        result = await conn.execute(select_stmt)
        return [
            DBAttributo(
                id=a.id,
                beamtime_id=a.beamtime_id,
                name=a.name,
                description=a.description,
                group=a.group,
                associated_table=a.associated_table,
                attributo_type=schema_json_to_attributo_type(a.json_schema),
            )
            for a in result
        ]

    async def retrieve_refinement_results(
        self, conn: Connection, beamtime_id: None | BeamtimeId = None
    ) -> list[DBRefinementResultOutput]:
        rr = self.tables.refinement_result.c
        statement = sa.select(
            rr.id,
            rr.merge_result_id,
            rr.pdb_file_id,
            rr.mtz_file_id,
            rr.r_free,
            rr.r_work,
            rr.rms_bond_angle,
            rr.rms_bond_length,
        )
        if beamtime_id is not None:
            statement = (
                statement.join(
                    self.tables.merge_result,
                    self.tables.merge_result.c.id == rr.merge_result_id,
                )
                .join(
                    self.tables.merge_result_has_indexing_result,
                    self.tables.merge_result_has_indexing_result.c.merge_result_id
                    == self.tables.merge_result.c.id,
                )
                .join(
                    self.tables.indexing_result,
                    self.tables.indexing_result.c.id
                    == self.tables.merge_result_has_indexing_result.c.indexing_result_id,
                )
                .join(
                    self.tables.run,
                    self.tables.run.c.id == self.tables.indexing_result.c.run_id,
                )
                .where(self.tables.run.c.beamtime_id == beamtime_id)
                .distinct()
            )
        return [
            DBRefinementResultOutput(
                id=row.id,
                merge_result_id=row.merge_result_id,
                pdb_file_id=row.pdb_file_id,
                mtz_file_id=row.mtz_file_id,
                r_free=row.r_free,
                r_work=row.r_work,
                rms_bond_angle=row.rms_bond_angle,
                rms_bond_length=row.rms_bond_length,
            )
            for row in await conn.execute(statement)
        ]

    async def create_refinement_result(
        self,
        conn: Connection,
        merge_result_id: int,
        pdb_file_id: int,
        mtz_file_id: int,
        rfree: float,
        rwork: float,
        rms_bond_angle: float,
        rms_bond_length: float,
    ) -> int:
        result: int = (
            await conn.execute(
                self.tables.refinement_result.insert().values(
                    merge_result_id=merge_result_id,
                    pdb_file_id=pdb_file_id,
                    mtz_file_id=mtz_file_id,
                    r_free=rfree,
                    r_work=rwork,
                    rms_bond_angle=rms_bond_angle,
                    rms_bond_length=rms_bond_length,
                )
            )
        ).inserted_primary_key[0]
        return result

    async def create_chemical(
        self,
        conn: Connection,
        beamtime_id: BeamtimeId,
        name: str,
        responsible_person: str,
        type_: ChemicalType,
        attributi: AttributiMap,
    ) -> int:
        new_chemical_id: int = (
            await conn.execute(
                self.tables.chemical.insert().values(
                    beamtime_id=beamtime_id,
                    name=name,
                    modified=datetime.datetime.utcnow(),
                    type=type_,
                    responsible_person=responsible_person,
                )
            )
        ).inserted_primary_key[0]
        for aid, avalue in attributi.items():
            if avalue is None:
                continue
            await conn.execute(
                self.tables.chemical_has_attributo_value.insert().values(
                    {"chemical_id": new_chemical_id, "attributo_id": aid}
                    | attributo_value_to_db_row(
                        avalue,
                        cast(DBAttributo, attributi.retrieve_type(aid)).attributo_type,
                    )
                )
            )
        return new_chemical_id

    async def update_chemical(
        self,
        conn: Connection,
        id_: int,
        name: str,
        responsible_person: str,
        type_: ChemicalType,
        attributi: AttributiMap,
    ) -> None:
        # We could do some magic to only update attributi that changed, but removing all and re-inserting them is hopefully fast enough.
        await conn.execute(
            sa.delete(self.tables.chemical_has_attributo_value).where(
                self.tables.chemical_has_attributo_value.c.chemical_id == id_
            )
        )
        await conn.execute(
            sa.update(self.tables.chemical)
            .values(
                name=name,
                responsible_person=responsible_person,
                modified=datetime.datetime.utcnow(),
                type=type_,
            )
            .where(self.tables.chemical.c.id == id_)
        )
        for aid, avalue in attributi.items():
            if avalue is None:
                continue
            await conn.execute(
                self.tables.chemical_has_attributo_value.insert().values(
                    {"chemical_id": id_, "attributo_id": aid}
                    | attributo_value_to_db_row(
                        avalue,
                        cast(DBAttributo, attributi.retrieve_type(aid)).attributo_type,
                    )
                )
            )

    async def _retrieve_files(
        self,
        conn: Connection,
        association_column: sa.Column[Any],
        where_clause: Any | None = None,
    ) -> dict[int, list[DBFile]]:
        select_stmt = (
            sa.select(
                association_column,
                self.tables.file.c.id,
                self.tables.file.c.description,
                self.tables.file.c.file_name,
                self.tables.file.c.type,
                self.tables.file.c.size_in_bytes,
                self.tables.file.c.original_path,
            )
            .select_from(
                association_column.table.join(
                    self.tables.file,
                    association_column.table.c.file_id == self.tables.file.c.id,
                )
            )
            .order_by(association_column)
        )
        if where_clause is not None:
            select_stmt = select_stmt.where(where_clause)
        file_results = (await conn.execute(select_stmt)).fetchall()

        result: dict[int, list[DBFile]] = {
            key: [
                DBFile(
                    id=row.id,
                    description=row.description,
                    type_=row.type,
                    file_name=row.file_name,
                    size_in_bytes=row.size_in_bytes,
                    original_path=row.original_path,
                    contents=None,
                )
                for row in group
            ]
            for key, group in itertools.groupby(
                file_results,
                # pylint: disable=protected-access
                key=lambda row: row._mapping[association_column.name],  # type: ignore
            )
        }
        return result

    async def retrieve_chemicals(
        self, conn: Connection, beamtime_id: BeamtimeId, attributi: list[DBAttributo]
    ) -> list[DBChemical]:
        chemical_to_files = await self._retrieve_files(
            conn, self.tables.chemical_has_file.c.chemical_id
        )

        cc = self.tables.chemical.c
        chemical_select_stmt = (
            sa.select(
                cc.id,
                cc.beamtime_id,
                cc.name,
                cc.responsible_person,
                cc.type,
            )
            .where(cc.beamtime_id == beamtime_id)
            .order_by(cc.name)
        )

        result = await conn.execute(chemical_select_stmt)

        chav = self.tables.chemical_has_attributo_value.c

        chemical_id_to_attributi: dict[
            int, list[tuple[int, AttributoId, AttributoValue]]
        ] = group_by(
            (
                (
                    cast(int, row.chemical_id),
                    AttributoId(cast(int, row.attributo_id)),
                    attributo_value_for_db_row(row),
                )
                for row in await conn.execute(
                    sa.select(
                        chav.chemical_id,
                        chav.attributo_id,
                        chav.integer_value,
                        chav.float_value,
                        chav.string_value,
                        chav.bool_value,
                        chav.datetime_value,
                        chav.list_value,
                    )
                    .select_from(self.tables.chemical_has_attributo_value)
                    .join(
                        self.tables.chemical,
                        self.tables.chemical.c.id == chav.chemical_id,
                    )
                    .where(self.tables.chemical.c.beamtime_id == beamtime_id)
                )
            ),
            lambda value_tuple: value_tuple[0],
        )

        return [
            DBChemical(
                id=a.id,
                beamtime_id=BeamtimeId(a.beamtime_id),
                name=a.name,
                responsible_person=a.responsible_person,
                type_=a.type,
                attributi=AttributiMap.from_types_and_value_rows(
                    types=attributi,
                    value_rows=[
                        (x[1], x[2]) for x in chemical_id_to_attributi.get(a.id, [])
                    ],
                ),
                files=chemical_to_files.get(a.id, []),
            )
            for a in result
        ]

    async def delete_event(
        self,
        conn: Connection,
        id_: int,
    ) -> None:
        await conn.execute(
            sa.delete(self.tables.event_log).where(self.tables.event_log.c.id == id_)
        )

    async def delete_file(
        self,
        conn: Connection,
        id_: int,
    ) -> None:
        await conn.execute(
            sa.delete(self.tables.file).where(self.tables.file.c.id == id_)
        )

    async def retrieve_events(
        self, conn: Connection, beamtime_id: BeamtimeId, log_level: None | EventLogLevel
    ) -> list[DBEvent]:
        ec = self.tables.event_log.c
        event_to_files = await self._retrieve_files(
            conn, self.tables.event_has_file.c.event_id
        )
        return [
            DBEvent(
                row.id,
                row.created,
                EventLogLevel(row.level),
                row.source,
                row.text,
                files=event_to_files.get(row.id, []),
            )
            for row in await conn.execute(
                sa.select(ec.id, ec.created, ec.level, ec.source, ec.text)
                .order_by(ec.created.desc())
                .where(
                    (ec.beamtime_id == beamtime_id)
                    & (ec.level == log_level if log_level is not None else True)
                )
            )
        ]

    async def create_event(
        self,
        conn: Connection,
        beamtime_id: BeamtimeId,
        level: EventLogLevel,
        source: str,
        text: str,
    ) -> int:
        result: int = (
            await conn.execute(
                self.tables.event_log.insert().values(
                    beamtime_id=beamtime_id,
                    created=datetime.datetime.utcnow(),
                    level=level.value,
                    source=source,
                    text=text,
                )
            )
        ).inserted_primary_key[0]
        return result

    async def retrieve_run_internal_from_external_id(
        self, conn: Connection, external_id: RunExternalId, beamtime_id: BeamtimeId
    ) -> None | RunInternalId:
        result = (
            await conn.execute(
                sa.select(self.tables.run.c.id).where(
                    (self.tables.run.c.external_id == external_id)
                    & (self.tables.run.c.beamtime_id == beamtime_id)
                )
            )
        ).one_or_none()
        return RunInternalId(result[0]) if result is not None else None

    async def retrieve_runs(
        self, conn: Connection, beamtime_id: BeamtimeId, attributi: list[DBAttributo]
    ) -> list[DBRunOutput]:
        select_stmt = (
            self.tables.run.select()
            .where(self.tables.run.c.beamtime_id == beamtime_id)
            .order_by(self.tables.run.c.external_id.desc())
        )

        result = await conn.execute(select_stmt)

        rhav = self.tables.run_has_attributo_value.c

        run_id_to_attributi: dict[
            int, list[tuple[RunInternalId, AttributoId, AttributoValue]]
        ] = group_by(
            (
                (
                    RunInternalId(cast(int, row.run_id)),
                    AttributoId(cast(int, row.attributo_id)),
                    attributo_value_for_db_row(row),
                )
                for row in await conn.execute(
                    sa.select(
                        rhav.run_id,
                        rhav.attributo_id,
                        rhav.integer_value,
                        rhav.float_value,
                        rhav.string_value,
                        rhav.bool_value,
                        rhav.datetime_value,
                        rhav.list_value,
                        rhav.chemical_value,
                    )
                    .select_from(self.tables.run_has_attributo_value)
                    .join(
                        self.tables.run,
                        self.tables.run.c.id == rhav.run_id,
                    )
                    .where(self.tables.run.c.beamtime_id == beamtime_id)
                )
            ),
            lambda value_tuple: value_tuple[0],
        )

        return [
            DBRunOutput(
                id=RunInternalId(a.id),
                external_id=RunExternalId(a.external_id),
                beamtime_id=a.beamtime_id,
                experiment_type_id=a.experiment_type_id,
                attributi=AttributiMap.from_types_and_value_rows(
                    types=attributi,
                    value_rows=[
                        (attributo_id, attributo_value)
                        for _, attributo_id, attributo_value in run_id_to_attributi.get(
                            RunInternalId(a.id), []
                        )
                    ],
                ),
                started=a.started,
                stopped=a.stopped,
                files=[],
            )
            for a in result
        ]

    async def delete_chemical(
        self,
        conn: Connection,
        id_: int,
        delete_in_dependencies: bool,
    ) -> None:
        if (
            not delete_in_dependencies
            and (
                await conn.execute(
                    self.tables.run_has_attributo_value.select().where(
                        self.tables.run_has_attributo_value.c.chemical_value == id_
                    )
                )
            ).one_or_none()
            is not None
        ):
            raise Exception("chemical still being used")

        # Small but important special case here: data sets are associations from attributo to a value. Data sets can contain chemical attributi.
        # If a chemical is delete that's part of a data set, then we want to delete the whole data set, not just the one row from DataSetHasAttributoValue.
        await conn.execute(
            sa.delete(self.tables.data_set).where(
                self.tables.data_set.c.id.in_(
                    sa.select(self.tables.data_set_has_attributo_value.c.data_set_id)
                    .select_from(self.tables.data_set_has_attributo_value)
                    .where(
                        self.tables.data_set_has_attributo_value.c.chemical_value == id_
                    )
                )
            )
        )
        await conn.execute(
            sa.delete(self.tables.chemical).where(self.tables.chemical.c.id == id_)
        )

    async def create_attributo(
        self,
        conn: Connection,
        name: str,
        beamtime_id: BeamtimeId,
        description: str,
        group: str,
        associated_table: AssociatedTable,
        type_: AttributoType,
    ) -> AttributoId:
        if associated_table == AssociatedTable.CHEMICAL and isinstance(
            type_, AttributoTypeChemical
        ):
            raise ValueError(f"{name}: chemicals can't have attributi of type chemical")
        if isinstance(type_, AttributoTypeDecimal) and type_.standard_unit:
            if type_.suffix is None:
                raise ValueError(f"{name}: got a standard unit, but no suffix")
            if not valid_pint_unit(type_.suffix):
                raise ValueError(f"{name}: unit {type_.suffix} not a valid unit")
        return AttributoId(
            (
                await conn.execute(
                    self.tables.attributo.insert().values(
                        beamtime_id=beamtime_id,
                        name=name,
                        description=description,
                        associated_table=associated_table,
                        group=group,
                        json_schema=attributo_type_to_schema(type_).dict(),
                    )
                )
            ).inserted_primary_key[0]
        )

    async def delete_attributo(
        self,
        conn: Connection,
        attributo_id: AttributoId,
    ) -> None:
        await conn.execute(
            sa.delete(self.tables.attributo).where(
                self.tables.attributo.c.id == attributo_id
            )
        )

    async def update_file(
        self, conn: Connection, id_: int, contents_location: Path
    ) -> None:
        mime = magic.from_file(str(contents_location), mime=True)  # type: ignore

        sha256 = sha256_file(contents_location)

        size_in_bytes = contents_location.stat().st_size
        with contents_location.open("rb") as f:
            await conn.execute(
                sa.update(self.tables.file)
                .values(
                    modified=datetime.datetime.utcnow(),
                    contents=f.read(),
                    type=mime,
                    sha256=sha256,
                    size_in_bytes=size_in_bytes,
                )
                .where(self.tables.file.c.id == id_)
            )

    async def duplicate_file(
        self, conn: Connection, id_: int, new_file_name: str | None
    ) -> CreateFileResult:
        original_file = await self.retrieve_file(conn, id_, with_contents=True)
        return await self.create_file_from_bytes(
            conn,
            file_name=original_file.file_name
            if new_file_name is None
            else new_file_name,
            description=original_file.description,
            original_path=Path(original_file.original_path)
            if original_file.original_path is not None
            else None,
            contents=cast(bytes, original_file.contents),
            deduplicate=False,
        )

    async def update_file_from_bytes(
        self,
        conn: Connection,
        id_: int,
        file_name: str,
        description: str,
        original_path: Path | None,
        contents: bytes,
    ) -> None:
        mime = magic.from_buffer(contents, mime=True)

        sha256 = hashlib.sha256(contents).hexdigest()

        await conn.execute(
            self.tables.file.update()
            .values(
                type=mime,
                modified=datetime.datetime.utcnow(),
                contents=contents,
                file_name=file_name,
                original_path=str(original_path),
                description=description,
                sha256=sha256,
                size_in_bytes=len(contents),
            )
            .where(self.tables.file.c.id == id_)
        )

    async def create_file_from_bytes(
        self,
        conn: Connection,
        file_name: str,
        description: str,
        original_path: Path | None,
        contents: bytes,
        deduplicate: bool,
    ) -> CreateFileResult:
        mime = magic.from_buffer(contents, mime=True)

        sha256 = hashlib.sha256(contents).hexdigest()

        if deduplicate:
            existing_file = (
                await conn.execute(
                    sa.select(
                        self.tables.file.c.id,
                        self.tables.file.c.type,
                        self.tables.file.c.size_in_bytes,
                    ).where(
                        (self.tables.file.c.sha256 == sha256)
                        & (self.tables.file.c.file_name == file_name)
                    )
                )
            ).fetchone()

            if existing_file is not None:
                logger.info(f"file {file_name} already found, not creating another one")
                return CreateFileResult(
                    existing_file.id,
                    existing_file.type,
                    existing_file.size_in_bytes,
                )

        return CreateFileResult(
            id=(
                await conn.execute(
                    self.tables.file.insert().values(
                        type=mime,
                        modified=datetime.datetime.utcnow(),
                        contents=contents,
                        file_name=file_name,
                        original_path=str(original_path),
                        description=description,
                        sha256=sha256,
                        size_in_bytes=len(contents),
                    )
                )
            ).inserted_primary_key[0],
            type_=mime,
            size_in_bytes=len(contents),
        )

    async def create_file(
        self,
        conn: Connection,
        file_name: str,
        description: str,
        original_path: Path | None,
        contents_location: Path,
        deduplicate: bool,
    ) -> CreateFileResult:
        mime = magic.from_file(str(contents_location), mime=True)  # type: ignore

        sha256 = sha256_file(contents_location)

        if deduplicate:
            existing_file = (
                await conn.execute(
                    sa.select(
                        self.tables.file.c.id,
                        self.tables.file.c.type,
                        self.tables.file.c.size_in_bytes,
                    ).where(
                        (self.tables.file.c.sha256 == sha256)
                        & (self.tables.file.c.file_name == file_name)
                    )
                )
            ).fetchone()

            if existing_file is not None:
                logger.info(f"file {file_name} already found, not creating another one")
                return CreateFileResult(
                    existing_file.id,
                    existing_file.type,
                    existing_file.size_in_bytes,
                )

        size_in_bytes = contents_location.stat().st_size
        with contents_location.open("rb") as f:
            return CreateFileResult(
                id=(
                    await conn.execute(
                        self.tables.file.insert().values(
                            type=mime,
                            modified=datetime.datetime.utcnow(),
                            contents=f.read(),
                            file_name=file_name,
                            original_path=str(original_path),
                            description=description,
                            sha256=sha256,
                            size_in_bytes=size_in_bytes,
                        )
                    )
                ).inserted_primary_key[0],
                type_=mime,
                size_in_bytes=size_in_bytes,
            )

    async def update_attributo(
        self,
        conn: Connection,
        id_: AttributoId,
        beamtime_id: BeamtimeId,
        conversion_flags: AttributoConversionFlags,
        new_attributo: DBAttributo,
    ) -> None:
        current_attributi = await self.retrieve_attributi(
            conn, beamtime_id, associated_table=new_attributo.associated_table
        )

        current_attributo = next(
            iter(x for x in current_attributi if x.id == id_), None
        )

        if current_attributo is None:
            raise Exception(f"couldn't find attributo with ID {id_}")

        # first, change the attributo itself, then its actual values (if possible)
        await conn.execute(
            sa.update(self.tables.attributo)
            .values(
                name=new_attributo.name,
                description=new_attributo.description,
                group=new_attributo.group,
                json_schema=attributo_type_to_schema(
                    new_attributo.attributo_type
                ).dict(),
            )
            .where(self.tables.attributo.c.id == id_)
        )

        if new_attributo.associated_table == AssociatedTable.CHEMICAL:
            chav = self.tables.chemical_has_attributo_value
            delete_ids: set[int] = set()
            insertions: list[dict[str, Any]] = []
            for row in await conn.execute(
                chav.select().where(chav.c.attributo_id == id_)
            ):
                delete_ids.add(row.chemical_id)
                insertions.append(
                    {"attributo_id": id_, "chemical_id": row.chemical_id}
                    | attributo_value_to_db_row(
                        convert_attributo_value(
                            before_type=current_attributo.attributo_type,
                            after_type=new_attributo.attributo_type,
                            conversion_flags=conversion_flags,
                            value=attributo_value_for_db_row(row),
                        ),
                        new_attributo.attributo_type,
                    )
                )

            # Again, we could intelligently update, but sending two queries might be just fast enough.
            if delete_ids:
                await conn.execute(
                    chav.delete().where(chav.c.chemical_id.in_(delete_ids))
                )
            if insertions:
                await conn.execute(sa.insert(chav), insertions)

        elif new_attributo.associated_table == AssociatedTable.RUN:
            chav = self.tables.run_has_attributo_value
            delete_ids: set[int] = set()
            insertions: list[dict[str, Any]] = []
            for row in await conn.execute(
                chav.select().where(chav.c.attributo_id == id_)
            ):
                # Again, we could intelligently update, but sending two queries might be just fast enough.
                delete_ids.add(row.run_id)
                insertions.append(
                    {"attributo_id": id_, "run_id": row.run_id}
                    | attributo_value_to_db_row(
                        convert_attributo_value(
                            before_type=current_attributo.attributo_type,
                            after_type=new_attributo.attributo_type,
                            conversion_flags=conversion_flags,
                            value=attributo_value_for_db_row(row),
                        ),
                        new_attributo.attributo_type,
                    )
                )
            # Again, we could intelligently update, but sending two queries might be just fast enough.
            if delete_ids:
                await conn.execute(chav.delete().where(chav.c.run_id.in_(delete_ids)))
            if insertions:
                await conn.execute(sa.insert(chav), insertions)
        else:
            raise Exception(
                f"unimplemented: is there a new associated table {new_attributo.associated_table}?"
            )

        chav = self.tables.data_set_has_attributo_value
        delete_ids: set[int] = set()
        insertions: list[dict[str, Any]] = []
        for row in await conn.execute(chav.select().where(chav.c.attributo_id == id_)):
            # Again, we could intelligently update, but sending two queries might be just fast enough.
            delete_ids.add(row.data_set_id)
            insertions.append(
                {"attributo_id": id_, "data_set_id": row.data_set_id}
                | attributo_value_to_db_row(
                    convert_attributo_value(
                        before_type=current_attributo.attributo_type,
                        after_type=new_attributo.attributo_type,
                        conversion_flags=conversion_flags,
                        value=attributo_value_for_db_row(row),
                    ),
                    new_attributo.attributo_type,
                )
            )
        # Again, we could intelligently update, but sending two queries might be just fast enough.
        if delete_ids:
            await conn.execute(chav.delete().where(chav.c.data_set_id.in_(delete_ids)))
        if insertions:
            await conn.execute(sa.insert(chav), insertions)

    async def add_file_to_chemical(
        self, conn: Connection, file_id: int, chemical_id: int
    ) -> None:
        await conn.execute(
            sa.insert(self.tables.chemical_has_file).values(
                file_id=file_id, chemical_id=chemical_id
            )
        )

    async def add_file_to_event(
        self, conn: Connection, file_id: int, event_id: int
    ) -> None:
        await conn.execute(
            sa.insert(self.tables.event_has_file).values(
                file_id=file_id, event_id=event_id
            )
        )

    async def remove_files_from_chemical(
        self, conn: Connection, chemical_id: int
    ) -> None:
        await conn.execute(
            sa.delete(self.tables.chemical_has_file).where(
                self.tables.chemical_has_file.c.chemical_id == chemical_id
            )
        )

    async def create_indexing_result(
        self, conn: Connection, r: DBIndexingResultInput
    ) -> int:
        fom = (
            r.runtime_status.fom
            if isinstance(
                r.runtime_status,
                (DBIndexingResultRunning, DBIndexingResultDone),
            )
            else None
        )
        return (  # type: ignore
            await conn.execute(
                sa.insert(self.tables.indexing_result).values(
                    created=r.created,
                    run_id=r.run_id,
                    stream_file=str(r.runtime_status.stream_file)
                    if isinstance(
                        r.runtime_status,
                        (DBIndexingResultRunning, DBIndexingResultDone),
                    )
                    else None,
                    hits=r.hits,
                    frames=r.frames,
                    not_indexed_frames=r.not_indexed_frames,
                    hit_rate=fom.hit_rate if fom is not None else 0.0,
                    indexing_rate=fom.indexing_rate if fom is not None else 0.0,
                    indexed_frames=fom.indexed_frames if fom is not None else 0.0,
                    cell_description=coparse_cell_description(r.cell_description)
                    if r.cell_description is not None
                    else None,
                    point_group=r.point_group,
                    chemical_id=r.chemical_id,
                    job_id=r.runtime_status.job_id
                    if isinstance(r.runtime_status, DBIndexingResultRunning)
                    else None,
                    job_status=DBJobStatus.RUNNING
                    if isinstance(r.runtime_status, DBIndexingResultRunning)
                    else DBJobStatus.QUEUED
                    if r.runtime_status is None
                    else DBJobStatus.DONE,
                    job_error=r.runtime_status.job_error
                    if isinstance(r.runtime_status, DBIndexingResultDone)
                    else None,
                )
            )
        ).inserted_primary_key[0]

    async def create_run(
        self,
        conn: Connection,
        run_external_id: RunExternalId,
        started: datetime.datetime,
        beamtime_id: BeamtimeId,
        attributi: list[DBAttributo],
        attributi_map: AttributiMap,
        experiment_type_id: int,
        keep_manual_attributes_from_previous_run: bool,
    ) -> RunInternalId:
        final_attributi_map: AttributiMap
        if keep_manual_attributes_from_previous_run:
            latest_run = await self.retrieve_latest_run(conn, beamtime_id, attributi)
            if latest_run is not None:
                final_attributi_map = latest_run.attributi.create_sub_map_for_group(
                    ATTRIBUTO_GROUP_MANUAL
                )
                final_attributi_map.extend_with_attributi_map(attributi_map)
            else:
                final_attributi_map = attributi_map
        else:
            final_attributi_map = attributi_map
        internal_id = RunInternalId(
            (
                await conn.execute(
                    sa.insert(self.tables.run).values(
                        beamtime_id=beamtime_id,
                        external_id=run_external_id,
                        experiment_type_id=experiment_type_id,
                        started=started,
                        modified=datetime.datetime.utcnow(),
                    )
                )
            ).inserted_primary_key[0]
        )
        for aid, avalue in final_attributi_map.items():
            if avalue is None:
                continue
            await conn.execute(
                self.tables.run_has_attributo_value.insert().values(
                    {"run_id": internal_id, "attributo_id": aid}
                    | attributo_value_to_db_row(
                        avalue,
                        cast(
                            DBAttributo, final_attributi_map.retrieve_type(aid)
                        ).attributo_type,
                    )
                )
            )

        return internal_id

    async def retrieve_latest_run(
        self, conn: Connection, beamtime_id: BeamtimeId, attributi: list[DBAttributo]
    ) -> DBRunOutput | None:
        maximum_id = (
            await conn.execute(
                sa.select(
                    sa.func.max(self.tables.run.c.id)  # pylint: disable=not-callable
                ).where(self.tables.run.c.beamtime_id == beamtime_id)
            )
        ).fetchone()

        if maximum_id[0] is None:
            return None

        return await self.retrieve_run(conn, RunInternalId(maximum_id[0]), attributi)

    async def retrieve_chemical(
        self, conn: Connection, id_: int, attributi: list[DBAttributo]
    ) -> DBChemical | None:
        rc = self.tables.chemical.c
        r = (
            await conn.execute(
                sa.select(
                    rc.id,
                    rc.name,
                    rc.type,
                    rc.responsible_person,
                    rc.beamtime_id,
                ).where(rc.id == id_)
            )
        ).fetchone()
        files = await self._retrieve_files(
            conn,
            self.tables.chemical_has_file.c.chemical_id,
            (self.tables.chemical_has_file.c.chemical_id == id_),
        )
        if r is None:
            return None

        chav = self.tables.chemical_has_attributo_value.c

        attributi_values: list[tuple[AttributoId, AttributoValue]] = [
            (AttributoId(row.attributo_id), attributo_value_for_db_row(row))
            for row in await conn.execute(
                sa.select(
                    chav.attributo_id,
                    chav.integer_value,
                    chav.float_value,
                    chav.string_value,
                    chav.bool_value,
                    chav.datetime_value,
                    chav.list_value,
                )
                .select_from(self.tables.chemical_has_attributo_value)
                .where(chav.chemical_id == id_)
            )
        ]

        return DBChemical(
            id=id_,
            beamtime_id=BeamtimeId(r.beamtime_id),
            name=r.name,
            responsible_person=r.responsible_person,
            type_=r.type,
            attributi=AttributiMap.from_types_and_value_rows(
                attributi, value_rows=attributi_values
            ),
            files=files.get(id_, []),
        )

    async def retrieve_beamtime_for_run(
        self,
        conn: Connection,
        internal_id: RunInternalId,
    ) -> BeamtimeId:
        return (
            (
                await conn.execute(
                    self.tables.run.select().where(self.tables.run.c.id == internal_id)
                )
            ).fetchone()
        ).beamtime_id

    async def retrieve_run(
        self,
        conn: Connection,
        internal_id: RunInternalId,
        attributi: list[DBAttributo],
    ) -> DBRunOutput:
        rc = self.tables.run.c
        r = (
            await conn.execute(self.tables.run.select().where(rc.id == internal_id))
        ).fetchone()
        assert r is not None
        rhav = self.tables.run_has_attributo_value.c
        attributi_values: list[tuple[AttributoId, AttributoValue]] = [
            (AttributoId(row.attributo_id), attributo_value_for_db_row(row))
            for row in await conn.execute(
                sa.select(
                    rhav.attributo_id,
                    rhav.integer_value,
                    rhav.float_value,
                    rhav.string_value,
                    rhav.bool_value,
                    rhav.datetime_value,
                    rhav.list_value,
                    rhav.chemical_value,
                )
                .select_from(self.tables.run_has_attributo_value)
                .where(rhav.run_id == internal_id)
            )
        ]
        return DBRunOutput(
            id=internal_id,
            external_id=RunExternalId(r.external_id),
            beamtime_id=BeamtimeId(r.beamtime_id),
            experiment_type_id=r.experiment_type_id,
            started=r.started,
            stopped=r.stopped,
            attributi=AttributiMap.from_types_and_value_rows(
                attributi,
                value_rows=attributi_values,
            ),
            files=[],
        )

    async def update_run_experiment_type(
        self, conn: Connection, id_: RunInternalId, experiment_type_id: int
    ) -> None:
        await conn.execute(
            sa.update(self.tables.run)
            .values(experiment_type_id=experiment_type_id)
            .where(self.tables.run.c.id == id_)
        )

    async def update_run(
        self,
        conn: Connection,
        internal_id: RunInternalId,
        stopped: None | datetime.datetime,
        attributi: AttributiMap,
        new_experiment_type_id: None | int,
    ) -> None:
        # We could do some magic to only update attributi that changed, but removing all and re-inserting them is hopefully fast enough.
        await conn.execute(
            sa.delete(self.tables.run_has_attributo_value).where(
                self.tables.run_has_attributo_value.c.run_id == internal_id
            )
        )
        for aid, avalue in attributi.items():
            if avalue is None:
                continue
            await conn.execute(
                self.tables.run_has_attributo_value.insert().values(
                    {"run_id": internal_id, "attributo_id": aid}
                    | attributo_value_to_db_row(
                        avalue,
                        cast(DBAttributo, attributi.retrieve_type(aid)).attributo_type,
                    )
                )
            )
        await conn.execute(
            sa.update(self.tables.run)
            .values(
                {"stopped": stopped}
                | (
                    {}
                    if new_experiment_type_id is None
                    else {"experiment_type_id": new_experiment_type_id}
                )
            )
            .where(self.tables.run.c.id == internal_id)
        )

    async def create_experiment_type(
        self,
        conn: Connection,
        beamtime_id: BeamtimeId,
        name: str,
        experiment_attributi: Iterable[AttributoIdAndRole],
    ) -> int:
        experiment_type_id = (
            await conn.execute(
                self.tables.experiment_type.insert().values(
                    {"name": name, "beamtime_id": beamtime_id}
                )
            )
        ).inserted_primary_key[0]

        await conn.execute(
            self.tables.experiment_has_attributo.insert().values(
                [
                    {
                        "experiment_type_id": experiment_type_id,
                        "attributo_id": a.attributo_id,
                        "chemical_role": a.chemical_role,
                    }
                    for a in experiment_attributi
                ]
            )
        )

        return experiment_type_id  # type: ignore

    async def delete_experiment_type(self, conn: Connection, id_: int) -> None:
        await conn.execute(
            self.tables.experiment_type.delete().where(
                self.tables.experiment_type.c.id == id_
            )
        )

    async def retrieve_experiment_types(
        self, conn: Connection, beamtime_id: BeamtimeId
    ) -> list[DBExperimentType]:
        result: list[DBExperimentType] = []
        etc = self.tables.experiment_has_attributo.c
        et = self.tables.experiment_type
        for key, group in itertools.groupby(
            await conn.execute(
                sa.select(et.c.id, et.c.name, etc.attributo_id, etc.chemical_role)
                .join(et, et.c.id == etc.experiment_type_id)
                .where(et.c.beamtime_id == beamtime_id)
                .order_by(etc.experiment_type_id)
            ),
            key=lambda row: row.id,  # type: ignore
        ):
            group_list = list(group)
            result.append(
                DBExperimentType(
                    id=key,
                    name=group_list[0].name,
                    attributi=[
                        AttributoIdAndRole(
                            attributo_id=row.attributo_id,
                            chemical_role=row.chemical_role,
                        )
                        for row in group_list
                    ],
                )
            )
        return result

    async def create_data_set(
        self,
        conn: Connection,
        beamtime_id: BeamtimeId,
        experiment_type_id: int,
        attributi: AttributiMap,
    ) -> int:
        matching_experiment_type: DBExperimentType | None = next(
            (
                x
                for x in await self.retrieve_experiment_types(conn, beamtime_id)
                if x.id == experiment_type_id
            ),
            None,
        )
        if matching_experiment_type is None:
            raise Exception(
                f'couldn\'t find experiment type with ID "{experiment_type_id}"'
            )

        existing_attributo_ids = [
            a.attributo_id for a in matching_experiment_type.attributi
        ]

        superfluous_attributi = attributi.ids().difference(existing_attributo_ids)

        if superfluous_attributi:
            raise Exception(
                "the following attributi are not part of the data set definition: "
                + ", ".join(str(x) for x in superfluous_attributi)
            )

        if not attributi.items():
            raise Exception("You have to assign at least one value to an attributo")

        data_set_id: int = (
            await conn.execute(
                self.tables.data_set.insert().values(
                    experiment_type_id=experiment_type_id,
                )
            )
        ).inserted_primary_key[0]

        for aid, avalue in attributi.items():
            if avalue is None:
                continue
            await conn.execute(
                self.tables.data_set_has_attributo_value.insert().values(
                    {"data_set_id": data_set_id, "attributo_id": aid}
                    | attributo_value_to_db_row(
                        avalue,
                        cast(DBAttributo, attributi.retrieve_type(aid)).attributo_type,
                    )
                )
            )

        return data_set_id

    async def delete_data_set(self, conn: Connection, id_: int) -> None:
        await conn.execute(
            self.tables.data_set.delete().where(self.tables.data_set.c.id == id_)
        )

    async def retrieve_data_set(
        self,
        conn: Connection,
        data_set_id: int,
        attributi: list[DBAttributo],
    ) -> None | DBDataSet:
        dc = self.tables.data_set.c
        r = (
            await conn.execute(
                sa.select(dc.experiment_type_id).where(dc.id == data_set_id)
            )
        ).fetchone()
        if r is None:
            return None
        dshav = self.tables.data_set_has_attributo_value.c
        attributi_values: list[tuple[AttributoId, AttributoValue]] = [
            (AttributoId(row.attributo_id), attributo_value_for_db_row(row))
            for row in await conn.execute(
                sa.select(
                    dshav.attributo_id,
                    dshav.integer_value,
                    dshav.float_value,
                    dshav.string_value,
                    dshav.bool_value,
                    dshav.datetime_value,
                    dshav.list_value,
                    dshav.chemical_value,
                )
                .select_from(self.tables.data_set_has_attributo_value)
                .where(dshav.data_set_id == data_set_id)
            )
        ]
        return DBDataSet(
            id=data_set_id,
            experiment_type_id=r.experiment_type_id,
            attributi=AttributiMap.from_types_and_value_rows(
                attributi, value_rows=attributi_values
            ),
        )

    async def retrieve_data_sets(
        self,
        conn: Connection,
        beamtime_id: BeamtimeId,
        attributi: list[DBAttributo],
    ) -> list[DBDataSet]:
        dc = self.tables.data_set.c

        dshav = self.tables.data_set_has_attributo_value.c
        data_set_id_to_attributi: dict[
            int, list[tuple[int, AttributoId, AttributoValue]]
        ] = group_by(
            (
                (
                    cast(int, row.data_set_id),
                    AttributoId(cast(int, row.attributo_id)),
                    attributo_value_for_db_row(row),
                )
                for row in await conn.execute(
                    sa.select(
                        dshav.data_set_id,
                        dshav.attributo_id,
                        dshav.integer_value,
                        dshav.float_value,
                        dshav.string_value,
                        dshav.bool_value,
                        dshav.datetime_value,
                        dshav.list_value,
                        dshav.chemical_value,
                    )
                    .select_from(self.tables.data_set_has_attributo_value)
                    .join(
                        self.tables.data_set,
                        self.tables.data_set.c.id == dshav.data_set_id,
                    )
                    .join(
                        self.tables.experiment_type,
                        self.tables.experiment_type.c.id
                        == self.tables.data_set.c.experiment_type_id,
                    )
                    .where(self.tables.experiment_type.c.beamtime_id == beamtime_id)
                )
            ),
            lambda value_tuple: value_tuple[0],
        )
        return [
            DBDataSet(
                id=r.id,
                experiment_type_id=r.experiment_type_id,
                attributi=AttributiMap.from_types_and_value_rows(
                    attributi,
                    value_rows=[
                        (x[1], x[2]) for x in data_set_id_to_attributi.get(r.id, [])
                    ],
                ),
            )
            for r in await conn.execute(
                sa.select(dc.id, dc.experiment_type_id)
                .join(self.tables.experiment_type)
                .where(self.tables.experiment_type.c.beamtime_id == beamtime_id)
            )
        ]

    async def retrieve_bulk_run_attributi(
        self,
        conn: Connection,
        beamtime_id: BeamtimeId,
        attributi: list[DBAttributo],
        run_ids: Iterable[RunInternalId],
    ) -> dict[AttributoId, set[AttributoValue]]:
        runs = await self.retrieve_runs(conn, beamtime_id, attributi)

        interesting_attributi = attributi

        attributi_values: dict[AttributoId, set[AttributoValue]] = {
            a.id: set() for a in interesting_attributi
        }
        for run in (run for run in runs if run.id in run_ids):
            for a in (a for a in interesting_attributi):
                run_attributo_value = run.attributi.select(a.id)
                if run_attributo_value is not None:
                    attributi_values[a.id].add(run_attributo_value)
        return attributi_values

    async def update_bulk_run_attributi(
        self,
        conn: Connection,
        beamtime_id: BeamtimeId,
        attributi: list[DBAttributo],
        run_ids: set[RunInternalId],
        attributi_values: AttributiMap,
        new_experiment_type_id: None | int,
    ) -> None:
        runs = await self.retrieve_runs(conn, beamtime_id, attributi)

        for run in (run for run in runs if run.id in run_ids):
            for attributo_id, attributo_value in attributi_values.items():
                run.attributi.append_single(attributo_id, attributo_value)
            await self.update_run(
                conn,
                internal_id=run.id,
                stopped=run.stopped,
                attributi=run.attributi,
                new_experiment_type_id=new_experiment_type_id,
            )

    async def retrieve_indexing_results(
        self,
        conn: Connection,
        beamtime_id: None | BeamtimeId,
        job_status_filter: None | DBJobStatus = None,
    ) -> list[DBIndexingResultOutput]:
        ir = self.tables.indexing_result

        run_external_id = self.tables.run.c.external_id.label("run_external_id")
        return [
            DBIndexingResultOutput(
                id=r.id,
                created=r.created,
                run_id=RunInternalId(r.run_id),
                run_external_id=RunExternalId(r.run_external_id),
                frames=r.frames,
                hits=r.hits,
                not_indexed_frames=r.not_indexed_frames,
                cell_description=parse_cell_description(r.cell_description)
                if r.cell_description is not None
                else None,
                point_group=r.point_group,
                chemical_id=r.chemical_id,
                runtime_status=_evaluate_indexing_result_runtime_status(r),
            )
            for r in await conn.execute(
                sa.select(
                    ir.c.id,
                    ir.c.created,
                    ir.c.run_id,
                    run_external_id,
                    ir.c.stream_file,
                    ir.c.frames,
                    ir.c.hits,
                    ir.c.not_indexed_frames,
                    ir.c.cell_description,
                    ir.c.point_group,
                    ir.c.chemical_id,
                    ir.c.hit_rate,
                    ir.c.indexing_rate,
                    ir.c.indexed_frames,
                    ir.c.detector_shift_x_mm,
                    ir.c.detector_shift_y_mm,
                    ir.c.job_id,
                    ir.c.job_status,
                    ir.c.job_error,
                )
                .join(
                    self.tables.run,
                    self.tables.run.c.id == ir.c.run_id,
                )
                .where(
                    self.tables.run.c.beamtime_id == beamtime_id
                    if beamtime_id is not None
                    else sa.true(),
                    ir.c.job_status == job_status_filter
                    if job_status_filter is not None
                    else sa.true(),
                )
            )
        ]

    async def add_indexing_result_statistic(
        self, conn: Connection, s: DBIndexingResultStatistic
    ) -> None:
        await conn.execute(
            self.tables.indexing_result_has_statistic.insert().values(
                indexing_result_id=s.indexing_result_id,
                time=s.time,
                frames=s.frames,
                hits=s.hits,
                indexed_frames=s.indexed_frames,
                indexed_crystals=s.indexed_crystals,
            )
        )

    async def retrieve_indexing_result_statistics(
        self,
        conn: Connection,
        beamtime_id: BeamtimeId,
        indexing_result_id: None | int,
    ) -> list[DBIndexingResultStatistic]:
        irhs = self.tables.indexing_result_has_statistic.c
        statement = (
            sa.select(
                irhs.indexing_result_id,
                irhs.time,
                irhs.frames,
                irhs.hits,
                irhs.indexed_frames,
                irhs.indexed_crystals,
            )
            .join(
                self.tables.indexing_result,
                self.tables.indexing_result.c.id
                == self.tables.indexing_result_has_statistic.c.indexing_result_id,
            )
            .join(
                self.tables.run,
                self.tables.run.c.id == self.tables.indexing_result.c.run_id,
            )
            .where(self.tables.run.c.beamtime_id == beamtime_id)
            .order_by(irhs.time)
        )
        if indexing_result_id is not None:
            statement = statement.where(irhs.indexing_result_id == indexing_result_id)
        return [
            DBIndexingResultStatistic(
                indexing_result_id=s.indexing_result_id,
                time=s.time,
                frames=s.frames,
                hits=s.hits,
                indexed_frames=s.indexed_frames,
                indexed_crystals=s.indexed_crystals,
            )
            for s in (await conn.execute(statement)).fetchall()
        ]

    async def update_indexing_result_status(
        self,
        conn: Connection,
        indexing_result_id: int,
        runtime_status: DBIndexingResultRuntimeStatus,
    ) -> None:
        await conn.execute(
            sa.update(self.tables.indexing_result)
            .values(
                {
                    "stream_file": None,
                    "job_id": None,
                    "job_status": DBJobStatus.QUEUED,
                    "job_error": None,
                }
                if runtime_status is None
                else {
                    "stream_file": str(runtime_status.stream_file),
                    "hit_rate": runtime_status.fom.hit_rate,
                    "indexing_rate": runtime_status.fom.indexing_rate,
                    "indexed_frames": runtime_status.fom.indexed_frames,
                    "detector_shift_x_mm": runtime_status.fom.detector_shift_x_mm,
                    "detector_shift_y_mm": runtime_status.fom.detector_shift_y_mm,
                    "job_id": runtime_status.job_id,
                    "job_status": DBJobStatus.RUNNING,
                    "job_error": None,
                }
                if isinstance(runtime_status, DBIndexingResultRunning)
                else {
                    "stream_file": str(runtime_status.stream_file),
                    "hit_rate": runtime_status.fom.hit_rate,
                    "indexing_rate": runtime_status.fom.indexing_rate,
                    "indexed_frames": runtime_status.fom.indexed_frames,
                    "detector_shift_x_mm": runtime_status.fom.detector_shift_x_mm,
                    "detector_shift_y_mm": runtime_status.fom.detector_shift_y_mm,
                    "job_id": None,
                    "job_status": DBJobStatus.DONE,
                    "job_error": runtime_status.job_error,
                }
            )
            .where(self.tables.indexing_result.c.id == indexing_result_id)
        )

    async def retrieve_indexing_result(
        self, conn: Connection, result_id: int
    ) -> None | DBIndexingResultOutput:
        ir = self.tables.indexing_result

        run_external_id = self.tables.run.c.external_id.label("run_external_id")
        r = (
            await conn.execute(
                sa.select(
                    ir.c.id,
                    ir.c.created,
                    ir.c.run_id,
                    run_external_id,
                    ir.c.stream_file,
                    ir.c.frames,
                    ir.c.hits,
                    ir.c.not_indexed_frames,
                    ir.c.hit_rate,
                    ir.c.indexing_rate,
                    ir.c.indexed_frames,
                    ir.c.detector_shift_x_mm,
                    ir.c.detector_shift_y_mm,
                    ir.c.cell_description,
                    ir.c.point_group,
                    ir.c.chemical_id,
                    ir.c.job_id,
                    ir.c.job_status,
                    ir.c.job_error,
                )
                .join(self.tables.run, self.tables.run.c.id == ir.c.run_id)
                .where(ir.c.id == result_id)
            )
        ).fetchone()

        if r is None:
            return None

        return DBIndexingResultOutput(
            id=r.id,
            created=r.created,
            run_id=RunInternalId(r.run_id),
            run_external_id=RunExternalId(r.run_external_id),
            frames=r.frames,
            hits=r.hits,
            not_indexed_frames=r.not_indexed_frames,
            cell_description=parse_cell_description(r.cell_description)
            if r.cell_description is not None
            else None,
            point_group=r.point_group,
            chemical_id=r.chemical_id,
            runtime_status=_evaluate_indexing_result_runtime_status(r),
        )

    async def create_merge_result(
        self, conn: Connection, merge_result: DBMergeResultInput
    ) -> MergeResultError | int:
        if not merge_result.indexing_results:
            return MergeResultError("list of indexing results was empty")

        point_groups = set(
            ir.point_group
            for ir in merge_result.indexing_results
            if ir.point_group is not None
        )
        cell_descriptions = set(
            ir.cell_description
            for ir in merge_result.indexing_results
            if ir.cell_description is not None
        )

        if len(point_groups) > 1:
            return MergeResultError(
                "we have more than one possible point group, namely: "
                + ", ".join(point_groups)
            )

        if len(cell_descriptions) > 1:
            return MergeResultError(
                "we have more than one possible cell description, namely: "
                + ", ".join(coparse_cell_description(cd) for cd in cell_descriptions)
            )

        if not cell_descriptions:
            return MergeResultError(
                "we need a cell description for merging, but found none"
            )

        # Explicit type necessary here, mypy will try to be too intelligent with type inference.
        mrp = merge_result.parameters
        merge_result_dict: dict[str, Any] = {
            "created": merge_result.created,
            "point_group": next(iter(point_groups), None),
            "cell_description": next(
                (coparse_cell_description(c) for c in cell_descriptions), None
            ),
            "input_merge_model": mrp.merge_model,
            "input_scale_intensities": mrp.scale_intensities,
            "input_post_refinement": mrp.post_refinement,
            "input_iterations": mrp.iterations,
            "input_polarisation_angle": int(
                mrp.polarisation.angle.to(
                    _UNIT_REGISTRY.degrees
                ).m  # pyright: ignore [reportUnknownArgumentType]
            )
            if mrp.polarisation is not None
            else None,
            "input_polarisation_percent": mrp.polarisation.percentage
            if mrp.polarisation is not None
            else None,
            "input_start_after": mrp.start_after,
            "input_stop_after": mrp.stop_after,
            "input_rel_b": mrp.rel_b,
            "input_no_pr": mrp.no_pr,
            "input_force_bandwidth": mrp.force_bandwidth,
            "input_force_radius": mrp.force_radius,
            "input_force_lambda": mrp.force_lambda,
            "input_no_delta_cc_half": mrp.no_delta_cc_half,
            "input_max_adu": mrp.max_adu,
            "input_min_measurements": mrp.min_measurements,
            "input_logs": mrp.logs,
            "input_min_res": mrp.min_res,
            "input_push_res": mrp.push_res,
            "input_w": mrp.w,
            "negative_handling": mrp.negative_handling,
        } | _runtime_status_sql_values(merge_result.runtime_status)

        mr_id = (
            await conn.execute(
                self.tables.merge_result.insert().values(merge_result_dict)
            )
        ).inserted_primary_key[0]
        for ir in merge_result.indexing_results:
            await conn.execute(
                self.tables.merge_result_has_indexing_result.insert().values(
                    merge_result_id=mr_id, indexing_result_id=ir.id
                )
            )
        if isinstance(merge_result.runtime_status, DBMergeRuntimeStatusDone):
            for shell in merge_result.runtime_status.result.detailed_foms:
                await conn.execute(
                    sa.insert(self.tables.merge_result_shell_fom).values(
                        _values_from_shell_foms(shell, merge_result_id=mr_id)
                    )
                )

        return mr_id  # type: ignore

    async def retrieve_merge_results(
        self,
        conn: Connection,
        job_status_filter: None | DBJobStatus = None,
        merge_result_id_filter: None | int = None,
        beamtime_id: None | BeamtimeId = None,
    ) -> list[DBMergeResultOutput]:
        refinement_results_by_merge_id: dict[
            int, list[DBRefinementResultOutput]
        ] = group_by(
            await self.retrieve_refinement_results(conn, beamtime_id),
            lambda rr: rr.merge_result_id,
        )

        ir = self.tables.indexing_result
        run_external_id = self.tables.run.c.external_id.label("run_external_id")
        merge_result_to_indexing_result: dict[int, list[DBIndexingResultOutput]] = {
            merge_result_id: [row[1] for row in indexing_results]
            for merge_result_id, indexing_results in group_by(
                (
                    (
                        r.merge_result_id,
                        DBIndexingResultOutput(
                            id=r.id,
                            created=r.created,
                            run_id=RunInternalId(r.run_id),
                            run_external_id=RunExternalId(r.run_external_id),
                            frames=r.frames,
                            hits=r.hits,
                            not_indexed_frames=r.not_indexed_frames,
                            cell_description=parse_cell_description(r.cell_description)
                            if r.cell_description is not None
                            else None,
                            point_group=r.point_group,
                            chemical_id=r.chemical_id,
                            runtime_status=_evaluate_indexing_result_runtime_status(r),
                        ),
                    )
                    for r in await conn.execute(
                        sa.select(
                            self.tables.merge_result_has_indexing_result.c.merge_result_id,
                            self.tables.merge_result_has_indexing_result.c.indexing_result_id,
                            ir.c.id,
                            ir.c.created,
                            ir.c.run_id,
                            run_external_id,
                            ir.c.stream_file,
                            ir.c.frames,
                            ir.c.hits,
                            ir.c.not_indexed_frames,
                            ir.c.cell_description,
                            ir.c.point_group,
                            ir.c.chemical_id,
                            ir.c.hit_rate,
                            ir.c.indexing_rate,
                            ir.c.indexed_frames,
                            ir.c.detector_shift_x_mm,
                            ir.c.detector_shift_y_mm,
                            ir.c.job_id,
                            ir.c.job_status,
                            ir.c.job_error,
                        )
                        .join(
                            self.tables.indexing_result,
                            self.tables.indexing_result.c.id
                            == self.tables.merge_result_has_indexing_result.c.indexing_result_id,
                        )
                        .join(
                            self.tables.run,
                            self.tables.run.c.id
                            == self.tables.indexing_result.c.run_id,
                        )
                        # Important to note (until we transition to the ORM): here is where we filter our merge results regarding their direct ID filter,
                        # and the beamtime ID, by joining both indexing result and run tables.
                        .where(
                            self.tables.merge_result_has_indexing_result.c.merge_result_id
                            == merge_result_id_filter
                            if merge_result_id_filter is not None
                            else self.tables.run.c.beamtime_id == beamtime_id
                            if beamtime_id is not None
                            else sa.true()
                        )
                    )
                ),
                lambda row: row[0],  # type: ignore
            ).items()
        }

        def convert_merge_result_row(
            row: Any, detailed_foms: list[Any]
        ) -> DBMergeResultOutput:
            runtime_status: DBMergeRuntimeStatus
            match row.job_status:
                case DBJobStatus.QUEUED:
                    runtime_status = None
                case DBJobStatus.RUNNING:
                    runtime_status = DBMergeRuntimeStatusRunning(
                        job_id=row.job_id,
                        started=row.started,
                        recent_log=row.recent_log,
                    )
                case _:
                    if row.job_error:
                        # pylint: disable=redefined-variable-type
                        runtime_status = DBMergeRuntimeStatusError(
                            error=row.job_error,
                            started=row.started,
                            stopped=row.stopped,
                            recent_log=row.recent_log,
                        )
                    else:
                        runtime_status = DBMergeRuntimeStatusDone(
                            started=row.started,
                            stopped=row.stopped,
                            recent_log=row.recent_log,
                            result=MergeResult(
                                detailed_foms=detailed_foms,
                                refinement_results=[
                                    RefinementResult(
                                        pdb_file_id=rr.pdb_file_id,
                                        mtz_file_id=rr.mtz_file_id,
                                        r_free=rr.r_free,
                                        r_work=rr.r_work,
                                        rms_bond_angle=rr.rms_bond_angle,
                                        rms_bond_length=rr.rms_bond_length,
                                    )
                                    for rr in refinement_results_by_merge_id.get(
                                        row.id, []
                                    )
                                ],
                                mtz_file_id=row.mtz_file_id,
                                fom=MergeResultFom(
                                    snr=row.fom_snr,
                                    wilson=row.fom_wilson,
                                    ln_k=row.fom_ln_k,
                                    discarded_reflections=row.fom_discarded_reflections,
                                    one_over_d_from=row.fom_one_over_d_from,
                                    one_over_d_to=row.fom_one_over_d_to,
                                    redundancy=row.fom_redundancy,
                                    completeness=row.fom_completeness,
                                    measurements_total=row.fom_measurements_total,
                                    reflections_total=row.fom_reflections_total,
                                    reflections_possible=row.fom_reflections_possible,
                                    r_split=row.fom_r_split,
                                    r1i=row.fom_r1i,
                                    r2=row.fom_2,
                                    cc=row.fom_cc,
                                    ccstar=row.fom_ccstar,
                                    ccano=row.fom_ccano,
                                    crdano=row.fom_crdano,
                                    rano=row.fom_rano,
                                    rano_over_r_split=row.fom_rano_over_r_split,
                                    d1sig=row.fom_d1sig,
                                    d2sig=row.fom_d2sig,
                                    outer_shell=MergeResultOuterShell(
                                        resolution=row.fom_outer_resolution,
                                        ccstar=row.fom_outer_ccstar,
                                        r_split=row.fom_outer_r_split,
                                        cc=row.fom_outer_cc,
                                        unique_reflections=row.fom_outer_unique_reflections,
                                        completeness=row.fom_outer_completeness,
                                        redundancy=row.fom_outer_redundancy,
                                        snr=row.fom_outer_snr,
                                        min_res=row.fom_outer_min_res,
                                        max_res=row.fom_outer_max_res,
                                    ),
                                ),
                            ),
                        )

            return DBMergeResultOutput(
                id=row.id,
                created=row.created,
                indexing_results=merge_result_to_indexing_result.get(row.id, []),
                parameters=DBMergeParameters(
                    point_group=row.point_group,
                    cell_description=cast(
                        CrystFELCellFile,
                        parse_cell_description(row.cell_description),
                    ),
                    negative_handling=row.negative_handling,
                    merge_model=MergeModel(row.input_merge_model),
                    scale_intensities=ScaleIntensities(row.input_scale_intensities),
                    post_refinement=row.input_post_refinement,
                    iterations=row.input_iterations,
                    polarisation=Polarisation(
                        angle=row.input_polarisation_angle
                        * _UNIT_REGISTRY.degrees,  # pyright: ignore [reportUnknownArgumentType]
                        percentage=row.input_polarisation_percent,
                    )
                    if row.input_polarisation_angle is not None
                    and row.input_polarisation_percent is not None
                    else None,
                    start_after=row.input_start_after,
                    stop_after=row.input_stop_after,
                    rel_b=row.input_rel_b,
                    no_pr=row.input_no_pr,
                    force_bandwidth=row.input_force_bandwidth,
                    force_radius=row.input_force_radius,
                    force_lambda=row.input_force_lambda,
                    no_delta_cc_half=row.input_no_delta_cc_half,
                    max_adu=row.input_max_adu,
                    min_measurements=row.input_min_measurements,
                    logs=row.input_logs,
                    min_res=row.input_min_res,
                    push_res=row.input_push_res,
                    w=row.input_w,
                ),
                runtime_status=runtime_status,
            )

        mr = self.tables.merge_result.c
        mdfoms = self.tables.merge_result_shell_fom.c

        merge_results = await conn.execute(
            sa.select(
                mr.id,
                mr.created,
                mr.recent_log,
                mr.negative_handling,
                mr.job_status,
                mr.started,
                mr.stopped,
                mr.point_group,
                mr.cell_description,
                mr.job_id,
                mr.job_error,
                mr.mtz_file_id,
                mr.input_merge_model,
                mr.input_scale_intensities,
                mr.input_post_refinement,
                mr.input_iterations,
                mr.input_polarisation_angle,
                mr.input_polarisation_percent,
                mr.input_start_after,
                mr.input_stop_after,
                mr.input_rel_b,
                mr.input_no_pr,
                mr.input_force_bandwidth,
                mr.input_force_radius,
                mr.input_force_lambda,
                mr.input_no_delta_cc_half,
                mr.input_max_adu,
                mr.input_min_measurements,
                mr.input_logs,
                mr.input_min_res,
                mr.input_push_res,
                mr.input_w,
                mr.fom_snr,
                mr.fom_wilson,
                mr.fom_ln_k,
                mr.fom_discarded_reflections,
                mr.fom_one_over_d_from,
                mr.fom_one_over_d_to,
                mr.fom_redundancy,
                mr.fom_completeness,
                mr.fom_measurements_total,
                mr.fom_reflections_total,
                mr.fom_reflections_possible,
                mr.fom_r_split,
                mr.fom_r1i,
                mr.fom_2,
                mr.fom_cc,
                mr.fom_ccstar,
                mr.fom_ccano,
                mr.fom_crdano,
                mr.fom_rano,
                mr.fom_rano_over_r_split,
                mr.fom_d1sig,
                mr.fom_d2sig,
                mr.fom_outer_resolution,
                mr.fom_outer_ccstar,
                mr.fom_outer_r_split,
                mr.fom_outer_cc,
                mr.fom_outer_unique_reflections,
                mr.fom_outer_completeness,
                mr.fom_outer_redundancy,
                mr.fom_outer_snr,
                mr.fom_outer_min_res,
                mr.fom_outer_max_res,
            ).where(
                sa.and_(
                    mr.job_status == job_status_filter  # pyright: ignore
                    if job_status_filter is not None
                    else True,
                    mr.id.in_(merge_result_to_indexing_result.keys()),
                )
            )
        )

        async def fetch_shell_foms(merge_result_id: int) -> list[MergeResultShell]:
            merge_result_shells = await conn.execute(
                sa.select(
                    mdfoms.one_over_d_centre,
                    mdfoms.nref,
                    mdfoms.d_over_a,
                    mdfoms.min_res,
                    mdfoms.max_res,
                    mdfoms.cc,
                    mdfoms.ccstar,
                    mdfoms.r_split,
                    mdfoms.reflections_possible,
                    mdfoms.completeness,
                    mdfoms.measurements,
                    mdfoms.redundancy,
                    mdfoms.snr,
                    mdfoms.mean_i,
                ).where(mdfoms.merge_result_id == merge_result_id)
            )
            return [
                MergeResultShell(
                    one_over_d_centre=s.one_over_d_centre,
                    nref=s.nref,
                    d_over_a=s.d_over_a,
                    min_res=s.min_res,
                    max_res=s.max_res,
                    cc=s.cc,
                    ccstar=s.ccstar,
                    r_split=s.r_split,
                    reflections_possible=s.reflections_possible,
                    completeness=s.completeness,
                    measurements=s.measurements,
                    redundancy=s.redundancy,
                    snr=s.snr,
                    mean_i=s.mean_i,
                )
                for s in merge_result_shells
            ]

        return [
            convert_merge_result_row(
                row=row, detailed_foms=await fetch_shell_foms(row.id)
            )
            for row in merge_results
        ]

    async def update_merge_result_status(
        self,
        conn: Connection,
        merge_result_id: int,
        runtime_status: DBMergeRuntimeStatus,
    ) -> None:
        await conn.execute(
            sa.update(self.tables.merge_result)
            .values(_runtime_status_sql_values(runtime_status))
            .where(self.tables.merge_result.c.id == merge_result_id)
        )

        if isinstance(runtime_status, DBMergeRuntimeStatusDone):
            detailed_foms = runtime_status.result.detailed_foms
            assert detailed_foms is not None
            for shell in detailed_foms:
                await conn.execute(
                    sa.insert(self.tables.merge_result_shell_fom).values(
                        _values_from_shell_foms(shell, merge_result_id=merge_result_id)
                    )
                )
