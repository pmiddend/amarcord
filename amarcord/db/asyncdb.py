import datetime
import itertools
import os
import re
from dataclasses import dataclass
from pathlib import Path
from typing import List, cast, Tuple, Dict, Iterable, Any
from typing import Optional

import magic
import sqlalchemy as sa

from amarcord.db.analysis_result import DBCFELAnalysisResult
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi import attributo_type_to_schema
from amarcord.db.attributi import schema_json_to_attributo_type
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import (
    AttributoType,
    AttributoTypeSample,
    AttributoTypeDecimal,
)
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.constants import ATTRIBUTO_NAME_REGEX
from amarcord.db.data_set import DBDataSet
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.dbcontext import Connection
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.experiment_type import DBExperimentType
from amarcord.db.indexing_job import DBIndexingJob
from amarcord.db.job_status import DBJobStatus
from amarcord.db.run_group import RunGroup
from amarcord.db.table_classes import DBSample, DBFile, DBRun, DBEvent
from amarcord.db.tables import DBTables
from amarcord.pint_util import valid_pint_unit
from amarcord.util import sha256_file


@dataclass(frozen=True)
class CreateFileResult:
    id: int
    type_: str


def create_run_groups(
    attributi_names: Iterable[str], runs: List[DBRun]
) -> List[RunGroup]:
    def run_duration(run_: DBRun) -> datetime.timedelta:
        stopped = run_.attributi.select_datetime("stopped")
        started = run_.attributi.select_datetime("started")
        return (
            datetime.timedelta()
            if stopped is None or started is None
            else stopped - started
        )

    groups: List[RunGroup] = []
    # Try to fit each run into a group
    for run in runs:
        # Fill this run's attributi value combination
        attributi_values: Dict[AttributoId, AttributoValue] = {}
        for a in attributi_names:
            attributi_values[a] = run.attributi.select(a)

        this_run_minutes = int(run_duration(run).total_seconds() / 60)

        # Try to find its group
        found = False
        for group in groups:
            if attributi_values == group.attributi_values:
                group.run_ids.append(run.id)
                group.total_minutes += this_run_minutes
                found = True
                break

        if not found:
            groups.append(
                RunGroup(
                    run_ids=[run.id],
                    attributi_values=attributi_values,
                    total_minutes=this_run_minutes,
                )
            )

    return groups


class AsyncDB:
    def __init__(self, dbcontext: AsyncDBContext, tables: DBTables) -> None:
        self.dbcontext = dbcontext
        self.tables = tables

    def connect(self) -> Connection:
        return self.dbcontext.connect()

    def begin(self) -> Connection:
        return self.dbcontext.begin()

    async def dispose(self) -> None:
        await self.dbcontext.dispose()

    async def retrieve_file(
        self, conn: Connection, file_id: int
    ) -> Tuple[str, str, bytes, int]:
        result = await conn.execute(
            sa.select(
                [
                    self.tables.file.c.file_name,
                    self.tables.file.c.type,
                    self.tables.file.c.contents,
                    self.tables.file.c.size_in_bytes,
                ]
            ).where(self.tables.file.c.id == file_id)
        )

        return result.fetchone()

    async def retrieve_attributi(
        self, conn: Connection, associated_table: Optional[AssociatedTable]
    ) -> List[DBAttributo]:
        ac = self.tables.attributo.c
        select_stmt = sa.select(
            [
                ac.name,
                ac.description,
                ac.group,
                ac.json_schema,
                ac.associated_table,
            ]
        ).order_by(ac.associated_table)

        if associated_table is not None:
            select_stmt = select_stmt.where(ac.associated_table == associated_table)

        result = await conn.execute(select_stmt)
        return [
            DBAttributo(
                name=AttributoId(a["name"]),
                description=a["description"],
                group=a["group"],
                associated_table=a["associated_table"],
                attributo_type=schema_json_to_attributo_type(a["json_schema"]),
            )
            for a in result
        ]

    async def create_sample(
        self,
        conn: Connection,
        name: str,
        attributi: AttributiMap,
    ) -> int:
        return (
            await conn.execute(
                self.tables.sample.insert().values(
                    name=name,
                    modified=datetime.datetime.utcnow(),
                    attributi=attributi.to_json(),
                )
            )
        ).inserted_primary_key[0]

    async def update_sample(
        self,
        conn: Connection,
        id_: int,
        name: str,
        attributi: AttributiMap,
    ) -> None:
        await conn.execute(
            sa.update(self.tables.sample)
            .values(
                name=name,
                modified=datetime.datetime.utcnow(),
                attributi=attributi.to_json(),
            )
            .where(self.tables.sample.c.id == id_)
        )

    async def _retrieve_files(
        self,
        conn: Connection,
        association_column: sa.Column,
        where_clause: Optional[Any] = None,
    ) -> Dict[int, List[DBFile]]:
        select_stmt = (
            sa.select(
                [
                    association_column,
                    self.tables.file.c.id,
                    self.tables.file.c.description,
                    self.tables.file.c.file_name,
                    self.tables.file.c.type,
                    self.tables.file.c.size_in_bytes,
                ]
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

        result: Dict[int, List[DBFile]] = {}

        for key, group in itertools.groupby(
            file_results, key=lambda row: row[association_column.name]
        ):
            result[key] = [
                DBFile(
                    id=row["id"],
                    description=row["description"],
                    type_=row["type"],
                    file_name=row["file_name"],
                    size_in_bytes=row["size_in_bytes"],
                )
                for row in group
            ]

        return result

    async def retrieve_samples(
        self, conn: Connection, attributi_iterable: Iterable[DBAttributo]
    ) -> Iterable[DBSample]:
        sample_to_files = await self._retrieve_files(
            conn, self.tables.sample_has_file.c.sample_id
        )

        select_stmt = sa.select(
            [
                self.tables.sample.c.id,
                self.tables.sample.c.name,
                self.tables.sample.c.attributi,
            ]
        ).order_by(self.tables.sample.c.name)

        result = await conn.execute(select_stmt)

        attributi = list(attributi_iterable)
        return (
            DBSample(
                id=a["id"],
                name=a["name"],
                attributi=AttributiMap.from_types_and_json(
                    types=attributi,
                    # Sample IDs not needed since samples cannot refer to themselves (yet!)
                    sample_ids=[],
                    raw_attributi=a["attributi"],
                ),
                files=sample_to_files.get(a["id"], []),
            )
            for a in result
        )

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

    async def retrieve_events(self, conn: Connection) -> List[DBEvent]:
        ec = self.tables.event_log.c
        return [
            DBEvent(row["id"], row["created"], row["level"], row["source"], row["text"])
            for row in await conn.execute(
                sa.select([ec.id, ec.created, ec.level, ec.source, ec.text]).order_by(
                    ec.created.desc()
                )
            )
        ]

    async def create_event(
        self, conn: Connection, level: EventLogLevel, source: str, text: str
    ) -> int:
        return (
            await conn.execute(
                self.tables.event_log.insert().values(
                    created=datetime.datetime.utcnow(),
                    level=level,
                    source=source,
                    text=text,
                )
            )
        ).inserted_primary_key[0]

    async def retrieve_runs(
        self, conn: Connection, attributi_iterable: Iterable[DBAttributo]
    ) -> List[DBRun]:
        run_to_files = await self._retrieve_files(
            conn, self.tables.run_has_file.c.run_id
        )

        select_stmt = sa.select(
            [
                self.tables.run.c.id,
                self.tables.run.c.attributi,
            ]
        ).order_by(self.tables.run.c.id.desc())

        result = await conn.execute(select_stmt)

        sample_ids = await self.retrieve_sample_ids(conn)

        attributi = list(attributi_iterable)
        return [
            DBRun(
                id=a["id"],
                attributi=AttributiMap.from_types_and_json(
                    types=attributi,
                    sample_ids=sample_ids,
                    raw_attributi=a["attributi"],
                ),
                files=run_to_files.get(a["id"], []),
            )
            for a in result
        ]

    async def delete_sample(
        self, conn: Connection, id_: int, delete_in_runs: bool
    ) -> None:
        attributi = await self.retrieve_attributi(conn, AssociatedTable.RUN)

        # check if we have a sample attributo in runs and then do integrity checking
        sample_attributi = [
            x for x in attributi if isinstance(x.attributo_type, AttributoTypeSample)
        ]

        if sample_attributi:
            for r in await self.retrieve_runs(conn, attributi):
                run_attributi = r.attributi
                changed = False
                for sample_attributo in sample_attributi:
                    run_sample = r.attributi.select_sample_id(sample_attributo.name)
                    if run_sample == id_:
                        if delete_in_runs:
                            run_attributi.remove(sample_attributo.name)
                            changed = True
                        else:
                            raise Exception(f"run {r.id} still has sample {id_}")
                if changed:
                    await self.update_run_attributi(conn, r.id, run_attributi)

        await conn.execute(
            sa.delete(self.tables.sample).where(self.tables.sample.c.id == id_)
        )

    async def create_attributo(
        self,
        conn: Connection,
        name: str,
        description: str,
        group: str,
        associated_table: AssociatedTable,
        type_: AttributoType,
    ) -> None:
        if not re.fullmatch(ATTRIBUTO_NAME_REGEX, name, re.IGNORECASE):
            raise ValueError(
                f'attributo name "{name}" contains invalid characters (maybe a number at the beginning '
                f"or a dash?)"
            )
        if associated_table == AssociatedTable.SAMPLE and isinstance(
            type_, AttributoTypeSample
        ):
            raise ValueError("Samples can't have attributi of type sample")
        if isinstance(type_, AttributoTypeDecimal) and type_.standard_unit:
            if type_.suffix is None:
                raise ValueError("Got a standard unit, but no suffix")
            if not valid_pint_unit(type_.suffix):
                raise ValueError(f"Unit {type_.suffix} not a valid unit")
        await conn.execute(
            self.tables.attributo.insert().values(
                name=name,
                description=description,
                associated_table=associated_table,
                group=group,
                json_schema=attributo_type_to_schema(type_),
            )
        )

    async def delete_attributo(
        self,
        conn: Connection,
        name: str,
    ) -> None:
        attributi = await self.retrieve_attributi(conn, associated_table=None)

        found_attributo = next((x for x in attributi if x.name == name), None)
        if found_attributo is None:
            raise Exception(f'couldn\'t find attributo "{name}"')

        await conn.execute(
            sa.delete(self.tables.attributo).where(self.tables.attributo.c.name == name)
        )

        if found_attributo.associated_table == AssociatedTable.SAMPLE:
            # This is the tricky bit: we need to retrieve the samples with the old attributi list. The samples haven't
            # been converted to the new format, so using the new attributi list would make that fail validation.
            for s in await self.retrieve_samples(conn, attributi):
                # Then remove the attributo from the sample and the accompanying types, and update.
                s.attributi.remove(name)
                await self.update_sample(conn, cast(int, s.id), s.name, s.attributi)
        elif found_attributo.associated_table == AssociatedTable.RUN:
            # Explanation, see above for samples
            for r in await self.retrieve_runs(conn, attributi):
                r.attributi.remove(name)
                await self.update_run_attributi(conn, r.id, r.attributi)
        else:
            raise Exception(
                f"unimplemented: is there a new associated table {found_attributo.associated_table}?"
            )

    async def create_file(
        self,
        conn: Connection,
        file_name: str,
        description: str,
        contents_location: Path,
    ) -> CreateFileResult:
        mime = magic.from_file(str(contents_location), mime=True)

        sha256 = sha256_file(contents_location)
        with contents_location.open("rb") as f:
            old_file_position = f.tell()
            f.seek(0, os.SEEK_END)
            size_in_bytes = f.tell()
            f.seek(old_file_position, os.SEEK_SET)

            return CreateFileResult(
                id=(
                    await conn.execute(
                        self.tables.file.insert().values(
                            type=mime,
                            modified=datetime.datetime.utcnow(),
                            # FIXME: Don't load the whole thing into memory ffs
                            contents=f.read(),
                            file_name=file_name,
                            description=description,
                            sha256=sha256,
                            size_in_bytes=size_in_bytes,
                        )
                    )
                ).inserted_primary_key[0],
                type_=mime,
            )

    async def update_attributo(
        self,
        conn: Connection,
        name: str,
        # pylint: disable=unused-argument
        conversion_flags: AttributoConversionFlags,
        new_attributo: DBAttributo,
    ) -> None:
        current_attributi = list(
            await self.retrieve_attributi(conn, associated_table=None)
        )

        current_attributo = next((x for x in current_attributi if x.name == name), None)

        if current_attributo is None:
            raise Exception(
                f"couldn't find attributo for table {new_attributo.associated_table} and name {name}"
            )

        existing_attributo = next(
            (a for a in current_attributi if a.name == new_attributo.name), None
        )
        if new_attributo.name != name and existing_attributo is not None:
            raise Exception(
                f"cannot rename {name} to {new_attributo.name} because we already have an attributo of that "
                "name"
            )

        # first, change the attributo itself, then its actual values (if possible)
        await conn.execute(
            sa.update(self.tables.attributo)
            .values(
                name=new_attributo.name,
                description=new_attributo.description,
                group=new_attributo.group,
                json_schema=attributo_type_to_schema(new_attributo.attributo_type),
            )
            .where(self.tables.attributo.c.name == name)
        )

        if new_attributo.associated_table == AssociatedTable.SAMPLE:
            for s in await self.retrieve_samples(conn, current_attributi):
                s.attributi.convert_attributo(
                    conversion_flags=conversion_flags,
                    old_name=name,
                    new_name=new_attributo.name,
                    after_type=new_attributo.attributo_type,
                )
                await self.update_sample(conn, cast(int, s.id), s.name, s.attributi)
        elif new_attributo.associated_table == AssociatedTable.RUN:
            for r in await self.retrieve_runs(conn, current_attributi):
                r.attributi.convert_attributo(
                    conversion_flags=conversion_flags,
                    old_name=name,
                    new_name=new_attributo.name,
                    after_type=new_attributo.attributo_type,
                )
                await self.update_run_attributi(conn, r.id, r.attributi)
        else:
            raise Exception(
                f"unimplemented: is there a new associated table {new_attributo.associated_table}?"
            )

        sample_ids = await self.retrieve_sample_ids(conn)
        for ds in await self.retrieve_data_sets(conn, sample_ids, current_attributi):
            ds.attributi.convert_attributo(
                conversion_flags=conversion_flags,
                old_name=name,
                new_name=new_attributo.name,
                after_type=new_attributo.attributo_type,
            )
            await self.update_data_set_attributi(conn, ds.id, ds.attributi)

    async def add_file_to_sample(
        self, conn: Connection, file_id: int, sample_id: int
    ) -> None:
        await conn.execute(
            sa.insert(self.tables.sample_has_file).values(
                file_id=file_id, sample_id=sample_id
            )
        )

    async def remove_files_from_sample(self, conn: Connection, sample_id: int) -> None:
        await conn.execute(
            sa.delete(self.tables.sample_has_file).where(
                self.tables.sample_has_file.c.sample_id == sample_id
            )
        )

    async def retrieve_run_ids(self, conn: Connection) -> List[int]:
        return [
            row[0]
            for row in conn.execute(
                sa.select([self.tables.run.c.id]).order_by(self.tables.run.c.id)
            ).fetchall()
        ]

    async def clear_cfel_analysis_results(
        self, conn: Connection, delete_after_run_id: Optional[int] = None
    ) -> None:
        if delete_after_run_id is None:
            conn.execute(sa.delete(self.tables.cfel_analysis_results))
        else:
            conn.execute(
                sa.delete(self.tables.cfel_analysis_results).where(
                    self.tables.cfel_analysis_results.c.run_from > delete_after_run_id
                )
            )

    async def create_cfel_analysis_result(
        self, conn: Connection, r: DBCFELAnalysisResult
    ) -> None:
        await conn.execute(
            sa.insert(self.tables.cfel_analysis_results).values(
                directory_name=r.directory_name,
                run_from=r.run_from,
                run_to=r.run_to,
                resolution=r.resolution,
                rsplit=r.rsplit,
                cchalf=r.cchalf,
                ccstar=r.ccstar,
                snr=r.snr,
                completeness=r.completeness,
                multiplicity=r.multiplicity,
                total_measurements=r.total_measurements,
                unique_reflections=r.unique_reflections,
                wilson_b=r.wilson_b,
                outer_shell=r.outer_shell,
                num_patterns=r.num_patterns,
                num_hits=r.num_hits,
                indexed_patterns=r.indexed_patterns,
                indexed_crystals=r.indexed_crystals,
                comment=r.comment,
            )
        )

    async def retrieve_cfel_analysis_results(
        self, conn: Connection
    ) -> Iterable[DBCFELAnalysisResult]:
        ar = self.tables.cfel_analysis_results.c
        return (
            DBCFELAnalysisResult(
                r["directory_name"],
                r["run_from"],
                r["run_to"],
                r["resolution"],
                r["rsplit"],
                r["cchalf"],
                r["ccstar"],
                r["snr"],
                r["completeness"],
                r["multiplicity"],
                r["total_measurements"],
                r["unique_reflections"],
                r["wilson_b"],
                r["outer_shell"],
                r["num_patterns"],
                r["num_hits"],
                r["indexed_patterns"],
                r["indexed_crystals"],
                r["comment"],
            )
            for r in await (
                conn.execute(
                    sa.select(
                        [
                            ar.directory_name,
                            ar.run_from,
                            ar.run_to,
                            ar.resolution,
                            ar.rsplit,
                            ar.cchalf,
                            ar.ccstar,
                            ar.snr,
                            ar.completeness,
                            ar.multiplicity,
                            ar.total_measurements,
                            ar.unique_reflections,
                            ar.wilson_b,
                            ar.outer_shell,
                            ar.num_patterns,
                            ar.num_hits,
                            ar.indexed_patterns,
                            ar.indexed_crystals,
                            ar.comment,
                        ]
                    )
                )
            )
        )

    async def create_run(
        self, conn: Connection, run_id: int, attributi: AttributiMap
    ) -> None:
        await conn.execute(
            sa.insert(self.tables.run).values(
                id=run_id,
                attributi=attributi.to_json(),
                modified=datetime.datetime.utcnow(),
            )
        )

    async def retrieve_indexing_jobs(
        self, conn: Connection, statuses: List[DBJobStatus]
    ) -> List[DBIndexingJob]:
        ij = self.tables.indexing_jobs
        select_stmt = sa.select(
            [
                ij.c.id,
                ij.c.run_Id,
                ij.c.status,
                ij.c.started,
                ij.c.stopped,
                ij.c.metadata,
            ]
        )
        if statuses:
            select_stmt.where(ij.c.status.in_(statuses))
        return [
            DBIndexingJob(
                id=row[0],
                run_id=row[1],
                status=row[3],
                started_utc=row[4],
                stopped_utc=row[5],
                metadata=row[6],
            )
            for row in await conn.execute(select_stmt)
        ]

    async def retrieve_runs_without_indexing_jobs(self, conn: Connection) -> List[int]:
        runs = self.tables.run.alias("runs")

        return [
            row[0]
            for row in await conn.execute(
                sa.select([runs.c.id])
                .select_from(runs)
                .where(
                    ~(
                        sa.select([self.tables.indexing_jobs.c.id])
                        .where(
                            (self.tables.indexing_jobs.c.run_id == runs.c.run_id)
                            & (
                                self.tables.indexing_jobs.c.status.in_(
                                    [DBJobStatus.RUNNING, DBJobStatus.SUCCESS]
                                )
                            )
                        )
                        .exists()
                    )
                )
            )
        ]

    async def retrieve_latest_run(
        self, conn: Connection, attributi: List[DBAttributo]
    ) -> Optional[DBRun]:
        maximum_id = (
            await conn.execute(sa.select([sa.func.max(self.tables.run.c.id)]))
        ).fetchone()

        if maximum_id is None:
            return None

        return await self.retrieve_run(conn, maximum_id[0], attributi)

    async def retrieve_run(
        self, conn: Connection, id_: int, attributi: List[DBAttributo]
    ) -> Optional[DBRun]:
        rc = self.tables.run.c
        r = (
            await conn.execute(sa.select([rc.id, rc.attributi]).where(rc.id == id_))
        ).fetchone()
        files = await self._retrieve_files(
            conn,
            self.tables.run_has_file.c.run_id,
            (self.tables.run_has_file.c.run_id == id_),
        )
        if r is None:
            return None
        return DBRun(
            id=id_,
            attributi=AttributiMap.from_types_and_json(
                attributi, await self.retrieve_sample_ids(conn), r["attributi"]
            ),
            files=files.get(id_, []),
        )

    async def update_run_attributi(
        self, conn: Connection, id_: int, attributi: AttributiMap
    ) -> None:
        await conn.execute(
            sa.update(self.tables.run)
            .values(attributi=attributi.to_json())
            .where(self.tables.run.c.id == id_)
        )

    async def retrieve_sample_ids(self, conn: Connection) -> List[int]:
        return [r[0] for r in await conn.execute(sa.select([self.tables.sample.c.id]))]

    async def clear_analysis_results(
        self, conn: Connection, delete_after_run_id: Optional[int] = None
    ) -> None:
        if delete_after_run_id is None:
            await conn.execute(sa.delete(self.tables.cfel_analysis_results))
        else:
            await conn.execute(
                sa.delete(self.tables.cfel_analysis_results).where(
                    self.tables.cfel_analysis_results.c.run_from > delete_after_run_id
                )
            )

    async def add_analysis_result(
        self, conn: Connection, r: DBCFELAnalysisResult
    ) -> None:
        await conn.execute(
            sa.insert(self.tables.cfel_analysis_results).values(
                directory_name=r.directory_name,
                run_from=r.run_from,
                run_to=r.run_to,
                resolution=r.resolution,
                rsplit=r.rsplit,
                cchalf=r.cchalf,
                ccstar=r.ccstar,
                snr=r.snr,
                completeness=r.completeness,
                multiplicity=r.multiplicity,
                total_measurements=r.total_measurements,
                unique_reflections=r.unique_reflections,
                wilson_b=r.wilson_b,
                outer_shell=r.outer_shell,
                num_patterns=r.num_patterns,
                num_hits=r.num_hits,
                indexed_patterns=r.indexed_patterns,
                indexed_crystals=r.indexed_crystals,
                comment=r.comment,
            )
        )

    async def retrieve_analysis_results(
        self, conn: Connection
    ) -> Iterable[DBCFELAnalysisResult]:
        ar = self.tables.cfel_analysis_results.c
        return (
            DBCFELAnalysisResult(
                r["directory_name"],
                r["run_from"],
                r["run_to"],
                r["resolution"],
                r["rsplit"],
                r["cchalf"],
                r["ccstar"],
                r["snr"],
                r["completeness"],
                r["multiplicity"],
                r["total_measurements"],
                r["unique_reflections"],
                r["wilson_b"],
                r["outer_shell"],
                r["num_patterns"],
                r["num_hits"],
                r["indexed_patterns"],
                r["indexed_crystals"],
                r["comment"],
            )
            for r in (
                await conn.execute(
                    sa.select(
                        [
                            ar.directory_name,
                            ar.run_from,
                            ar.run_to,
                            ar.resolution,
                            ar.rsplit,
                            ar.cchalf,
                            ar.ccstar,
                            ar.snr,
                            ar.completeness,
                            ar.multiplicity,
                            ar.total_measurements,
                            ar.unique_reflections,
                            ar.wilson_b,
                            ar.outer_shell,
                            ar.num_patterns,
                            ar.num_hits,
                            ar.indexed_patterns,
                            ar.indexed_crystals,
                            ar.comment,
                        ]
                    )
                )
            ).fetchall()
        )

    async def create_experiment_type(
        self, conn: Connection, name: str, experiment_attributi_names: Iterable[str]
    ) -> None:
        existing_attributi_names = {
            a.name for a in await self.retrieve_attributi(conn, associated_table=None)
        }

        not_found = [
            a for a in experiment_attributi_names if a not in existing_attributi_names
        ]

        if not_found:
            raise Exception(
                "couldn't find the following attributi: "
                + ", ".join(experiment_attributi_names)
            )

        await conn.execute(
            self.tables.experiment_has_attributo.insert().values(
                [
                    {"experiment_type": name, "attributo_name": a}
                    for a in experiment_attributi_names
                ]
            )
        )

    async def delete_experiment_type(self, conn: Connection, name: str) -> None:
        await conn.execute(
            self.tables.experiment_has_attributo.delete().where(
                self.tables.experiment_has_attributo.c.experiment_type == name
            )
        )

    async def retrieve_experiment_types(
        self, conn: Connection
    ) -> Iterable[DBExperimentType]:
        result: List[DBExperimentType] = []
        etc = self.tables.experiment_has_attributo.c
        for key, group in itertools.groupby(
            await conn.execute(
                sa.select([etc.experiment_type, etc.attributo_name]).order_by(
                    etc.experiment_type
                )
            ),
            key=lambda row: row["experiment_type"],
        ):
            result.append(
                DBExperimentType(key, [row["attributo_name"] for row in group])
            )
        return result

    async def create_data_set(
        self, conn: Connection, experiment_type: str, attributi: AttributiMap
    ) -> int:
        matching_experiment_type: Optional[DBExperimentType] = next(
            (
                x
                for x in await self.retrieve_experiment_types(conn)
                if x.name == experiment_type
            ),
            None,
        )
        if matching_experiment_type is None:
            raise Exception(
                f'couldn\'t find experiment type named "{matching_experiment_type}"'
            )

        existing_attributo_names = matching_experiment_type.attributo_names

        superfluous_attributi = attributi.names().difference(existing_attributo_names)

        if superfluous_attributi:
            raise Exception(
                "the following attributi are not part of the data set definition: "
                + ", ".join(superfluous_attributi)
            )

        return (
            await conn.execute(
                self.tables.data_set.insert().values(
                    experiment_type=experiment_type,
                    attributi=attributi.to_json(),
                )
            )
        ).inserted_primary_key[0]

    async def delete_data_set(self, conn: Connection, id_: int) -> None:
        await conn.execute(
            self.tables.data_set.delete().where(self.tables.data_set.c.id == id_)
        )

    async def retrieve_data_sets(
        self,
        conn: Connection,
        sample_ids: List[int],
        iterable_attributi: Iterable[DBAttributo],
    ) -> Iterable[DBDataSet]:
        dc = self.tables.data_set.c
        attributi = list(iterable_attributi)
        return (
            DBDataSet(
                id=r["id"],
                experiment_type=r["experiment_type"],
                attributi=AttributiMap.from_types_and_json(
                    attributi, sample_ids=sample_ids, raw_attributi=r["attributi"]
                ),
            )
            for r in await conn.execute(
                sa.select([dc.id, dc.experiment_type, dc.attributi])
            )
        )

    async def update_data_set_attributi(
        self, conn: Connection, id_: int, attributi: AttributiMap
    ) -> None:
        dc = self.tables.data_set.c
        await conn.execute(
            sa.update(self.tables.data_set)
            .values(attributi=attributi.to_json())
            .where(dc.id == id_)
        )
