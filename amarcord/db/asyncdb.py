import datetime
import itertools
import logging
import os
from dataclasses import dataclass
from pathlib import Path
from typing import List, cast, Tuple, Dict, Iterable, Any, Set
from typing import Optional

import magic
import sqlalchemy as sa
from openpyxl import Workbook

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext, Connection
from amarcord.db.attributi import (
    AttributoConversionFlags,
    ATTRIBUTO_STOPPED,
    ATTRIBUTO_STARTED,
    attributo_type_to_string,
    attributo_sort_key,
)
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
from amarcord.db.cfel_analysis_result import DBCFELAnalysisResult
from amarcord.db.data_set import DBDataSet
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.experiment_type import DBExperimentType
from amarcord.db.migrations.alembic_utilities import upgrade_to_head_connection
from amarcord.db.table_classes import DBSample, DBFile, DBRun, DBEvent, DBFileBlueprint
from amarcord.db.tables import DBTables
from amarcord.pint_util import valid_pint_unit
from amarcord.util import sha256_file, group_by, datetime_to_local

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class CreateFileResult:
    id: int
    type_: str
    size_in_bytes: int


ATTRIBUTO_GROUP_MANUAL = "manual"


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
                    self.tables.file.c.original_path,
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
                    original_path=row["original_path"],
                )
                for row in group
            ]

        return result

    async def retrieve_samples(
        self, conn: Connection, attributi: List[DBAttributo]
    ) -> List[DBSample]:
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

        return [
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

    async def retrieve_events(self, conn: Connection) -> List[DBEvent]:
        ec = self.tables.event_log.c
        event_to_files = await self._retrieve_files(
            conn, self.tables.event_has_file.c.event_id
        )
        return [
            DBEvent(
                row["id"],
                row["created"],
                row["level"],
                row["source"],
                row["text"],
                files=event_to_files.get(row["id"], []),
            )
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
        self, conn: Connection, attributi: List[DBAttributo]
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
        self, conn: Connection, id_: int, delete_in_dependencies: bool
    ) -> None:
        attributi = await self.retrieve_attributi(conn, AssociatedTable.RUN)

        sample_ids = await self.retrieve_sample_ids(conn)

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
                        if delete_in_dependencies:
                            run_attributi.remove_with_type(sample_attributo.name)
                            changed = True
                        else:
                            raise Exception(f"run {r.id} still has sample {id_}")
                if changed:
                    await self.update_run_attributi(conn, r.id, run_attributi)

            for ds in await self.retrieve_data_sets(
                conn, sample_ids=sample_ids, attributi=attributi
            ):
                changed = False
                for sample_attributo in sample_attributi:
                    ds_sample = ds.attributi.select_sample_id(sample_attributo.name)
                    if ds_sample == id_:
                        if delete_in_dependencies:
                            ds.attributi.remove_with_type(sample_attributo.name)
                            changed = True
                        else:
                            raise Exception(f"data set {ds.id} still has sample {id_}")
                if changed:
                    await self.delete_data_set(conn, ds.id)

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
        # I honestly don't remember why I added this
        # if not re.fullmatch(ATTRIBUTO_NAME_REGEX, name, re.IGNORECASE):
        #     raise ValueError(
        #         f'attributo name "{name}" contains invalid characters (maybe a number at the beginning '
        #         f"or a dash?)"
        #     )
        if associated_table == AssociatedTable.SAMPLE and isinstance(
            type_, AttributoTypeSample
        ):
            raise ValueError(f"{name}: Samples can't have attributi of type sample")
        if isinstance(type_, AttributoTypeDecimal) and type_.standard_unit:
            if type_.suffix is None:
                raise ValueError(f"{name}: got a standard unit, but no suffix")
            if not valid_pint_unit(type_.suffix):
                raise ValueError(f"{name}: unit {type_.suffix} not a valid unit")
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
        name: AttributoId,
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
                s.attributi.remove_with_type(name)
                await self.update_sample(conn, cast(int, s.id), s.name, s.attributi)
        elif found_attributo.associated_table == AssociatedTable.RUN:
            # Explanation, see above for samples
            for r in await self.retrieve_runs(conn, attributi):
                r.attributi.remove_with_type(name)
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
        original_path: Optional[Path],
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
                    existing_file["id"],
                    existing_file["type"],
                    existing_file["size_in_bytes"],
                )

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
        name: AttributoId,
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

    async def add_file_to_event(
        self, conn: Connection, file_id: int, event_id: int
    ) -> None:
        await conn.execute(
            sa.insert(self.tables.event_has_file).values(
                file_id=file_id, event_id=event_id
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

    async def clear_cfel_analysis_results(self, conn: Connection) -> None:
        await conn.execute(sa.delete(self.tables.cfel_analysis_results))

    async def create_cfel_analysis_result(
        self, conn: Connection, r: DBCFELAnalysisResult, files: List[DBFileBlueprint]
    ) -> None:
        file_ids = [
            (
                await self.create_file(
                    conn,
                    f.location.name,
                    f.description,
                    contents_location=f.location,
                    original_path=f.location.resolve(),
                    deduplicate=True,
                )
            ).id
            for f in files
        ]
        result_id = (
            await conn.execute(
                sa.insert(self.tables.cfel_analysis_results).values(
                    directory_name=r.directory_name,
                    data_set_id=r.data_set_id,
                    resolution=r.resolution,
                    rsplit=r.rsplit,
                    cchalf=r.cchalf,
                    ccstar=r.ccstar,
                    snr=r.snr,
                    completeness=r.completeness,
                    multiplicity=r.multiplicity,
                    total_measurements=r.total_measurements,
                    unique_reflections=r.unique_reflections,
                    num_patterns=r.num_patterns,
                    num_hits=r.num_hits,
                    indexed_patterns=r.indexed_patterns,
                    indexed_crystals=r.indexed_crystals,
                    crystfel_version=r.crystfel_version,
                    ccstar_rsplit=r.ccstar_rsplit,
                    created=r.created,
                )
            )
        ).inserted_primary_key[0]
        for fid in file_ids:
            await conn.execute(
                sa.insert(self.tables.cfel_analysis_result_has_file).values(
                    analysis_result_id=result_id, file_id=fid
                )
            )
        return result_id

    async def retrieve_cfel_analysis_results(
        self, conn: Connection
    ) -> List[DBCFELAnalysisResult]:
        ar = self.tables.cfel_analysis_results.c
        file_table = self.tables.file
        result_to_file: Dict[int, List[Tuple[int, DBFile]]] = group_by(
            [
                (
                    r["analysis_result_id"],
                    DBFile(
                        id=r["id"],
                        description=r["description"],
                        type_=r["type"],
                        file_name=r["file_name"],
                        size_in_bytes=r["size_in_bytes"],
                        original_path=r["original_path"],
                    ),
                )
                for r in await (
                    conn.execute(
                        sa.select(
                            [
                                file_table.c.id,
                                file_table.c.description,
                                file_table.c.type,
                                file_table.c.file_name,
                                file_table.c.size_in_bytes,
                                file_table.c.original_path,
                                self.tables.cfel_analysis_result_has_file.c.analysis_result_id,
                            ]
                        ).join(
                            self.tables.cfel_analysis_result_has_file,
                            self.tables.cfel_analysis_result_has_file.c.file_id
                            == file_table.c.id,
                        )
                    )
                )
            ],
            key=lambda x: x[0],
        )
        return [
            DBCFELAnalysisResult(
                id=r["id"],
                directory_name=r["directory_name"],
                data_set_id=r["data_set_id"],
                resolution=r["resolution"],
                rsplit=r["rsplit"],
                cchalf=r["cchalf"],
                ccstar=r["ccstar"],
                snr=r["snr"],
                completeness=r["completeness"],
                multiplicity=r["multiplicity"],
                total_measurements=r["total_measurements"],
                unique_reflections=r["unique_reflections"],
                num_patterns=r["num_patterns"],
                num_hits=r["num_hits"],
                indexed_patterns=r["indexed_patterns"],
                indexed_crystals=r["indexed_crystals"],
                crystfel_version=r["crystfel_version"],
                ccstar_rsplit=r["ccstar_rsplit"],
                created=r["created"],
                files=[x[1] for x in result_to_file.get(r["id"], [])],
            )
            for r in await (
                conn.execute(
                    sa.select(
                        [
                            ar.id,
                            ar.directory_name,
                            ar.data_set_id,
                            ar.resolution,
                            ar.rsplit,
                            ar.cchalf,
                            ar.ccstar,
                            ar.snr,
                            ar.completeness,
                            ar.multiplicity,
                            ar.total_measurements,
                            ar.unique_reflections,
                            ar.num_patterns,
                            ar.num_hits,
                            ar.indexed_patterns,
                            ar.indexed_crystals,
                            ar.crystfel_version,
                            ar.ccstar_rsplit,
                            ar.created,
                        ]
                    )
                )
            )
        ]

    async def create_run(
        self,
        conn: Connection,
        run_id: int,
        attributi: List[DBAttributo],
        attributi_map: AttributiMap,
        keep_manual_attributes_from_previous_run: bool,
    ) -> None:
        final_attributi_map: AttributiMap
        if keep_manual_attributes_from_previous_run:
            latest_run = await self.retrieve_latest_run(conn, attributi)
            if latest_run is not None:
                final_attributi_map = latest_run.attributi.create_sub_map_for_group(
                    ATTRIBUTO_GROUP_MANUAL
                )
                final_attributi_map.remove_but_keep_type(ATTRIBUTO_STOPPED)
                final_attributi_map.extend_with_attributi_map(attributi_map)
            else:
                final_attributi_map = attributi_map
        else:
            final_attributi_map = attributi_map
        await conn.execute(
            sa.insert(self.tables.run).values(
                id=run_id,
                attributi=final_attributi_map.to_json(),
                modified=datetime.datetime.utcnow(),
            )
        )

    async def retrieve_latest_run(
        self, conn: Connection, attributi: List[DBAttributo]
    ) -> Optional[DBRun]:
        maximum_id = (
            await conn.execute(sa.select([sa.func.max(self.tables.run.c.id)]))
        ).fetchone()

        if maximum_id is None:
            return None

        return await self.retrieve_run(conn, maximum_id[0], attributi)

    async def retrieve_sample(
        self, conn: Connection, id_: int, attributi: List[DBAttributo]
    ) -> Optional[DBSample]:
        rc = self.tables.sample.c
        r = (
            await conn.execute(
                sa.select([rc.id, rc.name, rc.attributi]).where(rc.id == id_)
            )
        ).fetchone()
        files = await self._retrieve_files(
            conn,
            self.tables.sample_has_file.c.sample_id,
            (self.tables.sample_has_file.c.sample_id == id_),
        )
        if r is None:
            return None
        return DBSample(
            id=id_,
            name=r["name"],
            attributi=AttributiMap.from_types_and_json(
                attributi, sample_ids=[], raw_attributi=r["attributi"]
            ),
            files=files.get(id_, []),
        )

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
    ) -> List[DBExperimentType]:
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

        if not attributi.items():
            raise Exception("You have to assign at least one value to an attributo")

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
        attributi: List[DBAttributo],
    ) -> List[DBDataSet]:
        dc = self.tables.data_set.c
        return [
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
        ]

    async def update_data_set_attributi(
        self, conn: Connection, id_: int, attributi: AttributiMap
    ) -> None:
        dc = self.tables.data_set.c
        await conn.execute(
            sa.update(self.tables.data_set)
            .values(attributi=attributi.to_json())
            .where(dc.id == id_)
        )

    async def retrieve_bulk_run_attributi(
        self, conn: Connection, attributi: List[DBAttributo], run_ids: Iterable[int]
    ) -> Dict[AttributoId, Set[AttributoValue]]:
        runs = await self.retrieve_runs(conn, attributi)

        uninteresting_attributi = (ATTRIBUTO_STOPPED, ATTRIBUTO_STARTED)
        interesting_attributi = [
            a for a in attributi if a.name not in uninteresting_attributi
        ]

        attributi_values: Dict[AttributoId, Set[AttributoValue]] = {
            a.name: set() for a in interesting_attributi
        }
        for run in (run for run in runs if run.id in run_ids):
            for a in (a for a in interesting_attributi):
                run_attributo_value = run.attributi.select(a.name)
                if run_attributo_value is not None:
                    attributi_values[a.name].add(run_attributo_value)
        return attributi_values

    async def update_bulk_run_attributi(
        self,
        conn: Connection,
        attributi: List[DBAttributo],
        run_ids: Set[int],
        attributi_values: AttributiMap,
    ) -> None:
        runs = await self.retrieve_runs(conn, attributi)

        for run in (run for run in runs if run.id in run_ids):
            for attributo_id, attributo_value in attributi_values.items():
                run.attributi.append_single(attributo_id, attributo_value)
            await self.update_run_attributi(conn, run.id, run.attributi)


# Any until openpyxl has official types
def attributo_value_to_spreadsheet_cell(
    sample_id_to_name: Dict[int, str],
    attributo_type: AttributoType,
    attributo_value: AttributoValue,
) -> Any:
    if attributo_value is None:
        return None
    if isinstance(attributo_type, AttributoTypeSample):
        if not isinstance(attributo_value, int):
            raise TypeError(f"sample IDs have to have type int, got {attributo_value}")
        return sample_id_to_name.get(
            attributo_value, f"invalid sample ID {attributo_value}"
        )
    if isinstance(attributo_value, datetime.datetime):
        return datetime_to_local(attributo_value)
    if isinstance(
        attributo_value,
        (str, int, float, bool),
    ):
        return attributo_value
    if isinstance(attributo_value, list):
        return str(attributo_value)
    raise TypeError(
        f"invalid attributo type {type(attributo_value)}: {attributo_value}"
    )


@dataclass(frozen=True)
class WorkbookOutput:
    workbook: Workbook
    files: Set[int]


async def create_workbook(
    db: AsyncDB, conn: Connection, with_events: bool
) -> WorkbookOutput:
    wb = Workbook(iso_dates=True)

    runs_sheet = wb.active
    runs_sheet.title = "Runs"
    attributi_sheet = wb.create_sheet("Attributi")
    samples_sheet = wb.create_sheet("Samples")

    attributi = await db.retrieve_attributi(conn, associated_table=None)
    attributi.sort(key=attributo_sort_key)

    for attributo_column, attributo_header_name in enumerate(
        (
            "Table",
            "Name",
            "Group",
            "Description",
            "Type",
        ),
        start=1,
    ):
        cell = attributi_sheet.cell(
            row=1, column=attributo_column, value=attributo_header_name
        )
        cell.font = cell.font.copy(bold=True)

    for attributo_row_idx, attributo in enumerate(attributi, start=2):
        attributi_sheet.cell(
            row=attributo_row_idx,
            column=1,
            value=attributo.associated_table.value.capitalize(),
        )
        attributi_sheet.cell(row=attributo_row_idx, column=2, value=attributo.name)
        attributi_sheet.cell(row=attributo_row_idx, column=3, value=attributo.group)
        attributi_sheet.cell(
            row=attributo_row_idx, column=4, value=attributo.description
        )
        attributi_sheet.cell(
            row=attributo_row_idx,
            column=5,
            value=attributo_type_to_string(attributo.attributo_type),
        )

    sample_attributi = [
        a for a in attributi if a.associated_table == AssociatedTable.SAMPLE
    ]
    for sample_column, sample_header_name in enumerate(
        [AttributoId("Name")]
        + [a.name for a in sample_attributi]
        + [AttributoId("File IDs")],
        start=1,
    ):
        cell = samples_sheet.cell(
            row=1, column=sample_column, value=str(sample_header_name)
        )
        cell.font = cell.font.copy(bold=True)

    files_to_include: Set[int] = set()
    samples = await db.retrieve_samples(conn, attributi)
    for sample_row_idx, sample in enumerate(samples, start=2):
        samples_sheet.cell(
            row=sample_row_idx,
            column=1,
            value=sample.name,
        )
        for sample_column_idx, sample_attributo in enumerate(
            sample_attributi,
            start=2,
        ):
            samples_sheet.cell(
                row=sample_row_idx,
                column=sample_column_idx,
                value=attributo_value_to_spreadsheet_cell(
                    sample_id_to_name={},
                    attributo_type=sample_attributo.attributo_type,
                    attributo_value=sample.attributi.select(sample_attributo.name),
                ),
            )
        if sample.files:
            samples_sheet.cell(
                row=sample_row_idx,
                column=2 + len(sample_attributi),
                value=", ".join(str(f.id) for f in sample.files),
            )
            files_to_include.update(cast(int, f.id) for f in sample.files)

    run_attributi = [a for a in attributi if a.associated_table == AssociatedTable.RUN]
    for run_column, run_header_name in enumerate(
        [AttributoId("ID")] + [a.name for a in run_attributi],
        start=1,
    ):
        cell = runs_sheet.cell(row=1, column=run_column, value=str(run_header_name))
        cell.font = cell.font.copy(bold=True)

    sample_id_to_name: Dict[int, str] = {s.id: s.name for s in samples}
    events = await db.retrieve_events(conn)
    event_iterator = 0
    run_row_idx = 2
    for run in await db.retrieve_runs(conn, attributi):
        started = run.attributi.select_datetime(ATTRIBUTO_STARTED)
        event_start = event_iterator
        while (
            with_events
            and started is not None
            and event_iterator < len(events)
            and events[event_iterator].created >= started
        ):
            event_iterator += 1

        for event in events[event_start:event_iterator]:
            runs_sheet.cell(row=run_row_idx, column=2, value=event.created)
            event_text = f"{event.source}: {event.text}"
            if event.files:
                event_text += (
                    " (file IDs: " + ", ".join(str(f.id) for f in event.files) + ")"
                )
                files_to_include.update(cast(int, f.id) for f in event.files)
            runs_sheet.cell(row=run_row_idx, column=4, value=event_text)
            run_row_idx += 1

        runs_sheet.cell(
            row=run_row_idx,
            column=1,
            value=run.id,
        )
        for run_column_idx, run_attributo in enumerate(run_attributi, start=2):
            runs_sheet.cell(
                row=run_row_idx,
                column=run_column_idx,
                value=attributo_value_to_spreadsheet_cell(
                    sample_id_to_name=sample_id_to_name,
                    attributo_type=run_attributo.attributo_type,
                    attributo_value=run.attributi.select(run_attributo.name),
                ),
            )
        run_row_idx += 1

    return WorkbookOutput(wb, files_to_include)
