import datetime
import logging
import pickle
from dataclasses import dataclass
from itertools import groupby
from time import time
from typing import Any, Dict, List, Optional, Tuple, cast

import sqlalchemy as sa
from sqlalchemy import and_

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    property_type_to_schema,
    schema_json_to_property_type,
)
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import (
    AttributoId,
)
from amarcord.db.comment import DBComment
from amarcord.db.constants import DB_SOURCE_NAME, MANUAL_SOURCE_NAME
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.karabo import Karabo
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.rich_attributo_type import RichAttributoType
from amarcord.db.tabled_attributo import TabledAttributo
from amarcord.db.tables import (
    DBTables,
)
from amarcord.modules.dbcontext import DBContext
from amarcord.query_parser import Row as QueryRow
from amarcord.util import dict_union, remove_duplicates_stable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DBTarget:
    id: Optional[int]
    name: str
    short_name: str
    molecular_weight: Optional[float]
    uniprot_id: str


@dataclass(frozen=True)
class DBSample:
    id: Optional[int]
    target_id: int
    compounds: Optional[List[int]]
    micrograph: Optional[str]
    protocol: Optional[str]
    attributi: RawAttributiMap


@dataclass(frozen=True)
class DBRun:
    attributi: RawAttributiMap
    id: int
    sample_id: Optional[int]
    proposal_id: int
    modified: datetime.datetime
    comments: List[DBComment]


Connection = Any


def _update_attributi(
    conn: Connection,
    entity_id: int,
    entity_table: sa.Table,
    runprop: AttributoId,
    value: Any,
):
    current_json = conn.execute(
        sa.select([entity_table.c.attributi]).where(entity_table.c.id == entity_id)
    ).first()[0]
    assert current_json is None or isinstance(
        current_json, dict
    ), f"attributi should be None or dictionary, got {type(current_json)}"
    if current_json is None:
        current_json = {}
    if MANUAL_SOURCE_NAME not in current_json:
        current_json[MANUAL_SOURCE_NAME] = {}
    manual = current_json[MANUAL_SOURCE_NAME]
    assert isinstance(
        manual, dict
    ), f"manual input should be dictionary, not {type(manual)}"
    manual[str(runprop)] = value
    conn.execute(
        sa.update(entity_table)
        .where(entity_table.c.id == entity_id)
        .values(attributi=current_json, modified=datetime.datetime.utcnow())
    )


OverviewAttributi = Dict[AssociatedTable, AttributiMap]


@dataclass(frozen=True)
class TableWithAttributi:
    table: AssociatedTable
    attributi: Dict[AttributoId, DBAttributo]


class RunNotFound(Exception):
    pass


def _run_to_attributi(r: DBRun, types: Dict[AttributoId, DBAttributo]) -> AttributiMap:
    result = AttributiMap(types, r.attributi)
    result.append_to_source(
        DB_SOURCE_NAME,
        {
            AttributoId("id"): r.id,
            AttributoId("sample_id"): r.sample_id,
            AttributoId("modified"): r.modified,
            AttributoId("comments"): r.comments,
            AttributoId("proposal_id"): r.proposal_id,
        },
    )
    return result


def _sample_to_attributi(
    s: DBSample, types: Dict[AttributoId, DBAttributo]
) -> AttributiMap:
    result = AttributiMap(types, s.attributi)
    result.append_to_source(DB_SOURCE_NAME, {AttributoId("id"): s.id})
    return result


class DB:
    def __init__(self, dbcontext: DBContext, tables: DBTables) -> None:
        self.dbcontext = dbcontext
        self.tables = tables

    def overview_update_time(self, conn: Connection) -> datetime.datetime:
        return max(
            conn.execute(
                sa.select(
                    [
                        sa.func.max(self.tables.run.c.modified),
                        sa.func.max(self.tables.sample.c.modified),
                    ]
                ).select_from(self.tables.run.outerjoin(self.tables.sample))
            ).fetchone()
        )

    def run_count(self, conn: Connection) -> int:
        return conn.execute(
            sa.select([sa.func.count()]).select_from(self.tables.run)
        ).fetchone()[0]

    def retrieve_overview(
        self,
        conn: Connection,
        proposal_id: ProposalId,
        types: Dict[AssociatedTable, Dict[AttributoId, DBAttributo]],
    ) -> List[OverviewAttributi]:
        sample_types = types[AssociatedTable.SAMPLE]
        samples: Dict[int, AttributiMap] = {
            cast(int, k.id): _sample_to_attributi(k, sample_types)
            for k in self.retrieve_samples(conn)
        }
        runs: List[DBRun] = self.retrieve_runs(conn, proposal_id, None)

        run_types = types[AssociatedTable.RUN]
        result: List[OverviewAttributi] = []
        for r in runs:
            sample_id = r.sample_id
            sample = samples.get(sample_id, None) if sample_id is not None else None
            assert sample is not None, f"run {r.id} has invalid sample {sample_id}"
            result.append(
                {
                    AssociatedTable.SAMPLE: sample,
                    AssociatedTable.RUN: _run_to_attributi(r, run_types),
                }
            )
        return result

    def retrieve_runs(
        self,
        conn: Connection,
        proposal_id: Optional[ProposalId],
        since: Optional[datetime.datetime],
    ) -> List[DBRun]:
        run = self.tables.run
        comment = self.tables.run_comment

        where_condition = (
            run.c.proposal_id == proposal_id if proposal_id is not None else True
        )
        if since:
            where_condition = and_(where_condition, run.c.modified >= since)
        select_stmt = (
            sa.select(
                [
                    comment.c.id.label("comment_id"),
                    comment.c.author,
                    comment.c.comment_text,
                    comment.c.created,
                    run.c.id,
                    run.c.sample_id,
                    run.c.proposal_id,
                    run.c.modified,
                    run.c.attributi,
                ]
            )
            .select_from(run.outerjoin(comment))
            .where(where_condition)
            .order_by(run.c.id, comment.c.id)
        )
        result: List[DBRun] = []
        before = time()
        select_results = conn.execute(select_stmt).fetchall()
        after = time()
        # logger.info("Retrieved runs in %ss", after - before)
        for _run_id, run_rows in groupby(
            select_results,
            lambda x: x["id"],
        ):
            rows = list(run_rows)
            run_meta = rows[0]
            comments = remove_duplicates_stable(
                DBComment(
                    id=row["comment_id"],
                    run_id=run_meta["id"],
                    author=row["author"],
                    text=row["comment_text"],
                    created=row["created"],
                )
                for row in rows
                if row[0] is not None
            )[0:5]
            result.append(
                DBRun(
                    RawAttributiMap(run_meta["attributi"]),
                    run_meta["id"],
                    run_meta["sample_id"],
                    run_meta["proposal_id"],
                    run_meta["modified"],
                    comments,
                )
            )
        return result

    def retrieve_run_ids(self, conn: Connection, proposal_id: ProposalId) -> List[int]:
        return [
            row[0]
            for row in conn.execute(
                sa.select([self.tables.run.c.id])
                .where(self.tables.run.c.proposal_id == proposal_id)
                .order_by(self.tables.run.c.id)
            ).fetchall()
        ]

    def retrieve_sample_ids(self, conn: Connection) -> List[int]:
        return [
            row[0]
            for row in conn.execute(
                sa.select([self.tables.sample.c.id]).order_by(self.tables.sample.c.id)
            ).fetchall()
        ]

    def change_comment(self, conn: Connection, c: DBComment) -> None:
        assert c.id is not None

        conn.execute(
            sa.update(self.tables.run_comment)
            .where(self.tables.run_comment.c.id == c.id)
            .values(author=c.author, comment_text=c.text)
        )
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == c.run_id)
            .values(modified=datetime.datetime.utcnow())
        )

    def retrieve_run(self, conn: Connection, run_id: int) -> DBRun:
        run = self.tables.run
        run_c = run.c
        comment = self.tables.run_comment
        select_statement = (
            sa.select(
                [
                    comment.c.id.label("comment_id"),
                    comment.c.author,
                    comment.c.comment_text,
                    comment.c.created,
                    run.c.id,
                    run.c.sample_id,
                    run.c.proposal_id,
                    run.c.modified,
                    run.c.attributi,
                ]
            )
            .select_from(run.outerjoin(comment))
            .where(run_c.id == run_id)
        )
        run_rows = conn.execute(select_statement).fetchall()
        if not run_rows:
            raise RunNotFound()
        run_meta = run_rows[0]
        return DBRun(
            RawAttributiMap(run_meta["attributi"]),
            run_meta["id"],
            run_meta["sample_id"],
            run_meta["proposal_id"],
            run_meta["modified"],
            remove_duplicates_stable(
                DBComment(
                    row["comment_id"],
                    run_meta["id"],
                    row["author"],
                    row["comment_text"],
                    row["created"],
                )
                for row in run_rows
                if row["comment_id"] is not None
            ),
        )

    def add_comment(self, conn: Connection, run_id: int, author: str, text: str) -> int:
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == run_id)
            .values(modified=datetime.datetime.utcnow())
        )
        result = conn.execute(
            sa.insert(self.tables.run_comment).values(
                run_id=run_id,
                author=author,
                comment_text=text,
                created=datetime.datetime.utcnow(),
            )
        )
        return result.inserted_primary_key[0]

    def delete_comment(self, conn: Connection, run_id: int, comment_id: int) -> None:
        conn.execute(
            sa.delete(self.tables.run_comment).where(
                self.tables.run_comment.c.id == comment_id
            )
        )
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == run_id)
            .values(modified=datetime.datetime.utcnow())
        )

    def add_run(
        self,
        conn: Connection,
        proposal_id: ProposalId,
        run_id: int,
        sample_id: Optional[int],
        attributi: RawAttributiMap,
    ) -> bool:
        with conn.begin():
            run_exists = conn.execute(
                sa.select([self.tables.run.c.id]).where(self.tables.run.c.id == run_id)
            ).fetchall()
            if len(run_exists) > 0:
                return False

            conn.execute(
                self.tables.run.insert().values(
                    proposal_id=proposal_id,
                    id=run_id,
                    sample_id=sample_id,
                    attributi=attributi.to_json(),
                    modified=datetime.datetime.utcnow(),
                )
            )
            return True

    def add_proposal(self, conn: Connection, prop_id: ProposalId) -> None:
        conn.execute(self.tables.proposal.insert().values(id=prop_id))

    def update_run_karabo(self, conn: Connection, run_id: int, karabo: bytes) -> None:
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == run_id)
            .values(karabo=karabo, modified=datetime.datetime.utcnow())
        )

    def retrieve_attributi(
        self, conn: Connection, filter_table: Optional[AssociatedTable] = None
    ) -> Dict[AssociatedTable, Dict[AttributoId, DBAttributo]]:
        select_stmt = sa.select(
            [
                self.tables.attributo.c.name,
                self.tables.attributo.c.description,
                self.tables.attributo.c.json_schema,
                self.tables.attributo.c.associated_table,
            ]
        ).order_by(self.tables.attributo.c.associated_table)

        if filter_table is not None:
            select_stmt = select_stmt.where(
                self.tables.attributo.c.associated_table == filter_table
            )

        result = {
            table: {
                AttributoId(row[0]): DBAttributo(
                    name=AttributoId(row[0]),
                    description=row[1],
                    rich_property_type=schema_json_to_property_type(json_schema=row[2]),
                    associated_table=table,
                )
                for row in rows
            }
            for table, rows in groupby(
                conn.execute(select_stmt).fetchall(),
                lambda x: x[3],
            )
        }
        for table, attributi in self.tables.additional_attributi.items():
            if table not in result:
                result[table] = attributi
            else:
                result[table].update(attributi)
        return result

    def retrieve_table_attributi(
        self, conn: Connection, table: AssociatedTable
    ) -> Dict[AttributoId, DBAttributo]:
        return self.retrieve_attributi(conn, table)[table]

    def run_attributi(self, conn: Connection) -> Dict[AttributoId, DBAttributo]:
        return self.retrieve_table_attributi(conn, AssociatedTable.RUN)

    def update_sample_attributo(
        self, conn: Connection, sample_id: int, attributo: AttributoId, value: Any
    ) -> None:
        _update_attributi(conn, sample_id, self.tables.sample, attributo, value)

    def update_run_attributi(
        self, conn: Connection, run_id: int, attributi: RawAttributiMap
    ) -> None:
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == run_id)
            .values(attributi=attributi.to_json(), modified=datetime.datetime.utcnow())
        )

    def update_sample_attributi(
        self, conn: Connection, sample_id: int, attributi: RawAttributiMap
    ) -> None:
        conn.execute(
            sa.update(self.tables.sample)
            .where(self.tables.sample.c.id == sample_id)
            .values(attributi=attributi.to_json(), modified=datetime.datetime.utcnow())
        )

    def update_run_attributo(
        self, conn: Connection, run_id: int, attributo: AttributoId, value: Any
    ) -> None:
        if attributo == self.tables.attributo_run_sample_id:
            assert isinstance(value, int), "sample ID should be an integer"

            conn.execute(
                sa.update(self.tables.run)
                .where(self.tables.run.c.id == run_id)
                .values(sample_id=value, modified=datetime.datetime.utcnow())
            )
            return

        assert isinstance(
            value, (str, int, float, list)
        ), f"attributi can only have str, int and float values currently, got {type(value)}"

        with conn.begin():
            # TODO: This has to be fixed
            _update_attributi(conn, run_id, self.tables.run, attributo, value)

    def connect(self) -> Connection:
        return self.dbcontext.connect()

    def retrieve_karabo(self, conn: Connection, run_id: int) -> Optional[Karabo]:
        result = conn.execute(
            sa.select([self.tables.run.c.karabo]).where(self.tables.run.c.id == run_id)
        ).fetchall()
        return (
            pickle.loads(result[0][0]) if result and result[0][0] is not None else None
        )

    def add_attributo(
        self,
        conn: Connection,
        name: str,
        description: str,
        associated_table: AssociatedTable,
        prop_type: RichAttributoType,
    ) -> None:
        conn.execute(
            self.tables.attributo.insert().values(
                name=name,
                description=description,
                associated_table=associated_table,
                json_schema=property_type_to_schema(prop_type),
            )
        )

    def retrieve_targets(self, conn: Connection) -> List[DBTarget]:
        tc = self.tables.target.c
        return [
            DBTarget(
                row["id"],
                row["name"],
                row["short_name"],
                row["molecular_weight"],
                row["uniprot_id"],
            )
            for row in conn.execute(
                sa.select(
                    [tc.id, tc.name, tc.short_name, tc.molecular_weight, tc.uniprot_id]
                ).order_by(tc.short_name)
            ).fetchall()
        ]

    def add_target(self, conn: Connection, t: DBTarget) -> None:
        conn.execute(
            sa.insert(self.tables.target).values(
                name=t.name,
                short_name=t.short_name,
                molecular_weight=t.molecular_weight,
                uniprot_id=t.uniprot_id,
            )
        )

    def edit_target(self, conn: Connection, t: DBTarget) -> None:
        conn.execute(
            sa.update(self.tables.target)
            .values(
                name=t.name,
                short_name=t.short_name,
                molecular_weight=t.molecular_weight,
                uniprot_id=t.uniprot_id,
            )
            .where(self.tables.target.c.id == t.id)
        )

    def delete_target(self, conn: Connection, tid: int) -> None:
        conn.execute(
            sa.delete(self.tables.target).where(self.tables.target.c.id == tid)
        )

    def retrieve_samples(
        self, conn: Connection, since: Optional[datetime.datetime] = None
    ) -> List[DBSample]:
        tc = self.tables.sample.c
        select_stmt = sa.select(
            [
                tc.id,
                tc.target_id,
                tc.compounds,
                tc.micrograph,
                tc.protocol,
                tc.attributi,
            ]
        ).order_by(tc.id)
        if since is not None:
            select_stmt = select_stmt.where(tc.modified >= since)

        def prepare_sample(row: Any) -> DBSample:
            return DBSample(
                id=row["id"],
                target_id=row["target_id"],
                compounds=row["compounds"],
                micrograph=row["micrograph"],
                protocol=row["protocol"],
                attributi=RawAttributiMap(row["attributi"]),
            )

        return [prepare_sample(row) for row in conn.execute(select_stmt).fetchall()]

    def add_sample(self, conn: Connection, t: DBSample) -> None:
        conn.execute(
            sa.insert(self.tables.sample).values(
                target_id=t.target_id,
                creator=t.creator,
                compounds=t.compounds,
                micrograph=t.micrograph,
                protocol=t.protocol,
                attributi=t.attributi.to_json() if t.attributi is not None else None,
                modified=datetime.datetime.utcnow(),
            )
        )

    def edit_sample(self, conn: Connection, t: DBSample) -> None:
        assert t.id is not None
        conn.execute(
            sa.update(self.tables.sample)
            .values(
                target_id=t.target_id,
                creator=t.creator,
                compounds=t.compounds,
                micrograph=t.micrograph,
                protocol=t.protocol,
                attributi=t.attributi.to_json() if t.attributi is not None else None,
                modified=datetime.datetime.utcnow(),
            )
            .where(self.tables.sample.c.id == t.id)
        )

    def delete_sample(self, conn: Connection, tid: int) -> None:
        conn.execute(
            sa.delete(self.tables.sample).where(self.tables.sample.c.id == tid)
        )

    def delete_run(self, conn: Connection, rid: int) -> None:
        conn.execute(sa.delete(self.tables.run).where(self.tables.run.c.id == rid))

    def delete_attributo(
        self, conn: Connection, table: AssociatedTable, name: AttributoId
    ) -> None:
        with conn.begin():
            conn.execute(
                sa.delete(self.tables.attributo).where(
                    and_(
                        self.tables.attributo.c.name == str(name),
                        self.tables.attributo.c.associated_table == table,
                    )
                )
            )
            if table == AssociatedTable.RUN:
                for run in self.retrieve_runs(conn, proposal_id=None, since=None):
                    existed = run.attributi.remove_attributo(name)
                    if existed:
                        self.update_run_attributi(
                            conn,
                            run.attributi.select_int_unsafe(
                                self.tables.attributo_run_id
                            ),
                            run.attributi,
                        )
            elif table == AssociatedTable.SAMPLE:
                for sample in self.retrieve_samples(conn, since=None):
                    existed = sample.attributi.remove_attributo(name)
                    if existed:
                        self.update_sample_attributi(
                            conn,
                            cast(int, sample.id),
                            sample.attributi,
                        )
            else:
                raise Exception(
                    f"cannot delete attributo {name} of table {table} because that code doesn't exist yet"
                )


def overview_row_to_query_row(
    row: OverviewAttributi, metadata: List[TabledAttributo]
) -> QueryRow:
    return dict_union(
        [
            table_attributi.to_query_row(
                [md.attributo.name for md in metadata if md.table == table],
                prefix=f"{table.value}.",
            )
            for table, table_attributi in row.items()
        ]
    )
