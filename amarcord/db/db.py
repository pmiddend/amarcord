import datetime
import logging
import pickle
from dataclasses import dataclass
from itertools import groupby
from time import time
from typing import Any, Dict, List, Mapping, Optional, Tuple, cast

import sqlalchemy as sa
from sqlalchemy import and_

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    AttributiMap,
    AttributoValue,
    DBAttributo,
    DBRunComment,
    RichAttributoType,
    Source,
    property_type_to_schema,
    schema_to_property_type,
)
from amarcord.db.attributo_id import (
    AttributoId,
)
from amarcord.db.constants import DB_SOURCE_NAME, MANUAL_SOURCE_NAME
from amarcord.db.karabo import Karabo
from amarcord.db.proposal_id import ProposalId
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
    created: datetime.datetime
    target_id: int
    average_crystal_size: Optional[float]
    crystal_shape: Optional[Tuple[float, float, float]]
    incubation_time: Optional[datetime.datetime]
    crystallization_temperature: Optional[float]
    shaking_time: Optional[datetime.timedelta]
    shaking_strength: Optional[float]
    protein_concentration: Optional[float]
    comment: str
    crystal_settlement_volume: Optional[float]
    seed_stock_used: str
    plate_origin: str
    creator: str
    crystallization_method: str
    filters: Optional[List[str]]
    compounds: Optional[List[int]]
    micrograph: Optional[str]
    protocol: Optional[str]
    attributi: AttributiMap


Connection = Any


def _decode_attributi_to_values(
    attributi: Mapping[str, Any]
) -> Dict[Source, Dict[AttributoId, AttributoValue]]:
    result: Dict[Source, Dict[AttributoId, AttributoValue]] = {}
    for k, v in attributi.items():
        assert isinstance(v, dict)
        result[AttributoId(k)] = v
    return result


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

    def retrieve_overview(
        self,
        conn: Connection,
        proposal_id: ProposalId,
    ) -> List[OverviewAttributi]:
        samples: Dict[int, AttributiMap] = {
            cast(int, k.id): k.attributi for k in self.retrieve_samples(conn)
        }
        runs: List[AttributiMap] = self.retrieve_runs(conn, proposal_id, None)

        result: List[OverviewAttributi] = []
        for r in runs:
            sample_id = r.select_int(self.tables.attributo_run_sample_id)
            sample = samples.get(sample_id, None) if sample_id is not None else None
            assert (
                sample is not None
            ), f"run {r.select_int_unsafe(self.tables.attributo_run_id)} has invalid sample {sample_id}"
            result.append({AssociatedTable.SAMPLE: sample, AssociatedTable.RUN: r})
        return result

    def retrieve_runs(
        self,
        conn: Connection,
        proposal_id: ProposalId,
        since: Optional[datetime.datetime],
    ) -> List[AttributiMap]:
        run = self.tables.run
        comment = self.tables.run_comment

        where_condition = run.c.proposal_id == proposal_id
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
        result: List[AttributiMap] = []
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
                DBRunComment(
                    id=row["comment_id"],
                    run_id=run_meta["id"],
                    author=row["author"],
                    text=row["comment_text"],
                    created=row["created"],
                )
                for row in rows
                if row[0] is not None
            )[0:5]
            attributi = AttributiMap(run_meta["attributi"])
            attributi.append_to_source(
                DB_SOURCE_NAME,
                {
                    self.tables.attributo_run_id: run_meta["id"],
                    self.tables.attributo_run_sample_id: run_meta["sample_id"],
                    self.tables.attributo_run_proposal_id: run_meta["proposal_id"],
                    self.tables.attributo_run_modified: run_meta["modified"],
                    self.tables.attributo_run_comments: comments,
                },
            )
            result.append(attributi)
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

    def change_comment(self, conn: Connection, c: DBRunComment) -> None:
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

    def retrieve_run(self, conn: Connection, run_id: int) -> AttributiMap:
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
            raise Exception(f"couldn't find any runs with id {run_id}")
        run_meta = run_rows[0]
        result = AttributiMap(run_meta["attributi"])
        result.append_to_source(
            DB_SOURCE_NAME,
            {
                self.tables.attributo_run_id: run_meta["id"],
                self.tables.attributo_run_sample_id: run_meta["sample_id"],
                self.tables.attributo_run_proposal_id: run_meta["proposal_id"],
                self.tables.attributo_run_modified: run_meta["modified"],
                self.tables.attributo_run_comments: remove_duplicates_stable(
                    DBRunComment(
                        row["comment_id"],
                        run_meta["id"],
                        row["author"],
                        row["comment_text"],
                        row["created"],
                    )
                    for row in run_rows
                    if row["comment_id"] is not None
                ),
            },
        )
        return result

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
                    attributi={},
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
                self.tables.attributo.c.suffix,
                self.tables.attributo.c.json_schema,
                self.tables.attributo.c.associated_table,
            ]
        )
        if filter_table is not None:
            select_stmt = select_stmt.order_by(
                self.tables.attributo.c.associated_table
            ).where(self.tables.attributo.c.associated_table == filter_table)

        result = {
            table: {
                AttributoId(row[0]): DBAttributo(
                    name=AttributoId(row[0]),
                    description=row[1],
                    suffix=row[2],
                    rich_property_type=schema_to_property_type(
                        json_schema=row[3], suffix=row[2]
                    ),
                    associated_table=table,
                )
                for row in rows
            }
            for table, rows in groupby(
                conn.execute(select_stmt).fetchall(),
                lambda x: x[4],
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
        suffix: Optional[str],
        prop_type: RichAttributoType,
    ) -> None:
        conn.execute(
            self.tables.attributo.insert().values(
                name=name,
                description=description,
                suffix=suffix,
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
                tc.created,
                tc.average_crystal_size,
                tc.target_id,
                tc.crystal_shape,
                tc.incubation_time,
                tc.crystallization_temperature,
                tc.shaking_time_seconds,
                tc.shaking_strength,
                tc.protein_concentration,
                tc.comment,
                tc.crystal_settlement_volume,
                tc.seed_stock_used,
                tc.plate_origin,
                tc.creator,
                tc.crystallization_method,
                tc.filters,
                tc.compounds,
                tc.micrograph,
                tc.protocol,
                tc.attributi,
            ]
        ).order_by(tc.id)
        if since is not None:
            select_stmt = select_stmt.where(tc.modified >= since)
        return [
            DBSample(
                id=row["id"],
                created=row["created"],
                target_id=row["target_id"],
                average_crystal_size=row["average_crystal_size"],
                crystal_shape=row["crystal_shape"],
                incubation_time=row["incubation_time"],
                crystallization_temperature=row["crystallization_temperature"],
                shaking_time=datetime.timedelta(seconds=row["shaking_time_seconds"])
                if row["shaking_time_seconds"] is not None
                else None,
                shaking_strength=row["shaking_strength"],
                protein_concentration=row["protein_concentration"],
                comment=row["comment"],
                crystal_settlement_volume=row["crystal_settlement_volume"],
                seed_stock_used=row["seed_stock_used"],
                plate_origin=row["plate_origin"],
                creator=row["creator"],
                crystallization_method=row["crystallization_method"],
                filters=row["filters"],
                compounds=row["compounds"],
                micrograph=row["micrograph"],
                protocol=row["protocol"],
                attributi=AttributiMap(row["attributi"]),
            )
            for row in conn.execute(select_stmt).fetchall()
        ]

    def add_sample(self, conn: Connection, t: DBSample) -> None:
        conn.execute(
            sa.insert(self.tables.sample).values(
                target_id=t.target_id,
                average_crystal_size=t.average_crystal_size,
                crystal_shape=t.crystal_shape,
                incubation_time=t.incubation_time,
                # crystal_buffer=t.crystal_buffer,
                crystallization_temperature=t.crystallization_temperature,
                shaking_time_seconds=int(t.shaking_time.total_seconds())
                if t.shaking_time is not None
                else None,
                shaking_strength=t.shaking_strength,
                protein_concentration=t.protein_concentration,
                comment=t.comment,
                crystal_settlement_volume=t.crystal_settlement_volume,
                seed_stock_used=t.seed_stock_used,
                plate_origin=t.plate_origin,
                creator=t.creator,
                crystallization_method=t.crystallization_method,
                filters=t.filters,
                compounds=t.compounds,
                micrograph=t.micrograph,
                protocol=t.protocol,
                attributi=t.attributi.to_json() if t.attributi is not None else None,
                modified=datetime.datetime.utcnow(),
            )
        )

    def edit_sample(self, conn: Connection, t: DBSample) -> None:
        conn.execute(
            sa.update(self.tables.sample)
            .values(
                target_id=t.target_id,
                average_crystal_size=t.average_crystal_size,
                crystal_shape=t.crystal_shape,
                incubation_time=t.incubation_time,
                # crystal_buffer=t.crystal_buffer,
                crystallization_temperature=t.crystallization_temperature,
                shaking_time_seconds=int(t.shaking_time.total_seconds())
                if t.shaking_time is not None
                else None,
                shaking_strength=t.shaking_strength,
                protein_concentration=t.protein_concentration,
                comment=t.comment,
                crystal_settlement_volume=t.crystal_settlement_volume,
                seed_stock_used=t.seed_stock_used,
                plate_origin=t.plate_origin,
                creator=t.creator,
                crystallization_method=t.crystallization_method,
                filters=t.filters,
                compounds=t.compounds,
                micrograph=t.micrograph,
                protocol=t.protocol,
                attributi=t.attributi.to_json() if t.attributi is not None else None,
            )
            .where(self.tables.sample.c.id == t.id)
        )

    def delete_sample(self, conn: Connection, tid: int) -> None:
        conn.execute(
            sa.delete(self.tables.sample).where(self.tables.sample.c.id == tid)
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
