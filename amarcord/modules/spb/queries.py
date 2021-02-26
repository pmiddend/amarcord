import datetime
import pickle
from dataclasses import dataclass
from itertools import groupby
from time import time
from typing import Any, Dict, List, Optional, Tuple
import logging

import sqlalchemy as sa

from amarcord.modules.dbcontext import DBContext
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.run_id import RunId
from amarcord.modules.spb.run_property import (
    RunProperty,
)
from amarcord.modules.spb.tables import (
    CustomRunPropertyType,
    Tables,
    run_property_db_columns,
)
from amarcord.qt.properties import (
    PropertyDouble,
    PropertyString,
    PropertyTags,
    RichPropertyType,
)
from amarcord.util import capitalized_decamelized, dict_union, remove_duplicates_stable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RunSimple:
    run_id: RunId
    finished: bool


@dataclass(frozen=True)
class Comment:
    id: Optional[int]
    author: str
    text: str
    created: datetime.datetime


Karabo = Tuple[Dict[str, Any], Dict[str, Any]]


@dataclass(frozen=True)
class Run:
    properties: Dict[RunProperty, Any]
    karabo: Optional[Karabo]


@dataclass(frozen=True)
class CustomRunProperty:
    name: RunProperty
    prop_type: CustomRunPropertyType

    def to_rich_property_type(self) -> RichPropertyType:
        return (
            PropertyDouble()
            if self.prop_type == CustomRunPropertyType.DOUBLE
            else PropertyString()
        )


Connection = Any


@dataclass(frozen=True)
class RunPropertyMetadata:
    name: str
    rich_prop_type: Optional[RichPropertyType]


class SPBQueries:
    def __init__(self, dbcontext: DBContext, tables: Tables) -> None:
        self.dbcontext = dbcontext
        self.tables = tables

    def retrieve_runs(
        self, conn: Connection, proposal_id: ProposalId
    ) -> List[Dict[RunProperty, Any]]:
        run = self.tables.run
        tag = self.tables.run_tag
        comment = self.tables.run_comment

        interesting_columns: List[sa.Column] = run_property_db_columns(
            self.tables, with_blobs=False
        )
        select_stmt = (
            sa.select(
                [
                    comment.c.id.label("comment_id"),
                    comment.c.author,
                    comment.c.comment_text,
                    comment.c.created,
                    tag.c.tag_text,
                ]
                + interesting_columns  # type: ignore
            )
            .select_from(run.outerjoin(tag).outerjoin(comment))
            .where(run.c.proposal_id == proposal_id)
            .order_by(run.c.id, comment.c.id)
        )
        result: List[Dict[RunProperty, Any]] = []
        for _run_id, run_rows in groupby(
            conn.execute(select_stmt).fetchall(),
            lambda x: x["id"],
        ):
            rows = list(run_rows)
            run_meta = rows[0]
            tags = set(row["tag_text"] for row in rows if row["tag_text"] is not None)
            comments = remove_duplicates_stable(
                Comment(
                    id=row["comment_id"],
                    author=row["author"],
                    text=row["comment_text"],
                    created=row["created"],
                )
                for row in rows
                if row[0] is not None
            )[0:5]
            custom = run_meta["custom"]
            assert custom is None or isinstance(
                custom, dict
            ), f"custom column has type {type(custom)}"
            result.append(
                dict_union(
                    [
                        {
                            self.tables.property_tags: tags,
                            self.tables.property_comments: comments,
                        },
                        {
                            RunProperty(v.name): run_meta[v.name]
                            for v in interesting_columns
                        },
                        {RunProperty(k): v for k, v in custom.items()}
                        if custom is not None
                        else {},
                    ]
                )
            )
        return result

    def retrieve_run_ids(
        self, conn: Connection, proposal_id: ProposalId
    ) -> List[RunSimple]:
        return [
            RunSimple(row["id"], row["status"] == "finished")
            for row in conn.execute(
                sa.select([self.tables.run.c.id, self.tables.run.c.status])
                .where(self.tables.run.c.proposal_id == proposal_id)
                .order_by(self.tables.run.c.id)
            ).fetchall()
        ]

    def retrieve_tags(self, conn: Connection) -> List[str]:
        return [
            row[0]
            for row in conn.execute(
                sa.select([self.tables.run_tag.c.tag_text])
                .order_by(self.tables.run_tag.c.tag_text)
                .distinct()
            ).fetchall()
        ]

    def retrieve_sample_ids(self, conn: Connection) -> List[int]:
        return [
            row[0]
            for row in conn.execute(
                sa.select([self.tables.sample.c.sample_id]).order_by(
                    self.tables.sample.c.sample_id
                )
            ).fetchall()
        ]

    def change_sample(
        self, conn: Connection, run_id: int, new_sample: Optional[int]
    ) -> None:
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == run_id)
            .values(sample_id=new_sample)
        )

    def change_comment(self, conn: Connection, c: Comment) -> None:
        assert c.id is not None

        conn.execute(
            sa.update(self.tables.run_comment)
            .where(self.tables.run_comment.c.id == c.id)
            .values(author=c.author, comment_text=c.text)
        )

    def change_tags(self, conn: Connection, run_id: int, new_tags: List[str]) -> None:
        with conn.begin():
            conn.execute(
                sa.delete(self.tables.run_tag).where(
                    self.tables.run_tag.c.run_id == run_id
                )
            )
            if new_tags:
                conn.execute(
                    self.tables.run_tag.insert(),
                    [{"run_id": run_id, "tag_text": t} for t in new_tags],
                )

    def retrieve_run(self, conn: Connection, run_id: RunId) -> Run:
        run = self.tables.run
        run_c = run.c
        comment = self.tables.run_comment
        tag = self.tables.run_tag
        interesting_columns = run_property_db_columns(self.tables, with_blobs=False)
        before = time()
        select_statement = (
            sa.select(
                [
                    comment.c.id.label("comment_id"),
                    comment.c.author,
                    comment.c.comment_text,
                    comment.c.created,
                    tag.c.tag_text,
                ]
                + interesting_columns  # type: ignore
            )
            .select_from(run.outerjoin(tag).outerjoin(comment))
            .where(run_c.id == run_id)
        )
        run_rows = conn.execute(select_statement).fetchall()
        after = time()
        if not run_rows:
            raise Exception(f"couldn't find any runs with id {run_id}")
        run_meta = run_rows[0]
        custom = run_meta["custom"]
        return Run(
            properties=dict_union(
                [
                    {
                        self.tables.property_tags: remove_duplicates_stable(
                            [
                                row["tag_text"]
                                for row in run_rows
                                if row["tag_text"] is not None
                            ]
                        ),
                        self.tables.property_comments: remove_duplicates_stable(
                            Comment(
                                row["comment_id"],
                                row["author"],
                                row["comment_text"],
                                row["created"],
                            )
                            for row in run_rows
                            if row["comment_id"] is not None
                        ),
                    },
                    {
                        RunProperty(v.name): run_meta[v.name]
                        for v in interesting_columns
                    },
                    {RunProperty(k): v for k, v in custom.items() if custom is not None}
                    if custom is not None
                    else {},
                ],
            ),
            karabo=None,
        )

    def add_comment(self, conn: Connection, run_id: int, author: str, text: str) -> int:
        result = conn.execute(
            sa.insert(self.tables.run_comment).values(
                run_id=run_id,
                author=author,
                comment_text=text,
                created=datetime.datetime.utcnow(),
            )
        )
        return result.inserted_primary_key[0]

    def delete_comment(self, conn: Connection, comment_id: int) -> None:
        conn.execute(
            sa.delete(self.tables.run_comment).where(
                self.tables.run_comment.c.id == comment_id
            )
        )

    def create_run(
        self,
        conn: Connection,
        proposal_id: ProposalId,
        run_id: RunId,
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
                    status="running",
                    started=datetime.datetime.utcnow(),
                    modified=datetime.datetime.utcnow(),
                )
            )
            return True

    def create_proposal(self, conn: Connection, prop_id: ProposalId) -> None:
        conn.execute(self.tables.proposal.insert().values(id=prop_id))

    def update_run_karabo(self, conn: Connection, run_id: RunId, karabo: bytes) -> None:
        conn.execute(
            sa.update(self.tables.run)
            .where(self.tables.run.c.id == run_id)
            .values(karabo=karabo)
        )

    def custom_run_properties(self, conn: Connection) -> List[CustomRunProperty]:
        return [
            CustomRunProperty(name=RunProperty(row[0]), prop_type=row[1])
            for row in conn.execute(
                sa.select(
                    [
                        self.tables.custom_run_property.c.name,
                        self.tables.custom_run_property.c.prop_type,
                    ]
                )
            ).fetchall()
        ]

    def run_property_metadata(
        self, conn: Connection
    ) -> Dict[RunProperty, RunPropertyMetadata]:
        custom_props = self.custom_run_properties(conn)
        return dict_union(
            [
                {
                    c.name: RunPropertyMetadata(str(c.name), c.to_rich_property_type())
                    for c in custom_props
                },
                {
                    RunProperty(c.name): RunPropertyMetadata(
                        capitalized_decamelized(c.name),
                        self.tables.property_types.get(RunProperty(c.name), None),
                    )
                    for c in run_property_db_columns(self.tables, with_blobs=False)
                    if c.name != self.tables.property_custom
                },
                {
                    self.tables.property_tags: RunPropertyMetadata(
                        "Tags", PropertyTags()
                    ),
                    self.tables.property_comments: RunPropertyMetadata(
                        "Comments", None
                    ),
                },
            ]
        )

    def update_run_property(
        self, conn: Connection, run_id: int, runprop: RunProperty, value: Any
    ) -> None:
        if runprop == self.tables.property_tags:
            if not isinstance(value, list):
                raise ValueError(f"tags should be a list, got {type(value)}")
            self.change_tags(conn, run_id, value)
            return
        interesting_columns = run_property_db_columns(self.tables)
        for c in interesting_columns:
            if c.name == str(runprop):
                conn.execute(
                    sa.update(self.tables.run)
                    .where(self.tables.run.c.id == run_id)
                    .values({c: value})
                )
                return

        assert isinstance(
            value, (str, int, float)
        ), f"custom properties can only have str, int and float values currently, got {type(value)}"

        with conn.begin():
            current_json = conn.execute(
                sa.select([self.tables.run.c.custom]).where(
                    self.tables.run.c.id == run_id
                )
            ).first()[0]
            assert current_json is None or isinstance(
                current_json, dict
            ), f"custom column should be dictionary, got {type(current_json)}"
            if current_json is None:
                current_json = {}
            current_json[str(runprop)] = value
            conn.execute(
                sa.update(self.tables.run)
                .where(self.tables.run.c.id == run_id)
                .values({self.tables.run.c.custom: current_json})
            )

    def connect(self) -> Connection:
        return self.dbcontext.connect()

    def retrieve_karabo(self, conn: Connection, run_id: RunId) -> Optional[Karabo]:
        result = conn.execute(
            sa.select([self.tables.run.c.karabo]).where(self.tables.run.c.id == run_id)
        ).fetchall()
        return pickle.loads(result[0][0]) if result else None

    def add_custom_run_property(
        self, conn: Connection, name: str, type: CustomRunPropertyType
    ) -> None:
        conn.execute(
            self.tables.custom_run_property.insert().values(name=name, prop_type=type)
        )
