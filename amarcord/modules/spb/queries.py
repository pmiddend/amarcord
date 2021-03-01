import datetime
import logging
import pickle
from dataclasses import dataclass
from itertools import groupby
from time import time
from typing import Any, Dict, List, Mapping, Optional, Tuple

import sqlalchemy as sa

from amarcord.json_schema import (
    JSONSchemaArray,
    JSONSchemaInteger,
    JSONSchemaNumber,
    JSONSchemaString,
    parse_schema_type,
)
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.run_id import RunId
from amarcord.modules.spb.run_property import (
    RunProperty,
)
from amarcord.modules.spb.tables import (
    Tables,
)
from amarcord.qt.properties import (
    PropertyChoice,
    PropertyDouble,
    PropertyInt,
    PropertyString,
    PropertyTags,
    RichPropertyType,
)
from amarcord.util import dict_union, remove_duplicates_stable

logger = logging.getLogger(__name__)


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
    description: str
    suffix: Optional[str]
    rich_property_type: RichPropertyType


Connection = Any


def _schema_to_property_type(
    json_schema: Dict[str, Any], suffix: Optional[str]
) -> RichPropertyType:
    parsed_schema = parse_schema_type(json_schema)
    if isinstance(parsed_schema, JSONSchemaNumber):
        return PropertyDouble(
            range=None,
            suffix=suffix,
        )
    if isinstance(parsed_schema, JSONSchemaInteger):
        return PropertyInt(range=None)
    if isinstance(parsed_schema, JSONSchemaArray):
        assert isinstance(
            parsed_schema.value_type, JSONSchemaString
        ), "arrays of non-strings aren't supported yet"
        assert (
            parsed_schema.value_type.enum_ is None
        ), "arrays of enum strings aren't supported yet"
        return PropertyTags()
    if isinstance(parsed_schema, JSONSchemaString):
        if parsed_schema.enum_ is not None:
            return PropertyChoice([(s, s) for s in parsed_schema.enum_])
        return PropertyString()
    raise Exception(f'invalid schema type "{type(parsed_schema)}"')


def _property_type_to_schema(rp: RichPropertyType) -> Dict[str, Any]:
    if isinstance(rp, PropertyInt):
        result_int: Dict[str, Any] = {"type": "number"}
        if rp.range is not None:
            result_int["minimum"] = rp.range[0]
            result_int["maximum"] = rp.range[1]
        return result_int
    if isinstance(rp, PropertyDouble):
        result_double: Dict[str, Any] = {"type": "number"}
        if rp.range is not None:
            result_double["minimum"] = rp.range[0]
            result_double["maximum"] = rp.range[1]
        return result_double
    if isinstance(rp, PropertyString):
        return {"type": "string"}
    raise Exception(f"invalid property type {type(rp)}")


def _decode_custom_to_values(custom: Mapping[str, Any]) -> Dict[RunProperty, Any]:
    result: Dict[RunProperty, Any] = {}
    for source in custom.keys() - {"manual"}:
        values: Dict[str, Any] = custom[source]
        assert isinstance(
            values, dict
        ), f'custom source data isn\'t a dict for source "{source}"'
        for run_property_str, value in values.items():
            run_property = RunProperty(run_property_str)
            if run_property in result:
                logger.warning(
                    "source values overlap for %s, overriding (introduce priorities!)",
                    run_property_str,
                )
            result[run_property] = value
    if "manual" in custom:
        manual_values: Dict[str, Any] = custom["manual"]
        assert isinstance(
            manual_values, dict
        ), f"custom source data isn't a dict for manual source"
        for run_property_str, value in manual_values.items():
            result[RunProperty(run_property_str)] = value
    return result


@dataclass(frozen=True)
class RunPropertyMetadata:
    name: str
    description: str
    suffix: Optional[str]
    rich_prop_type: Optional[RichPropertyType]


class SPBQueries:
    def __init__(self, dbcontext: DBContext, tables: Tables) -> None:
        self.dbcontext = dbcontext
        self.tables = tables

    def retrieve_runs(
        self, conn: Connection, proposal_id: ProposalId
    ) -> List[Dict[RunProperty, Any]]:
        run = self.tables.run
        comment = self.tables.run_comment

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
                    run.c.custom,
                ]
            )
            .select_from(run.outerjoin(comment))
            .where(run.c.proposal_id == proposal_id)
            .order_by(run.c.id, comment.c.id)
        )
        result: List[Dict[RunProperty, Any]] = []
        before = time()
        select_results = conn.execute(select_stmt).fetchall()
        after = time()
        logger.info("Retrieved runs in %ss", after - before)
        for _run_id, run_rows in groupby(
            select_results,
            lambda x: x["id"],
        ):
            rows = list(run_rows)
            run_meta = rows[0]
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
            assert isinstance(custom, dict), f"custom column has type {type(custom)}"
            result.append(
                dict_union(
                    [
                        {
                            self.tables.property_comments: comments,
                        },
                        {
                            self.tables.property_run_id: run_meta["id"],
                            self.tables.property_sample: run_meta["sample_id"],
                            self.tables.property_proposal_id: run_meta["proposal_id"],
                        },
                        _decode_custom_to_values(run_meta["custom"]),
                    ]
                )
            )
        return result

    def retrieve_run_ids(
        self, conn: Connection, proposal_id: ProposalId
    ) -> List[RunId]:
        return [
            RunId(row[0])
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

    def retrieve_run(self, conn: Connection, run_id: RunId) -> Run:
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
                    run.c.custom,
                ]
            )
            .select_from(run.outerjoin(comment))
            .where(run_c.id == run_id)
        )
        run_rows = conn.execute(select_statement).fetchall()
        if not run_rows:
            raise Exception(f"couldn't find any runs with id {run_id}")
        run_meta = run_rows[0]
        custom = run_meta["custom"]
        return Run(
            properties=dict_union(
                [
                    {
                        self.tables.property_run_id: run_meta["id"],
                        self.tables.property_sample: run_meta["sample_id"],
                        self.tables.property_proposal_id: run_meta["proposal_id"],
                    },
                    {
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
                    _decode_custom_to_values(custom),
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
            CustomRunProperty(
                name=RunProperty(row[0]),
                description=row[1],
                suffix=row[2],
                rich_property_type=_schema_to_property_type(
                    json_schema=row[3], suffix=row[2]
                ),
            )
            for row in conn.execute(
                sa.select(
                    [
                        self.tables.custom_run_property.c.name,
                        self.tables.custom_run_property.c.description,
                        self.tables.custom_run_property.c.suffix,
                        self.tables.custom_run_property.c.json_schema,
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
                    k: RunPropertyMetadata(
                        name=str(k), description=str(k), rich_prop_type=v, suffix=None
                    )
                    for k, v in self.tables.property_types.items()
                },
                {
                    c.name: RunPropertyMetadata(
                        name=str(c.name),
                        description=c.description,
                        rich_prop_type=c.rich_property_type,
                        suffix=c.suffix,
                    )
                    for c in custom_props
                },
                {
                    self.tables.property_comments: RunPropertyMetadata(
                        name="comments",
                        description="Comments",
                        rich_prop_type=None,
                        suffix=None,
                    )
                },
            ]
        )

    def update_run_property(
        self, conn: Connection, run_id: int, runprop: RunProperty, value: Any
    ) -> None:
        if runprop == self.tables.property_sample:
            assert isinstance(value, int), "sample ID should be an integer"

            conn.execute(
                sa.update(self.tables.run)
                .where(self.tables.run.c.id == run_id)
                .values({self.tables.run.c.sample_id: value})
            )
            return

        assert isinstance(
            value, (str, int, float, list)
        ), f"custom properties can only have str, int and float values currently, got {type(value)}"

        with conn.begin():
            current_json = conn.execute(
                sa.select([self.tables.run.c.custom]).where(
                    self.tables.run.c.id == run_id
                )
            ).first()[0]
            assert isinstance(
                current_json, dict
            ), f"custom column should be dictionary, got {type(current_json)}"
            if "manual" not in current_json:
                current_json["manual"] = {}
            manual = current_json["manual"]
            assert isinstance(
                manual, dict
            ), f"manual input should be dictionary, not {type(manual)}"
            manual[str(runprop)] = value
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
        self,
        conn: Connection,
        name: str,
        description: str,
        suffix: Optional[str],
        prop_type: RichPropertyType,
    ) -> None:
        conn.execute(
            self.tables.custom_run_property.insert().values(
                name=name,
                description=description,
                suffix=suffix,
                json_schema=_property_type_to_schema(prop_type),
            )
        )
