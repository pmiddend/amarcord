import datetime
import logging
import pickle
from dataclasses import dataclass
from itertools import groupby
from time import time
from typing import Any, Dict, List, Mapping, Optional, Set, Tuple, Union

import sqlalchemy as sa
from sqlalchemy import and_

from amarcord.json_schema import (
    JSONSchemaArray,
    JSONSchemaInteger,
    JSONSchemaNumber,
    JSONSchemaString,
    JSONSchemaStringFormat,
    parse_schema_type,
)
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.json import JSONDict
from amarcord.modules.spb.constants import MANUAL_SOURCE_NAME
from amarcord.modules.spb.proposal_id import ProposalId
from amarcord.modules.spb.run_property import (
    RunProperty,
)
from amarcord.modules.spb.db_tables import (
    DBTables,
)
from amarcord.numeric_range import NumericRange
from amarcord.qt.properties import (
    PropertyChoice,
    PropertyDateTime,
    PropertyDouble,
    PropertyInt,
    PropertySample,
    PropertyString,
    PropertyTags,
    RichPropertyType,
)
from amarcord.util import dict_union, remove_duplicates_stable

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class DBRunComment:
    id: Optional[int]
    run_id: int
    author: str
    text: str
    created: datetime.datetime


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
    crystal_buffer: Optional[str]
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


Karabo = Tuple[Dict[str, Any], Dict[str, Any]]

RunPropertyValue = Union[List[DBRunComment], str, int, float, List[str]]


@dataclass(frozen=True)
class DBRun:
    properties: Dict[RunProperty, RunPropertyValue]
    karabo: Optional[Karabo]
    manual_properties: Set[RunProperty]


@dataclass(frozen=True)
class CustomRunProperty:
    name: RunProperty
    description: str
    suffix: Optional[str]
    rich_property_type: RichPropertyType


Connection = Any


def _schema_to_property_type(
    json_schema: JSONDict, suffix: Optional[str]
) -> RichPropertyType:
    parsed_schema = parse_schema_type(json_schema)
    if isinstance(parsed_schema, JSONSchemaNumber):
        return PropertyDouble(
            range=None
            if parsed_schema.minimum is None
            and parsed_schema.maximum is None
            and parsed_schema.exclusiveMaximum is None
            and parsed_schema.exclusiveMinimum is None
            else NumericRange(
                parsed_schema.minimum
                if parsed_schema.minimum is not None
                else parsed_schema.exclusiveMinimum,
                parsed_schema.exclusiveMinimum is not None,
                parsed_schema.maximum
                if parsed_schema.maximum is not None
                else parsed_schema.exclusiveMaximum,
                parsed_schema.exclusiveMaximum is not None,
            ),
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
        if parsed_schema.format_ == JSONSchemaStringFormat.DATE_TIME:
            return PropertyDateTime()
        return PropertyString()
    raise Exception(f'invalid schema type "{type(parsed_schema)}"')


def property_type_to_schema(rp: RichPropertyType) -> JSONDict:
    if isinstance(rp, PropertyInt):
        result_int: Dict[str, Any] = {"type": "number"}
        if rp.range is not None:
            result_int["minimum"] = rp.range[0]
            result_int["maximum"] = rp.range[1]
        return result_int
    if isinstance(rp, PropertyDouble):
        result_double: Dict[str, Any] = {"type": "number"}
        if rp.range is not None:
            if rp.range.minimum is not None:
                if rp.range.minimum_inclusive:
                    result_double["minimum"] = rp.range.minimum
                else:
                    result_double["exclusiveMinimum"] = rp.range.minimum
            if rp.range.maximum is not None:
                if rp.range.maximum_inclusive:
                    result_double["maximum"] = rp.range.maximum
                else:
                    result_double["exclusiveMaximum"] = rp.range.maximum
        return result_double
    if isinstance(rp, PropertyString):
        return {"type": "string"}
    if isinstance(rp, PropertySample):
        return {"type": "integer"}
    if isinstance(rp, PropertyChoice):
        return {"type": "string", "enum": [v[1] for v in rp.values]}
    if isinstance(rp, PropertyDateTime):
        return {"type": "string", "format": "date-time"}
    if isinstance(rp, PropertyTags):
        return {"type": "array", "items": {"type": "string"}}
    raise Exception(f"invalid property type {type(rp)}")


Source = str


@dataclass(frozen=True)
class PropertyValueWithSource:
    value: Any
    source: Source


def _decode_custom_to_values(
    custom: Mapping[str, Any]
) -> Dict[RunProperty, PropertyValueWithSource]:
    result: Dict[RunProperty, PropertyValueWithSource] = {}
    for source in custom.keys() - {MANUAL_SOURCE_NAME}:
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
            result[run_property] = PropertyValueWithSource(value=value, source=source)
    if MANUAL_SOURCE_NAME in custom:
        manual_values: Dict[str, Any] = custom[MANUAL_SOURCE_NAME]
        assert isinstance(
            manual_values, dict
        ), "custom source data isn't a dict for manual source"
        for run_property_str, value in manual_values.items():
            result[RunProperty(run_property_str)] = PropertyValueWithSource(
                value, source=MANUAL_SOURCE_NAME
            )
    return result


@dataclass(frozen=True)
class DBRunPropertyMetadata:
    name: str
    description: str
    suffix: Optional[str]
    rich_prop_type: Optional[RichPropertyType]


class DB:
    def __init__(self, dbcontext: DBContext, tables: DBTables) -> None:
        self.dbcontext = dbcontext
        self.tables = tables

    def retrieve_runs(
        self,
        conn: Connection,
        proposal_id: ProposalId,
        since: Optional[datetime.datetime],
    ) -> List[Dict[RunProperty, RunPropertyValue]]:
        logger.info("retrieving runs since %s", since)

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
                    run.c.custom,
                ]
            )
            .select_from(run.outerjoin(comment))
            .where(where_condition)
            .order_by(run.c.id, comment.c.id)
        )
        result: List[Dict[RunProperty, RunPropertyValue]] = []
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
            custom = run_meta["custom"]
            assert isinstance(custom, dict), f"custom column has type {type(custom)}"
            run_dict: Dict[RunProperty, RunPropertyValue] = {}
            run_dict.update(
                {
                    self.tables.property_comments: comments,
                    self.tables.property_run_id: run_meta["id"],
                    self.tables.property_sample: run_meta["sample_id"],
                    self.tables.property_proposal_id: run_meta["proposal_id"],
                }
            )
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
                        {
                            k: v.value
                            for k, v in _decode_custom_to_values(
                                run_meta["custom"]
                            ).items()
                        },
                    ]
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
        custom_decoded = _decode_custom_to_values(custom)
        return DBRun(
            properties=dict_union(
                [
                    {
                        self.tables.property_run_id: run_meta["id"],
                        self.tables.property_sample: run_meta["sample_id"],
                        self.tables.property_proposal_id: run_meta["proposal_id"],
                        self.tables.property_modified: run_meta["modified"],
                    },
                    {
                        self.tables.property_comments: remove_duplicates_stable(
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
                    {k: v.value for k, v in custom_decoded.items()},
                ],
            ),
            manual_properties={self.tables.property_sample}
            | {k for k, v in custom_decoded.items() if v.source == MANUAL_SOURCE_NAME},
            karabo=None,
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
                    custom={},
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
    ) -> Dict[RunProperty, DBRunPropertyMetadata]:
        custom_props = self.custom_run_properties(conn)
        return dict_union(
            [
                {
                    k: DBRunPropertyMetadata(
                        name=str(k),
                        description=self.tables.property_descriptions.get(k, str(k)),
                        rich_prop_type=v,
                        suffix=None,
                    )
                    for k, v in self.tables.property_types.items()
                },
                {
                    c.name: DBRunPropertyMetadata(
                        name=str(c.name),
                        description=c.description,
                        rich_prop_type=c.rich_property_type,
                        suffix=c.suffix,
                    )
                    for c in custom_props
                },
                {
                    self.tables.property_comments: DBRunPropertyMetadata(
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
                .values(sample_id=value, modified=datetime.datetime.utcnow())
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
            if MANUAL_SOURCE_NAME not in current_json:
                current_json[MANUAL_SOURCE_NAME] = {}
            manual = current_json[MANUAL_SOURCE_NAME]
            assert isinstance(
                manual, dict
            ), f"manual input should be dictionary, not {type(manual)}"
            manual[str(runprop)] = value
            conn.execute(
                sa.update(self.tables.run)
                .where(self.tables.run.c.id == run_id)
                .values(custom=current_json, modified=datetime.datetime.utcnow())
            )

    def connect(self) -> Connection:
        return self.dbcontext.connect()

    def retrieve_karabo(self, conn: Connection, run_id: int) -> Optional[Karabo]:
        result = conn.execute(
            sa.select([self.tables.run.c.karabo]).where(self.tables.run.c.id == run_id)
        ).fetchall()
        return (
            pickle.loads(result[0][0]) if result and result[0][0] is not None else None
        )

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

    def retrieve_samples(self, conn: Connection) -> List[DBSample]:
        tc = self.tables.sample.c
        return [
            DBSample(
                id=row["id"],
                created=row["created"],
                target_id=row["target_id"],
                average_crystal_size=row["average_crystal_size"],
                crystal_shape=row["crystal_shape"],
                incubation_time=row["incubation_time"],
                crystal_buffer=row["crystal_buffer"],
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
            )
            for row in conn.execute(
                sa.select(
                    [
                        tc.id,
                        tc.created,
                        tc.average_crystal_size,
                        tc.target_id,
                        tc.crystal_shape,
                        tc.incubation_time,
                        tc.crystal_buffer,
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
                    ]
                ).order_by(tc.id)
            ).fetchall()
        ]

    def add_sample(self, conn: Connection, t: DBSample) -> None:
        conn.execute(
            sa.insert(self.tables.sample).values(
                target_id=t.target_id,
                average_crystal_size=t.average_crystal_size,
                crystal_shape=t.crystal_shape,
                incubation_time=t.incubation_time,
                crystal_buffer=t.crystal_buffer,
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
                crystal_buffer=t.crystal_buffer,
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
            )
            .where(self.tables.sample.c.id == t.id)
        )

    def delete_sample(self, conn: Connection, tid: int) -> None:
        conn.execute(
            sa.delete(self.tables.sample).where(self.tables.sample.c.id == tid)
        )
