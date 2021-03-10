import logging
from typing import Dict

import sqlalchemy as sa
from sqlalchemy import ForeignKey, func

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi import (
    DBAttributo,
    PropertyComments,
    PropertyDateTime,
    PropertyInt,
    PropertySample,
)
from amarcord.db.attributo_id import AttributoId
from amarcord.modules.dbcontext import DBContext

logger = logging.getLogger(__name__)


def _table_attributo(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Attributo",
        metadata,
        sa.Column("name", sa.String(length=255), primary_key=True),
        sa.Column("description", sa.String(length=255)),
        sa.Column("suffix", sa.String(length=255)),
        sa.Column(
            "associated_table",
            sa.Enum(AssociatedTable),
            nullable=False,
            primary_key=True,
        ),
        sa.Column("json_schema", sa.JSON, nullable=False),
    )


def _table_target(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Target",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String, nullable=False),
        sa.Column("short_name", sa.String, nullable=False),
        sa.Column("molecular_weight", sa.Float, nullable=True),
        sa.Column("uniprot_id", sa.String(length=64), nullable=True),
    )


def _table_sample(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Sample",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("target_id", sa.Integer, sa.ForeignKey("Target.id"), nullable=False),
        sa.Column("average_crystal_size", sa.Float, nullable=True),
        sa.Column("crystal_shape", sa.JSON, nullable=True),
        sa.Column("created", sa.DateTime, nullable=False, server_default=func.now()),
        sa.Column("incubation_time", sa.DateTime, nullable=True),
        sa.Column("crystallization_temperature", sa.Float, nullable=True),
        sa.Column("protein_concentration", sa.Float, nullable=True),
        sa.Column("shaking_time_seconds", sa.Integer, nullable=True),
        sa.Column("shaking_strength", sa.Float, nullable=True),
        sa.Column("comment", sa.String(length=255), nullable=False, default=""),
        sa.Column("crystal_settlement_volume", sa.Float, nullable=True),
        sa.Column("seed_stock_used", sa.String, nullable=False, default=""),
        sa.Column("plate_origin", sa.String, nullable=False, default=""),
        sa.Column("creator", sa.String, nullable=False, default=""),
        sa.Column("crystallization_method", sa.String, nullable=False, default=""),
        sa.Column("filters", sa.JSON, nullable=True),
        sa.Column("compounds", sa.JSON, nullable=True),
        sa.Column("micrograph", sa.Text, nullable=True),
        sa.Column("protocol", sa.Text, nullable=True),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column("attributi", sa.JSON, nullable=False),
    )


def _table_run_comment(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "RunComment",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("run_id", sa.Integer, sa.ForeignKey("Run.id")),
        sa.Column("author", sa.String(length=255), nullable=False),
        sa.Column("comment_text", sa.String(length=255), nullable=False),
        sa.Column("created", sa.DateTime(), nullable=False),
    )


def _table_proposal(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Proposal",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("metadata", sa.JSON, nullable=True),
    )


def _table_run(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Run",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column(
            "proposal_id",
            sa.Integer,
            sa.ForeignKey("Proposal.id"),
            nullable=False,
        ),
        sa.Column("sample_id", sa.Integer, ForeignKey("Sample.id"), nullable=True),
        sa.Column("karabo", sa.BLOB, nullable=True),
        sa.Column("attributi", sa.JSON, nullable=False),
    )


class DBTables:
    def __init__(
        self,
        sample: sa.Table,
        proposal: sa.Table,
        run: sa.Table,
        run_comment: sa.Table,
        attributo: sa.Table,
        target: sa.Table,
    ) -> None:
        self.sample = sample
        self.proposal = proposal
        self.run = run
        self.run_comment = run_comment
        self.target = target
        self.attributo = attributo
        self.attributo_run_id = AttributoId("id")
        self.attributo_run_comments = AttributoId("comments")
        self.attributo_run_modified = AttributoId("modified")
        self.attributo_run_sample_id = AttributoId("sample_id")
        self.attributo_run_proposal_id = AttributoId("proposal_id")
        self.attributo_run_id = AttributoId("id")
        self.additional_attributi: Dict[
            AssociatedTable, Dict[AttributoId, DBAttributo]
        ] = {
            AssociatedTable.RUN: {
                self.attributo_run_id: DBAttributo(
                    name=self.attributo_run_id,
                    description="Run ID",
                    suffix=None,
                    associated_table=AssociatedTable.RUN,
                    rich_property_type=PropertyInt(),
                ),
                self.attributo_run_comments: DBAttributo(
                    name=self.attributo_run_comments,
                    description="Comments",
                    suffix=None,
                    associated_table=AssociatedTable.RUN,
                    rich_property_type=PropertyComments(),
                ),
                self.attributo_run_modified: DBAttributo(
                    name=self.attributo_run_modified,
                    description="Modified",
                    suffix=None,
                    associated_table=AssociatedTable.RUN,
                    rich_property_type=PropertyDateTime(),
                ),
                self.attributo_run_sample_id: DBAttributo(
                    name=self.attributo_run_sample_id,
                    description="Sample ID",
                    suffix=None,
                    associated_table=AssociatedTable.RUN,
                    rich_property_type=PropertySample(),
                ),
                self.attributo_run_proposal_id: DBAttributo(
                    name=self.attributo_run_proposal_id,
                    description="Proposal",
                    suffix=None,
                    associated_table=AssociatedTable.RUN,
                    rich_property_type=PropertyInt(),
                ),
            }
        }
        self.property_karabo = AttributoId(self.run.c.karabo.name)
        self.property_attributi = AttributoId(self.run.c.attributi.name)


def create_tables(context: DBContext) -> DBTables:
    return DBTables(
        sample=_table_sample(context.metadata),
        proposal=_table_proposal(context.metadata),
        run=_table_run(context.metadata),
        run_comment=_table_run_comment(context.metadata),
        attributo=_table_attributo(context.metadata),
        target=_table_target(context.metadata),
    )
