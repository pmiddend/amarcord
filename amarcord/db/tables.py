import logging
from typing import Dict

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import MetaData

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeComments
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeSample
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.event_log_level import EventLogLevel
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


def _table_sample(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Sample",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "proposal_id",
            sa.Integer,
            sa.ForeignKey("Proposal.id", use_alter=True),
            nullable=True,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
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
        sa.Column("run_id", sa.Integer, sa.ForeignKey("Run.id", ondelete="cascade")),
        sa.Column("author", sa.String(length=255), nullable=False),
        sa.Column("comment_text", sa.String(length=255), nullable=False),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )


def _table_proposal(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Proposal",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True, autoincrement=False),
        sa.Column("metadata", sa.JSON, nullable=True),
        sa.Column("admin_password", sa.String(length=255), nullable=True),
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
        sa.Column("attributi", sa.JSON, nullable=False),
    )


def _table_event_log(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "EventLog",
        metadata,
        sa.Column(
            "id",
            sa.Integer,
            primary_key=True,
        ),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("level", sa.Enum(EventLogLevel), nullable=False),
        sa.Column("source", sa.String(length=255), nullable=False),
        sa.Column("text", sa.Text, nullable=False),
    )


class DBTables:
    def __init__(
        self,
        sample: sa.Table,
        proposal: sa.Table,
        run: sa.Table,
        run_comment: sa.Table,
        attributo: sa.Table,
        event_log: sa.Table,
    ) -> None:
        self.event_log = event_log
        self.sample = sample
        self.proposal = proposal
        self.run = run
        self.run_comment = run_comment
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
            AssociatedTable.SAMPLE: {
                AttributoId("id"): DBAttributo(
                    name=AttributoId("id"),
                    description="Sample ID",
                    associated_table=AssociatedTable.SAMPLE,
                    attributo_type=AttributoTypeInt(),
                ),
                AttributoId("name"): DBAttributo(
                    name=AttributoId("name"),
                    description="Name",
                    associated_table=AssociatedTable.SAMPLE,
                    attributo_type=AttributoTypeString(),
                ),
                AttributoId("created"): DBAttributo(
                    name=AttributoId("created"),
                    description="Created",
                    associated_table=AssociatedTable.SAMPLE,
                    attributo_type=AttributoTypeDateTime(),
                ),
                AttributoId("micrograph"): DBAttributo(
                    name=AttributoId("micrograph"),
                    description="Micrograph",
                    associated_table=AssociatedTable.SAMPLE,
                    attributo_type=AttributoTypeString(),
                ),
                AttributoId("protocol"): DBAttributo(
                    name=AttributoId("protocol"),
                    description="Protocol",
                    associated_table=AssociatedTable.SAMPLE,
                    attributo_type=AttributoTypeString(),
                ),
            },
            AssociatedTable.RUN: {
                self.attributo_run_id: DBAttributo(
                    name=self.attributo_run_id,
                    description="Run ID",
                    associated_table=AssociatedTable.RUN,
                    attributo_type=AttributoTypeInt(),
                ),
                self.attributo_run_comments: DBAttributo(
                    name=self.attributo_run_comments,
                    description="Comments",
                    associated_table=AssociatedTable.RUN,
                    attributo_type=AttributoTypeComments(),
                ),
                self.attributo_run_modified: DBAttributo(
                    name=self.attributo_run_modified,
                    description="Modified",
                    associated_table=AssociatedTable.RUN,
                    attributo_type=AttributoTypeDateTime(),
                ),
                self.attributo_run_sample_id: DBAttributo(
                    name=self.attributo_run_sample_id,
                    description="Sample ID",
                    associated_table=AssociatedTable.RUN,
                    attributo_type=AttributoTypeSample(),
                ),
                self.attributo_run_proposal_id: DBAttributo(
                    name=self.attributo_run_proposal_id,
                    description="Proposal",
                    associated_table=AssociatedTable.RUN,
                    attributo_type=AttributoTypeInt(),
                ),
            },
        }


def create_tables_from_metadata(metadata: MetaData) -> DBTables:
    return DBTables(
        sample=_table_sample(metadata),
        proposal=_table_proposal(metadata),
        run=_table_run(metadata),
        run_comment=_table_run_comment(metadata),
        attributo=_table_attributo(metadata),
        event_log=_table_event_log(metadata),
    )


def create_tables(context: DBContext) -> DBTables:
    return create_tables_from_metadata(context.metadata)
