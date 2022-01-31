import logging
from typing import Dict

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import MetaData
from sqlalchemy.sql import ColumnElement

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeComments
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeSample
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.dbcontext import DBContext
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.job_status import DBJobStatus

logger = logging.getLogger(__name__)


def _fk_identifier(c: ColumnElement) -> str:
    if c.table.schema is not None:
        return f"{c.table.schema}.{c.table.name}.{c.name}"
    return f"{c.table.name}.{c.name}"


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


def _table_file(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "File",
        metadata,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("type", sa.String(length=255), nullable=False),
        sa.Column("file_name", sa.String(length=255), nullable=False),
        sa.Column("sha256", sa.String(length=40), nullable=False),
        sa.Column("modified", sa.DateTime(), nullable=False),
        sa.Column("contents", sa.LargeBinary(), nullable=False),
        sa.Column("description", sa.String(length=255)),
    )


def _table_sample_has_file(
    metadata: sa.MetaData, sample: sa.Table, file: sa.Table
) -> sa.Table:
    return sa.Table(
        "SampleHasFile",
        metadata,
        sa.Column(
            "sample_id",
            sa.Integer(),
            ForeignKey(_fk_identifier(sample.c.id), ondelete="cascade"),
        ),
        sa.Column(
            "file_id",
            sa.Integer(),
            ForeignKey(_fk_identifier(file.c.id), ondelete="cascade"),
        ),
    )


def _table_sample(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Sample",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column("attributi", sa.JSON, nullable=False),
    )


def _table_cfel_analysis_results(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "CFELAnalysisResults",
        metadata,
        sa.Column("directory_name", sa.String(length=255), nullable=False),
        sa.Column("run_from", sa.Integer, nullable=False),
        sa.Column("run_to", sa.Integer, nullable=False),
        sa.Column("resolution", sa.String(length=255), nullable=False),
        sa.Column("rsplit", sa.Float, nullable=False),
        sa.Column("cchalf", sa.Float, nullable=False),
        sa.Column("ccstar", sa.Float, nullable=False),
        sa.Column("snr", sa.Float, nullable=False),
        sa.Column("completeness", sa.Float, nullable=False),
        sa.Column("multiplicity", sa.Float, nullable=False),
        sa.Column("total_measurements", sa.Integer, nullable=False),
        sa.Column("unique_reflections", sa.Integer, nullable=False),
        sa.Column("wilson_b", sa.Float, nullable=False),
        sa.Column("outer_shell", sa.String(length=255), nullable=False),
        sa.Column("num_patterns", sa.Integer, nullable=False),
        sa.Column("num_hits", sa.Integer, nullable=False),
        sa.Column("indexed_patterns", sa.Integer, nullable=False),
        sa.Column("indexed_crystals", sa.Integer, nullable=False),
        sa.Column("comment", sa.String(length=255), nullable=False),
    )


def _table_run_comment(metadata: sa.MetaData, run: sa.Table) -> sa.Table:
    return sa.Table(
        "RunComment",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "run_id",
            sa.Integer,
            sa.ForeignKey(_fk_identifier(run.c.id), ondelete="cascade"),
        ),
        sa.Column("author", sa.String(length=255), nullable=False),
        sa.Column("comment_text", sa.String(length=255), nullable=False),
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
    )


def _table_run(metadata: sa.MetaData, sample: sa.Table) -> sa.Table:
    return sa.Table(
        "Run",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column(
            "sample_id",
            sa.Integer,
            ForeignKey(_fk_identifier(sample.c.id)),
            nullable=True,
        ),
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


def _table_indexing_jobs(metadata: sa.MetaData, run: sa.Table) -> sa.Table:
    return sa.Table(
        "IndexingJob",
        metadata,
        sa.Column(
            "id",
            sa.Integer,
            primary_key=True,
        ),
        sa.Column("started", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("stopped", sa.DateTime, nullable=True),
        sa.Column("status", sa.Enum(DBJobStatus), nullable=False),
        sa.Column("metadata", sa.JSON, nullable=False),
        sa.Column(
            "run_id",
            sa.Integer,
            ForeignKey(_fk_identifier(run.c.id), ondelete="cascade"),
            nullable=False,
        ),
    )


class DBTables:
    def __init__(
        self,
        sample: sa.Table,
        run: sa.Table,
        run_comment: sa.Table,
        attributo: sa.Table,
        event_log: sa.Table,
        cfel_analysis_results: sa.Table,
        file: sa.Table,
        sample_has_file: sa.Table,
        indexing_jobs: sa.Table,
    ) -> None:
        self.event_log = event_log
        self.sample = sample
        self.run = run
        self.run_comment = run_comment
        self.attributo = attributo
        self.cfel_analysis_results = cfel_analysis_results
        self.file = file
        self.sample_has_file = sample_has_file
        self.attributo_run_id = AttributoId("id")
        self.attributo_run_comments = AttributoId("comments")
        self.attributo_run_modified = AttributoId("modified")
        self.attributo_run_sample_id = AttributoId("sample_id")
        self.attributo_run_id = AttributoId("id")
        self.indexing_jobs = indexing_jobs
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
            },
        }


def create_tables_from_metadata(metadata: MetaData) -> DBTables:
    sample = _table_sample(metadata)
    run = _table_run(metadata, sample)
    file = _table_file(metadata)
    return DBTables(
        sample=sample,
        run=run,
        run_comment=_table_run_comment(metadata, run),
        attributo=_table_attributo(metadata),
        event_log=_table_event_log(metadata),
        cfel_analysis_results=_table_cfel_analysis_results(metadata),
        file=file,
        sample_has_file=_table_sample_has_file(metadata, sample, file),
        indexing_jobs=_table_indexing_jobs(metadata, run),
    )


def create_tables(context: DBContext) -> DBTables:
    return create_tables_from_metadata(context.metadata)
