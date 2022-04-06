import logging

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import LONGBLOB
from sqlalchemy.sql import ColumnElement

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.event_log_level import EventLogLevel

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
        sa.Column("description", sa.String(length=255), nullable=False),
        sa.Column("group", sa.String(length=255), nullable=False),
        sa.Column(
            "associated_table",
            sa.Enum(AssociatedTable),
            nullable=False,
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
        sa.Column("size_in_bytes", sa.Integer(), nullable=False),
        sa.Column("original_path", sa.Text(), nullable=True),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("modified", sa.DateTime(), nullable=False),
        # Seehttps://stackoverflow.com/questions/43791725/sqlalchemy-how-to-make-a-longblob-column-in-mysql
        sa.Column(
            "contents", sa.LargeBinary().with_variant(LONGBLOB, "mysql"), nullable=False
        ),
        sa.Column("description", sa.String(length=255)),
    )


def _table_experiment_has_attributo(
    metadata: sa.MetaData, attributo: sa.Table
) -> sa.Table:
    return sa.Table(
        "ExperimentHasAttributo",
        metadata,
        sa.Column("experiment_type", sa.String(length=255), nullable=False),
        sa.Column(
            "attributo_name",
            sa.String(length=255),
            # If the attributo vanishes, delete the experiment type with it
            ForeignKey(
                _fk_identifier(attributo.c.name), ondelete="cascade", onupdate="cascade"
            ),
            nullable=False,
        ),
    )


def _table_data_set(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "DataSet",
        metadata,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("experiment_type", sa.String(length=255), nullable=False),
        sa.Column("attributi", sa.JSON, nullable=False),
    )


def _table_cfel_analysis_result_has_file(
    metadata: sa.MetaData, cfel_analysis_result: sa.Table, file: sa.Table
) -> sa.Table:
    return sa.Table(
        "CFELAnalysisResultHasFile",
        metadata,
        sa.Column(
            "analysis_result_id",
            sa.Integer(),
            # If the analysis result vanishes, delete this row here
            ForeignKey(_fk_identifier(cfel_analysis_result.c.id), ondelete="cascade"),
        ),
        sa.Column(
            "file_id",
            sa.Integer(),
            # If the file vanishes (why would it?), delete this entry
            ForeignKey(_fk_identifier(file.c.id), ondelete="cascade"),
        ),
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
            # If the sample vanishes, delete this entry as well
            ForeignKey(_fk_identifier(sample.c.id), ondelete="cascade"),
        ),
        sa.Column(
            "file_id",
            sa.Integer(),
            # If the file vanishes, delete this entry as well
            ForeignKey(_fk_identifier(file.c.id), ondelete="cascade"),
        ),
    )


def _table_run_has_file(
    metadata: sa.MetaData, run: sa.Table, file: sa.Table
) -> sa.Table:
    return sa.Table(
        "RunHasFile",
        metadata,
        sa.Column(
            "run_id",
            sa.Integer(),
            # If the run vanishes, delete this entry as well
            ForeignKey(_fk_identifier(run.c.id), ondelete="cascade"),
        ),
        sa.Column(
            "file_id",
            sa.Integer(),
            # If the file vanishes, delete this entry as well
            ForeignKey(_fk_identifier(file.c.id), ondelete="cascade"),
        ),
    )


def _table_event_has_file(
    metadata: sa.MetaData, event_log: sa.Table, file: sa.Table
) -> sa.Table:
    return sa.Table(
        "EventHasFile",
        metadata,
        sa.Column(
            "event_id",
            sa.Integer(),
            # If the run vanishes, delete this entry as well
            ForeignKey(_fk_identifier(event_log.c.id), ondelete="cascade"),
        ),
        sa.Column(
            "file_id",
            sa.Integer(),
            # If the file vanishes, delete this entry as well
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


def _table_cfel_analysis_results(metadata: sa.MetaData, data_set: sa.Table) -> sa.Table:
    return sa.Table(
        "CFELAnalysisResults",
        metadata,
        sa.Column("directory_name", sa.String(length=255), nullable=False),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "data_set_id",
            sa.Integer,
            # If the data set vanishes, delete the corresponding analysis result as well
            ForeignKey(_fk_identifier(data_set.c.id), ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("resolution", sa.String(length=255), nullable=False),
        sa.Column("rsplit", sa.Float, nullable=False),
        sa.Column("cchalf", sa.Float, nullable=False),
        sa.Column("ccstar", sa.Float, nullable=False),
        sa.Column("snr", sa.Float, nullable=False),
        sa.Column("completeness", sa.Float, nullable=False),
        sa.Column("multiplicity", sa.Float, nullable=False),
        sa.Column("total_measurements", sa.Integer, nullable=False),
        sa.Column("unique_reflections", sa.Integer, nullable=False),
        sa.Column("num_patterns", sa.Integer, nullable=False),
        sa.Column("num_hits", sa.Integer, nullable=False),
        sa.Column("indexed_patterns", sa.Integer, nullable=False),
        sa.Column("indexed_crystals", sa.Integer, nullable=False),
        sa.Column("crystfel_version", sa.String(length=64), nullable=False),
        sa.Column("ccstar_rsplit", sa.Float, nullable=True),
        sa.Column("created", sa.DateTime(), nullable=False),
    )


def _table_run(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Run",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("modified", sa.DateTime, nullable=False),
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
        run: sa.Table,
        attributo: sa.Table,
        event_log: sa.Table,
        cfel_analysis_results: sa.Table,
        cfel_analysis_result_has_file: sa.Table,
        experiment_has_attributo: sa.Table,
        file: sa.Table,
        data_set: sa.Table,
        sample_has_file: sa.Table,
        run_has_file: sa.Table,
        event_has_file: sa.Table,
    ) -> None:
        self.event_has_file = event_has_file
        self.cfel_analysis_result_has_file = cfel_analysis_result_has_file
        self.data_set = data_set
        self.experiment_has_attributo = experiment_has_attributo
        self.run_has_file = run_has_file
        self.event_log = event_log
        self.sample = sample
        self.run = run
        self.attributo = attributo
        self.cfel_analysis_results = cfel_analysis_results
        self.file = file
        self.sample_has_file = sample_has_file


def create_tables_from_metadata(metadata: MetaData) -> DBTables:
    sample = _table_sample(metadata)
    run = _table_run(metadata)
    file = _table_file(metadata)
    table_attributo = _table_attributo(metadata)
    data_set = _table_data_set(metadata)
    cfel_analysis_results = _table_cfel_analysis_results(metadata, data_set)
    event_log = _table_event_log(metadata)
    return DBTables(
        sample=sample,
        run=run,
        attributo=table_attributo,
        event_log=event_log,
        data_set=data_set,
        cfel_analysis_results=cfel_analysis_results,
        experiment_has_attributo=_table_experiment_has_attributo(
            metadata, table_attributo
        ),
        file=file,
        sample_has_file=_table_sample_has_file(metadata, sample, file),
        cfel_analysis_result_has_file=_table_cfel_analysis_result_has_file(
            metadata, cfel_analysis_results, file
        ),
        run_has_file=_table_run_has_file(metadata, run, file),
        event_has_file=_table_event_has_file(metadata, event_log, file),
    )
