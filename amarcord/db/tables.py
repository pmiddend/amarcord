import logging

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import LONGBLOB
from sqlalchemy.sql import ColumnElement

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.event_log_level import EventLogLevel

logger = logging.getLogger(__name__)


def _fk_identifier(c: ColumnElement) -> str:
    if c.table.schema is not None:
        return f"{c.table.schema}.{c.table.name}.{c.name}"
    return f"{c.table.name}.{c.name}"


def _table_configuration(metadata: sa.MetaData, experiment_type: sa.Table) -> sa.Table:
    return sa.Table(
        "UserConfiguration",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created", sa.DateTime, nullable=False),
        sa.Column("auto_pilot", sa.Boolean, nullable=False),
        sa.Column("use_online_crystfel", sa.Boolean, nullable=False),
        sa.Column(
            "current_experiment_type_id",
            sa.Integer,
            ForeignKey(_fk_identifier(experiment_type.c.id), ondelete="set null"),
            nullable=True,
        ),
    )


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
        # See https://stackoverflow.com/questions/43791725/sqlalchemy-how-to-make-a-longblob-column-in-mysql
        sa.Column(
            "contents", sa.LargeBinary().with_variant(LONGBLOB, "mysql"), nullable=False
        ),
        sa.Column("description", sa.String(length=255)),
    )


def _table_beamtime_schedule(metadata: sa.MetaData, chemical: sa.Table) -> sa.Table:
    return sa.Table(
        "BeamtimeSchedule",
        metadata,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "chemical_id",
            sa.Integer(),
            # If the chemical vanishes, delete this entry as well
            ForeignKey(_fk_identifier(chemical.c.id), ondelete="cascade"),
        ),
        sa.Column("users", sa.String(length=255), nullable=False),
        sa.Column("td_support", sa.String(length=255), nullable=False),
        sa.Column("comment", sa.Text(), nullable=False),
        sa.Column("shift", sa.String(length=255), nullable=False),
        sa.Column("date", sa.String(length=10), nullable=False),
    )


def _table_experiment_type(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "ExperimentType",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False, unique=True),
    )


def _table_experiment_has_attributo(
    metadata: sa.MetaData, attributo: sa.Table, experiment_type: sa.Table
) -> sa.Table:
    return sa.Table(
        "ExperimentHasAttributo",
        metadata,
        sa.Column(
            "experiment_type_id",
            sa.Integer,
            ForeignKey(_fk_identifier(experiment_type.c.id)),
            nullable=False,
        ),
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


def _table_data_set(metadata: sa.MetaData, experiment_type: sa.Table) -> sa.Table:
    return sa.Table(
        "DataSet",
        metadata,
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "experiment_type_id",
            sa.Integer,
            ForeignKey(_fk_identifier(experiment_type.c.id)),
            nullable=False,
        ),
        sa.Column("attributi", sa.JSON, nullable=False),
    )


def _table_chemical_has_file(
    metadata: sa.MetaData, chemical: sa.Table, file: sa.Table
) -> sa.Table:
    return sa.Table(
        "ChemicalHasFile",
        metadata,
        sa.Column(
            "chemical_id",
            sa.Integer(),
            # If the chemical vanishes, delete this entry as well
            ForeignKey(_fk_identifier(chemical.c.id), ondelete="cascade"),
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


def _table_chemical(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "Chemical",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column("attributi", sa.JSON, nullable=False),
    )


def _table_indexing_result(metadata: sa.MetaData, run: sa.Table) -> sa.Table:
    return sa.Table(
        "IndexingResult",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created", sa.DateTime, nullable=False),
        sa.Column(
            "run_id",
            sa.Integer,
            ForeignKey(_fk_identifier(run.c.id), ondelete="cascade"),
        ),
        sa.Column("stream_file", sa.Text(), nullable=True),
        sa.Column("frames", sa.Integer(), nullable=True),
        sa.Column("hits", sa.Integer(), nullable=True),
        sa.Column("not_indexed_frames", sa.Integer(), nullable=True),
        sa.Column("hit_rate", sa.Float(), nullable=False),
        sa.Column("indexing_rate", sa.Float(), nullable=False),
        sa.Column("indexed_frames", sa.Integer(), nullable=False),
        sa.Column("job_id", sa.Integer, nullable=True),
        sa.Column("job_status", sa.Enum(DBJobStatus), nullable=False),
        sa.Column("job_error", sa.Text, nullable=True),
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
        configuration: sa.Table,
        chemical: sa.Table,
        run: sa.Table,
        attributo: sa.Table,
        event_log: sa.Table,
        experiment_type: sa.Table,
        experiment_has_attributo: sa.Table,
        file: sa.Table,
        data_set: sa.Table,
        chemical_has_file: sa.Table,
        run_has_file: sa.Table,
        event_has_file: sa.Table,
        beamtime_schedule: sa.Table,
        indexing_result: sa.Table,
    ) -> None:
        self.configuration = configuration
        self.event_has_file = event_has_file
        self.data_set = data_set
        self.experiment_type = experiment_type
        self.experiment_has_attributo = experiment_has_attributo
        self.run_has_file = run_has_file
        self.event_log = event_log
        self.chemical = chemical
        self.run = run
        self.attributo = attributo
        self.file = file
        self.chemical_has_file = chemical_has_file
        self.beamtime_schedule = beamtime_schedule
        self.indexing_result = indexing_result


def create_tables_from_metadata(metadata: MetaData) -> DBTables:
    chemical = _table_chemical(metadata)
    run = _table_run(metadata)
    file = _table_file(metadata)
    table_attributo = _table_attributo(metadata)
    experiment_type = _table_experiment_type(metadata)
    data_set = _table_data_set(metadata, experiment_type)
    event_log = _table_event_log(metadata)
    indexing_result = _table_indexing_result(metadata, run)
    return DBTables(
        configuration=_table_configuration(metadata, experiment_type),
        chemical=chemical,
        run=run,
        attributo=table_attributo,
        event_log=event_log,
        data_set=data_set,
        experiment_type=experiment_type,
        experiment_has_attributo=_table_experiment_has_attributo(
            metadata, table_attributo, experiment_type
        ),
        file=file,
        chemical_has_file=_table_chemical_has_file(metadata, chemical, file),
        run_has_file=_table_run_has_file(metadata, run, file),
        event_has_file=_table_event_has_file(metadata, event_log, file),
        beamtime_schedule=_table_beamtime_schedule(metadata, chemical),
        indexing_result=indexing_result,
    )
