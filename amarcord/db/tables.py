import logging

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import MetaData
from sqlalchemy.dialects.mysql import LONGBLOB
from sqlalchemy.sql import ColumnElement

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.merge_negative_handling import MergeNegativeHandling

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


def _table_beamtime_schedule_has_chemical(
    metadata: sa.MetaData,
    beamtime_schedule: sa.Table,
    chemical: sa.Table,
) -> sa.Table:
    return sa.Table(
        "BeamtimeScheduleHasChemical",
        metadata,
        sa.Column(
            "beamtime_schedule_id",
            sa.Integer(),
            # If the beam time shift vanishes, delete this entry as well
            ForeignKey(_fk_identifier(beamtime_schedule.c.id), ondelete="cascade"),
        ),
        sa.Column(
            "chemical_id",
            sa.Integer(),
            # If the chemical vanishes, delete this entry as well
            ForeignKey(_fk_identifier(chemical.c.id), ondelete="cascade"),
        ),
        sa.PrimaryKeyConstraint(
            "beamtime_schedule_id", "chemical_id", name="BeamtimeScheduleHasChemical_pk"
        ),
    )


def _table_beamtime_schedule(metadata: sa.MetaData) -> sa.Table:
    return sa.Table(
        "BeamtimeSchedule",
        metadata,
        sa.Column("id", sa.Integer(), primary_key=True),
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
        sa.Column("chemical_role", sa.Enum(ChemicalType), nullable=False)
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
        sa.Column("type", sa.Enum(ChemicalType), nullable=False)
    )


def _table_refinement_result(
    metadata: sa.MetaData, merge_result: sa.Table, file: sa.Table
) -> sa.Table:
    return sa.Table(
        "RefinementResult",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "merge_result_id",
            sa.Integer,
            sa.ForeignKey(_fk_identifier(merge_result.c.id), ondelete="cascade"),
        ),
        sa.Column(
            "pdb_file_id",
            sa.Integer,
            sa.ForeignKey(_fk_identifier(file.c.id), ondelete="cascade"),
        ),
        sa.Column(
            "mtz_file_id",
            sa.Integer,
            sa.ForeignKey(_fk_identifier(file.c.id), ondelete="cascade"),
        ),
        sa.Column("r_free", sa.Float, nullable=False),
        sa.Column("r_work", sa.Float, nullable=False),
        sa.Column("rms_bond_angle", sa.Float, nullable=False),
        sa.Column("rms_bond_length", sa.Float, nullable=False),
    )


def _table_merge_result_shell_fom(
    metadata: sa.MetaData, merge_result: sa.Table
) -> sa.Table:
    return sa.Table(
        "MergeResultShellFom",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "merge_result_id",
            sa.Integer,
            ForeignKey(merge_result.c.id, ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("one_over_d_centre", sa.Float, nullable=False),
        sa.Column("nref", sa.Integer, nullable=False),
        sa.Column("d_over_a", sa.Float, nullable=False),
        sa.Column("min_res", sa.Float, nullable=False),
        sa.Column("max_res", sa.Float, nullable=False),
        sa.Column("cc", sa.Float, nullable=False),
        sa.Column("ccstar", sa.Float, nullable=False),
        sa.Column("r_split", sa.Float, nullable=False),
        sa.Column("reflections_possible", sa.Integer, nullable=False),
        sa.Column("completeness", sa.Float, nullable=False),
        sa.Column("measurements", sa.Integer, nullable=False),
        sa.Column("redundancy", sa.Float, nullable=False),
        sa.Column("snr", sa.Float, nullable=False),
        sa.Column("mean_i", sa.Float, nullable=False),
    )


def _table_merge_result_has_indexing_result(
    metadata: sa.MetaData, merge_result: sa.Table, indexing_result: sa.Table
) -> sa.Table:
    return sa.Table(
        "MergeResultHasIndexingResult",
        metadata,
        sa.Column(
            "merge_result_id",
            sa.Integer,
            ForeignKey(_fk_identifier(merge_result.c.id), ondelete="cascade"),
        ),
        sa.Column(
            "indexing_result_id",
            sa.Integer,
            ForeignKey(_fk_identifier(indexing_result.c.id), ondelete="cascade"),
        ),
    )


def _table_merge_result(metadata: sa.MetaData, file: sa.Table) -> sa.Table:
    return sa.Table(
        "MergeResult",
        metadata,
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created", sa.DateTime, nullable=False),
        sa.Column("partialator_additional", sa.Text, nullable=False),
        sa.Column("recent_log", sa.Text, nullable=False),
        sa.Column("negative_handling", sa.Enum(MergeNegativeHandling), nullable=True),
        sa.Column("job_status", sa.Enum(DBJobStatus), nullable=False),
        sa.Column("started", sa.DateTime, nullable=True),
        sa.Column("stopped", sa.DateTime, nullable=True),
        sa.Column("point_group", sa.String(length=32), nullable=True),
        sa.Column("cell_description", sa.String(length=255), nullable=True),
        sa.Column("job_id", sa.Integer, nullable=True),
        sa.Column("job_error", sa.Text, nullable=True),
        sa.Column(
            "mtz_file_id",
            sa.Integer,
            ForeignKey(_fk_identifier(file.c.id), ondelete="cascade"),
            nullable=True,
        ),
        sa.Column("fom_snr", sa.Float, nullable=True),
        sa.Column("fom_wilson", sa.Float, nullable=True),
        sa.Column("fom_ln_k", sa.Float, nullable=True),
        sa.Column("fom_discarded_reflections", sa.Integer, nullable=True),
        sa.Column("fom_one_over_d_from", sa.Float, nullable=True),
        sa.Column("fom_one_over_d_to", sa.Float, nullable=True),
        sa.Column("fom_redundancy", sa.Float, nullable=True),
        sa.Column("fom_completeness", sa.Float, nullable=True),
        sa.Column("fom_measurements_total", sa.Integer, nullable=True),
        sa.Column("fom_reflections_total", sa.Integer, nullable=True),
        sa.Column("fom_reflections_possible", sa.Integer, nullable=True),
        sa.Column("fom_r_split", sa.Float, nullable=True),
        sa.Column("fom_r1i", sa.Float, nullable=True),
        sa.Column("fom_2", sa.Float, nullable=True),
        sa.Column("fom_cc", sa.Float, nullable=True),
        sa.Column("fom_ccstar", sa.Float, nullable=True),
        sa.Column("fom_ccano", sa.Float, nullable=True),
        sa.Column("fom_crdano", sa.Float, nullable=True),
        sa.Column("fom_rano", sa.Float, nullable=True),
        sa.Column("fom_rano_over_r_split", sa.Float, nullable=True),
        sa.Column("fom_d1sig", sa.Float, nullable=True),
        sa.Column("fom_d2sig", sa.Float, nullable=True),
        sa.Column("fom_outer_resolution", sa.Float, nullable=True),
        sa.Column("fom_outer_ccstar", sa.Float, nullable=True),
        sa.Column("fom_outer_r_split", sa.Float, nullable=True),
        sa.Column("fom_outer_cc", sa.Float, nullable=True),
        sa.Column("fom_outer_unique_reflections", sa.Integer, nullable=True),
        sa.Column("fom_outer_completeness", sa.Float, nullable=True),
        sa.Column("fom_outer_redundancy", sa.Float, nullable=True),
        sa.Column("fom_outer_snr", sa.Float, nullable=True),
        sa.Column("fom_outer_min_res", sa.Float, nullable=True),
        sa.Column("fom_outer_max_res", sa.Float, nullable=True),
    )


def _table_indexing_result(
    metadata: sa.MetaData, run: sa.Table, chemical: sa.Table
) -> sa.Table:
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
        sa.Column("cell_description", sa.String(length=255), nullable=False),
        sa.Column("point_group", sa.String(length=32), nullable=False),
        sa.Column(
            "chemical_id",
            sa.Integer,
            ForeignKey(
                _fk_identifier(chemical.c.id),
                ondelete="cascade",
                name="indexing_result_has_chemical_fk",
            ),
            nullable=True,
        ),
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
        beamtime_schedule_has_chemical: sa.Table,
        indexing_result: sa.Table,
        merge_result: sa.Table,
        merge_result_has_indexing_result: sa.Table,
        merge_result_shell_fom: sa.Table,
        refinement_result: sa.Table,
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
        self.beamtime_schedule_has_chemical = beamtime_schedule_has_chemical
        self.indexing_result = indexing_result
        self.merge_result = merge_result
        self.merge_result_shell_fom = merge_result_shell_fom
        self.merge_result_has_indexing_result = merge_result_has_indexing_result
        self.refinement_result = refinement_result


def create_tables_from_metadata(metadata: MetaData) -> DBTables:
    chemical = _table_chemical(metadata)
    beamtime_schedule = _table_beamtime_schedule(metadata)
    run = _table_run(metadata)
    file = _table_file(metadata)
    table_attributo = _table_attributo(metadata)
    experiment_type = _table_experiment_type(metadata)
    data_set = _table_data_set(metadata, experiment_type)
    event_log = _table_event_log(metadata)
    indexing_result = _table_indexing_result(metadata, run, chemical)
    merge_result = _table_merge_result(metadata, file)
    merge_result_shell_fom = _table_merge_result_shell_fom(metadata, merge_result)
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
        beamtime_schedule=beamtime_schedule,
        beamtime_schedule_has_chemical=_table_beamtime_schedule_has_chemical(
            metadata, beamtime_schedule, chemical
        ),
        indexing_result=indexing_result,
        merge_result=merge_result,
        merge_result_shell_fom=merge_result_shell_fom,
        merge_result_has_indexing_result=_table_merge_result_has_indexing_result(
            metadata, merge_result, indexing_result
        ),
        refinement_result=_table_refinement_result(metadata, merge_result, file),
    )
