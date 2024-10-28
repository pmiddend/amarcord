# pylint: disable=trailing-whitespace
"""initial tables

Revision ID: 341b2d1ddb48
Revises: 
Create Date: 2022-03-23 16:33:43.172942

"""
from enum import Enum
from typing import Any

import sqlalchemy as sa
from alembic import op
from sqlalchemy import ForeignKey
from sqlalchemy.dialects.mysql import LONGBLOB
from sqlalchemy.sql import ColumnElement

# revision identifiers, used by Alembic.
revision = "341b2d1ddb48"
down_revision = None
branch_labels = None
depends_on = None


class EventLogLevel(Enum):
    INFO = "info"
    WARNING = "warning"
    ERROR = "error"


class AssociatedTable(Enum):
    RUN = "run"
    CHEMICAL = "chemical"


class ChemicalType(Enum):
    CRYSTAL = "crystal"
    SOLUTION = "solution"


class DBJobStatus(Enum):
    QUEUED = "queued"
    RUNNING = "running"
    DONE = "done"


class MergeNegativeHandling(Enum):
    IGNORE = "ignore"
    ZERO = "zero"


class MergeModel(Enum):
    UNITY = "unity"
    XSPHERE = "xsphere"
    OFFSET = "offset"
    GGPM = "ggpm"


class ScaleIntensities(Enum):
    OFF = "off"
    NORMAL = "normal"
    DEBYE_WALLER = "debyewaller"


def _fk_identifier(c: ColumnElement[Any]) -> str:
    if c.table.schema is not None:
        return f"{c.table.schema}.{c.table.name}.{c.name}"
    return f"{c.table.name}.{c.name}"


def upgrade() -> None:
    beamtime = op.create_table(
        "Beamtime",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("external_id", sa.String(length=255), nullable=False),
        sa.Column("proposal", sa.String(length=255), nullable=False),
        sa.Column("beamline", sa.String(length=255), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=False),
        sa.Column("comment", sa.Text(), nullable=False),
        sa.Column("start", sa.DateTime, nullable=False),
        sa.Column("end", sa.DateTime, nullable=False),
    )

    # apparently create_table can return None, so we have to add these annoying assertions
    assert beamtime is not None

    experiment_type = op.create_table(
        "ExperimentType",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "beamtime_id",
            sa.Integer,
            ForeignKey(_fk_identifier(beamtime.c.id), ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
    )

    assert experiment_type is not None

    op.create_table(
        "UserConfiguration",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "beamtime_id",
            sa.Integer,
            # Beamtime vanishes => this must vanish as well
            ForeignKey(_fk_identifier(beamtime.c.id), ondelete="cascade"),
            nullable=False,
        ),
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

    attributo = op.create_table(
        "Attributo",
        sa.Column(
            "beamtime_id",
            sa.Integer,
            # Beamtime vanishes => this must vanish as well
            ForeignKey(_fk_identifier(beamtime.c.id), ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("description", sa.String(length=255), nullable=False),
        sa.Column("group", sa.String(length=255), nullable=False),
        sa.Column(
            "associated_table",
            sa.Enum(AssociatedTable),
            nullable=False,
        ),
        sa.Column("json_schema", sa.JSON, nullable=False),
    )

    assert attributo is not None

    file = op.create_table(
        "File",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column("type", sa.String(length=255), nullable=False),
        sa.Column("file_name", sa.String(length=255), nullable=False),
        sa.Column("size_in_bytes", sa.Integer(), nullable=False),
        sa.Column("original_path", sa.Text(), nullable=True),
        sa.Column("sha256", sa.String(length=64), nullable=False),
        sa.Column("modified", sa.DateTime(), nullable=False),
        sa.Column(
            "contents", sa.LargeBinary().with_variant(LONGBLOB, "mysql"), nullable=False
        ),
        sa.Column("description", sa.String(length=255)),
    )

    assert file is not None

    beamtime_schedule = op.create_table(
        "BeamtimeSchedule",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "beamtime_id",
            sa.Integer,
            # Beamtime vanishes => this must vanish as well
            ForeignKey(_fk_identifier(beamtime.c.id), ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("users", sa.String(length=255), nullable=False),
        sa.Column("td_support", sa.String(length=255), nullable=False),
        sa.Column("comment", sa.Text(), nullable=False),
        sa.Column("shift", sa.String(length=255), nullable=False),
        sa.Column("date", sa.String(length=10), nullable=False),
    )

    assert beamtime_schedule is not None

    chemical = op.create_table(
        "Chemical",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "beamtime_id",
            sa.Integer,
            # Beamtime vanishes => this must vanish as well
            ForeignKey(_fk_identifier(beamtime.c.id), ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("name", sa.String(length=255), nullable=False),
        sa.Column("responsible_person", sa.String(length=255), nullable=False),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column("type", sa.Enum(ChemicalType), nullable=False),
    )

    assert chemical is not None

    chemical_has_attributo_value = op.create_table(
        "ChemicalHasAttributoValue",
        sa.Column(
            "chemical_id",
            sa.Integer,
            ForeignKey(_fk_identifier(chemical.c.id), ondelete="cascade"),
            primary_key=True,
        ),
        sa.Column(
            "attributo_id",
            sa.Integer,
            ForeignKey(_fk_identifier(attributo.c.id), ondelete="cascade"),
            primary_key=True,
        ),
        sa.Column("integer_value", sa.Integer, nullable=True),
        sa.Column[float]("float_value", sa.Float, nullable=True),
        sa.Column("string_value", sa.Text, nullable=True),
        sa.Column("bool_value", sa.Boolean, nullable=True),
        sa.Column("datetime_value", sa.DateTime, nullable=True),
        sa.Column("list_value", sa.JSON, nullable=True),
    )

    assert chemical_has_attributo_value is not None

    op.create_table(
        "BeamtimeScheduleHasChemical",
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

    op.create_table(
        "ExperimentHasAttributo",
        sa.Column(
            "experiment_type_id",
            sa.Integer,
            ForeignKey(_fk_identifier(experiment_type.c.id), ondelete="cascade"),
            nullable=False,
        ),
        sa.Column(
            "attributo_id",
            sa.Integer,
            # If the attributo vanishes, delete the experiment type with it
            ForeignKey(
                _fk_identifier(attributo.c.id), ondelete="cascade", onupdate="cascade"
            ),
            nullable=False,
        ),
        sa.Column("chemical_role", sa.Enum(ChemicalType), nullable=False),
    )

    data_set = op.create_table(
        "DataSet",
        sa.Column("id", sa.Integer(), primary_key=True),
        sa.Column(
            "experiment_type_id",
            sa.Integer,
            ForeignKey(_fk_identifier(experiment_type.c.id), ondelete="cascade"),
            nullable=False,
        ),
    )

    assert data_set is not None

    data_set_has_attributo_value = op.create_table(
        "DataSetHasAttributoValue",
        sa.Column(
            "data_set_id",
            sa.Integer,
            ForeignKey(_fk_identifier(data_set.c.id), ondelete="cascade"),
            primary_key=True,
        ),
        sa.Column(
            "attributo_id",
            sa.Integer,
            ForeignKey(_fk_identifier(attributo.c.id), ondelete="cascade"),
            primary_key=True,
        ),
        sa.Column("integer_value", sa.Integer, nullable=True),
        sa.Column[float]("float_value", sa.Float, nullable=True),
        sa.Column("string_value", sa.Text, nullable=True),
        sa.Column("bool_value", sa.Boolean, nullable=True),
        sa.Column("datetime_value", sa.DateTime, nullable=True),
        sa.Column("list_value", sa.JSON, nullable=True),
        sa.Column(
            "chemical_value",
            sa.Integer,
            ForeignKey(_fk_identifier(chemical.c.id), ondelete="cascade"),
            nullable=True,
        ),
    )

    assert data_set_has_attributo_value is not None

    op.create_table(
        "ChemicalHasFile",
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

    event_log = op.create_table(
        "EventLog",
        sa.Column(
            "id",
            sa.Integer,
            primary_key=True,
        ),
        sa.Column(
            "beamtime_id",
            sa.Integer,
            ForeignKey(_fk_identifier(beamtime.c.id), ondelete="cascade"),
            nullable=False,
        ),
        # pylint: disable=not-callable
        sa.Column("created", sa.DateTime, nullable=False, server_default=sa.func.now()),
        sa.Column("level", sa.Enum(EventLogLevel), nullable=False),
        sa.Column("source", sa.String(length=255), nullable=False),
        sa.Column("text", sa.Text, nullable=False),
    )

    assert event_log is not None

    run = op.create_table(
        "Run",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("external_id", sa.Integer, nullable=False),
        sa.Column(
            "beamtime_id",
            sa.Integer,
            ForeignKey(_fk_identifier(beamtime.c.id), ondelete="cascade"),
            nullable=False,
        ),
        sa.Column("modified", sa.DateTime, nullable=False),
        sa.Column("started", sa.DateTime, nullable=False),
        sa.Column("stopped", sa.DateTime, nullable=True),
        sa.Column(
            "experiment_type_id",
            sa.Integer,
            ForeignKey(
                _fk_identifier(experiment_type.c.id),
                name="run_has_experiment_type_fk",
            ),
            nullable=False,
        ),
    )

    assert run is not None

    run_has_attributo_value = op.create_table(
        "RunHasAttributoValue",
        sa.Column(
            "run_id",
            sa.Integer,
            ForeignKey(_fk_identifier(run.c.id), ondelete="cascade"),
            primary_key=True,
        ),
        sa.Column(
            "attributo_id",
            sa.Integer,
            ForeignKey(_fk_identifier(attributo.c.id), ondelete="cascade"),
            primary_key=True,
        ),
        sa.Column("integer_value", sa.Integer, nullable=True),
        sa.Column[float]("float_value", sa.Float, nullable=True),
        sa.Column("string_value", sa.Text, nullable=True),
        sa.Column("bool_value", sa.Boolean, nullable=True),
        sa.Column("datetime_value", sa.DateTime, nullable=True),
        sa.Column("list_value", sa.JSON, nullable=True),
        sa.Column(
            "chemical_value",
            sa.Integer,
            ForeignKey(_fk_identifier(chemical.c.id), ondelete="cascade"),
            nullable=True,
        ),
    )

    assert run_has_attributo_value is not None

    indexing_result = op.create_table(
        "IndexingResult",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created", sa.DateTime, nullable=False),
        sa.Column(
            "run_id",
            sa.Integer,
            ForeignKey(_fk_identifier(run.c.id), ondelete="cascade"),
        ),
        sa.Column("stream_file", sa.Text(), nullable=True),
        sa.Column("cell_description", sa.String(length=255), nullable=True),
        sa.Column("point_group", sa.String(length=32), nullable=True),
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
        sa.Column("detector_shift_x_mm", sa.Float(), nullable=True),
        sa.Column("detector_shift_y_mm", sa.Float(), nullable=True),
        sa.Column("job_id", sa.Integer, nullable=True),
        sa.Column("job_status", sa.Enum(DBJobStatus), nullable=False),
        sa.Column("job_error", sa.Text, nullable=True),
    )

    assert indexing_result is not None

    op.create_table(
        "IndexingResultHasStatistic",
        sa.Column(
            "indexing_result_id",
            sa.Integer(),
            # If the run vanishes, delete this entry as well
            ForeignKey(_fk_identifier(indexing_result.c.id), ondelete="cascade"),
        ),
        sa.Column("time", sa.DateTime(), nullable=False),
        sa.Column("frames", sa.Integer(), nullable=False),
        sa.Column("hits", sa.Integer(), nullable=False),
        sa.Column("indexed_frames", sa.Integer(), nullable=False),
        sa.Column("indexed_crystals", sa.Integer(), nullable=False),
    )

    op.create_table(
        "EventHasFile",
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

    merge_result = op.create_table(
        "MergeResult",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("created", sa.DateTime, nullable=False),
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
        sa.Column("input_merge_model", sa.Enum(MergeModel), nullable=False),
        sa.Column("input_scale_intensities", sa.Enum(ScaleIntensities), nullable=False),
        sa.Column("input_post_refinement", sa.Boolean, nullable=False),
        sa.Column("input_iterations", sa.Integer, nullable=False),
        sa.Column("input_polarisation_angle", sa.Integer, nullable=True),
        sa.Column("input_polarisation_percent", sa.Integer, nullable=True),
        sa.Column("input_start_after", sa.Integer, nullable=True),
        sa.Column("input_stop_after", sa.Integer, nullable=True),
        sa.Column[float]("input_rel_b", sa.Float, nullable=False),
        sa.Column("input_no_pr", sa.Boolean, nullable=False),
        sa.Column[float]("input_force_bandwidth", sa.Float, nullable=True),
        sa.Column[float]("input_force_radius", sa.Float, nullable=True),
        sa.Column[float]("input_force_lambda", sa.Float, nullable=True),
        sa.Column("input_no_delta_cc_half", sa.Boolean, nullable=False),
        sa.Column[float]("input_max_adu", sa.Float, nullable=True),
        sa.Column("input_min_measurements", sa.Integer, nullable=False),
        sa.Column("input_logs", sa.Boolean, nullable=False),
        sa.Column[float]("input_min_res", sa.Float, nullable=True),
        sa.Column[float]("input_push_res", sa.Float, nullable=True),
        sa.Column("input_w", sa.String(length=255), nullable=True),
        sa.Column[float]("fom_snr", sa.Float, nullable=True),
        sa.Column[float]("fom_wilson", sa.Float, nullable=True),
        sa.Column[float]("fom_ln_k", sa.Float, nullable=True),
        sa.Column("fom_discarded_reflections", sa.Integer, nullable=True),
        sa.Column[float]("fom_one_over_d_from", sa.Float, nullable=True),
        sa.Column[float]("fom_one_over_d_to", sa.Float, nullable=True),
        sa.Column[float]("fom_redundancy", sa.Float, nullable=True),
        sa.Column[float]("fom_completeness", sa.Float, nullable=True),
        sa.Column("fom_measurements_total", sa.Integer, nullable=True),
        sa.Column("fom_reflections_total", sa.Integer, nullable=True),
        sa.Column("fom_reflections_possible", sa.Integer, nullable=True),
        sa.Column[float]("fom_r_split", sa.Float, nullable=True),
        sa.Column[float]("fom_r1i", sa.Float, nullable=True),
        sa.Column[float]("fom_2", sa.Float, nullable=True),
        sa.Column[float]("fom_cc", sa.Float, nullable=True),
        sa.Column[float]("fom_ccstar", sa.Float, nullable=True),
        sa.Column[float]("fom_ccano", sa.Float, nullable=True),
        sa.Column[float]("fom_crdano", sa.Float, nullable=True),
        sa.Column[float]("fom_rano", sa.Float, nullable=True),
        sa.Column[float]("fom_rano_over_r_split", sa.Float, nullable=True),
        sa.Column[float]("fom_d1sig", sa.Float, nullable=True),
        sa.Column[float]("fom_d2sig", sa.Float, nullable=True),
        sa.Column[float]("fom_outer_resolution", sa.Float, nullable=True),
        sa.Column[float]("fom_outer_ccstar", sa.Float, nullable=True),
        sa.Column[float]("fom_outer_r_split", sa.Float, nullable=True),
        sa.Column[float]("fom_outer_cc", sa.Float, nullable=True),
        sa.Column("fom_outer_unique_reflections", sa.Integer, nullable=True),
        sa.Column[float]("fom_outer_completeness", sa.Float, nullable=True),
        sa.Column[float]("fom_outer_redundancy", sa.Float, nullable=True),
        sa.Column[float]("fom_outer_snr", sa.Float, nullable=True),
        sa.Column[float]("fom_outer_min_res", sa.Float, nullable=True),
        sa.Column[float]("fom_outer_max_res", sa.Float, nullable=True),
    )

    assert merge_result is not None

    op.create_table(
        "MergeResultShellFom",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column(
            "merge_result_id",
            sa.Integer,
            ForeignKey(merge_result.c.id, ondelete="cascade"),
            nullable=False,
        ),
        sa.Column[float]("one_over_d_centre", sa.Float, nullable=False),
        sa.Column("nref", sa.Integer, nullable=False),
        sa.Column[float]("d_over_a", sa.Float, nullable=False),
        sa.Column[float]("min_res", sa.Float, nullable=False),
        sa.Column[float]("max_res", sa.Float, nullable=False),
        sa.Column[float]("cc", sa.Float, nullable=False),
        sa.Column[float]("ccstar", sa.Float, nullable=False),
        sa.Column[float]("r_split", sa.Float, nullable=False),
        sa.Column("reflections_possible", sa.Integer, nullable=False),
        sa.Column[float]("completeness", sa.Float, nullable=False),
        sa.Column("measurements", sa.Integer, nullable=False),
        sa.Column[float]("redundancy", sa.Float, nullable=False),
        sa.Column[float]("snr", sa.Float, nullable=False),
        sa.Column[float]("mean_i", sa.Float, nullable=False),
    )

    op.create_table(
        "MergeResultHasIndexingResult",
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

    op.create_table(
        "RefinementResult",
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
        sa.Column[float]("r_free", sa.Float, nullable=False),
        sa.Column[float]("r_work", sa.Float, nullable=False),
        sa.Column[float]("rms_bond_angle", sa.Float, nullable=False),
        sa.Column[float]("rms_bond_length", sa.Float, nullable=False),
    )


def downgrade() -> None:
    pass
