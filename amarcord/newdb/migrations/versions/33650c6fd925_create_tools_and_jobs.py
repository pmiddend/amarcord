"""create_tools_and_jobs

Revision ID: 33650c6fd925
Revises: 21956889bfbe
Create Date: 2021-07-29 13:50:02.003669

"""
import enum

from alembic import context
from alembic import op

# revision identifiers, used by Alembic.
from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import Text
from sqlalchemy import func

from amarcord.workflows.job_status import JobStatus

revision = "33650c6fd925"
down_revision = "21956889bfbe"
branch_labels = None
depends_on = None


class RefinementMethod(enum.Enum):
    HZB = "hzb"
    OTHER = "other"
    DMPL = "dmpl"
    DMPL2 = "dmpl2"
    DMPL2_ALIGNED = "dmpl2-aligned"
    DMPL2_QFIT = "dmpl2-qfit"


def upgrade():
    analysis_schema = context.get_x_argument(as_dictionary=True).get(
        "analysis-schema", None
    )
    main_schema = context.get_x_argument(as_dictionary=True).get("main-schema", None)
    collation = context.get_x_argument(as_dictionary=True).get("collation", None)
    if collation is None:
        raise Exception(
            'cannot migrate, please specify -x collation=baz (check with "SHOW CREATE TABLE Crystals" beforehand)'
        )
    if (
        analysis_schema is not None
        and main_schema is None
        or analysis_schema is None
        and main_schema is not None
    ):
        raise Exception(
            'cannot migrate, please specify both "-x analysis-schema=foo" as well as "-x main-schema=bar" (or none of '
            "both) "
        )
    main_schema_prefix = f"{main_schema}." if main_schema is not None else ""
    analysis_schema_prefix = (
        f"{analysis_schema}." if analysis_schema is not None else ""
    )

    if analysis_schema is None:
        op.drop_table("Job_To_Diffraction")
        op.drop_table("Job_To_Data_Reduction")
        op.drop_table("Jobs")
        op.drop_table("Tools")

        op.create_table(
            "Refinement",
            Column("refinement_id", Integer, primary_key=True),
            Column(
                "data_reduction_id",
                Integer,
                ForeignKey(f"{analysis_schema_prefix}Data_Reduction.data_reduction_id"),
            ),
            Column("analysis_time", DateTime, nullable=False),
            Column("folder_path", Text),
            Column("initial_pdb_path", Text, nullable=False),
            Column("final_pdb_path", Text),
            Column("refinement_mtz_path", Text),
            Column(
                "method",
                Enum(RefinementMethod, values_callable=lambda x: [e.value for e in x]),
                nullable=False,
            ),
            Column("comment", Text),
            Column("resolution_cut", Float, comment="angstrom"),
            Column("rfree", Float, comment="percent"),
            Column("rwork", Float, comment="percent"),
            Column("rms_bond_length", Float, comment="angstrom"),
            Column("rms_bond_angle", Float, comment="angstrom"),
            Column("num_blobs", SmallInteger, comment="count"),
            Column("average_model_b", Float, comment="angstrom**2"),
            schema=analysis_schema,
            mysql_collate=collation,
        )

    print("Creating v2 tools schema")
    op.create_table(
        "Tools",
        Column("id", Integer(), primary_key=True, autoincrement=True),
        Column("created", DateTime, server_default=func.now()),
        Column("name", String(length=255), nullable=False),
        Column("executable_path", Text(), nullable=False),
        Column("extra_files", JSON(), nullable=False),
        Column("command_line", Text(), nullable=False),
        Column("description", Text(), nullable=False),
        schema=analysis_schema,
        mysql_collate=collation,
    )
    op.create_table(
        "Jobs",
        Column("id", Integer(), primary_key=True),
        Column("queued", DateTime, nullable=False),
        Column("started", DateTime, nullable=True),
        Column("stopped", DateTime, nullable=True),
        Column("comment", Text, nullable=True),
        Column("status", Enum(JobStatus), nullable=False),
        Column("failure_reason", Text(), nullable=True),
        Column("output_directory", Text(), nullable=True),
        Column(
            "tool_id",
            Integer(),
            ForeignKey(
                f"{analysis_schema_prefix}Tools.id",
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            nullable=False,
        ),
        Column("tool_inputs", JSON(), nullable=False),
        Column("metadata", JSON(), nullable=True),
        schema=analysis_schema,
        mysql_collate=collation,
    )
    op.create_table(
        "Job_Has_Reduction_Result",
        Column(
            "job_id",
            Integer(),
            ForeignKey(
                f"{analysis_schema_prefix}Jobs.id",
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        Column(
            "data_reduction_id",
            Integer(),
            ForeignKey(
                f"{analysis_schema_prefix}Data_Reduction.data_reduction_id",
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        schema=analysis_schema,
        mysql_collate=collation,
    )
    op.create_table(
        "Job_Has_Refinement_Result",
        Column(
            "job_id",
            Integer(),
            ForeignKey(
                f"{analysis_schema_prefix}Jobs.id",
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        Column(
            "refinement_id",
            Integer(),
            ForeignKey(
                f"{analysis_schema_prefix}Refinement.refinement_id",
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        schema=analysis_schema,
        mysql_collate=collation,
    )
    op.create_table(
        "Job_Working_On_Reduction",
        Column(
            "job_id",
            Integer(),
            ForeignKey(
                f"{analysis_schema_prefix}Jobs.id",
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        Column(
            "data_reduction_id",
            Integer(),
            ForeignKey(
                f"{analysis_schema_prefix}Data_Reduction.data_reduction_id",
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        schema=analysis_schema,
        mysql_collate=collation,
    )
    op.create_table(
        "Job_Working_On_Diffraction",
        Column("job_id", Integer(), ForeignKey("Jobs.id"), primary_key=True),
        Column(
            "crystal_id",
            String(length=255),
            ForeignKey(f"{main_schema_prefix}Crystals.crystal_id"),
            primary_key=True,
        ),
        Column(
            "run_id",
            Integer(),
            primary_key=True,
        ),
        ForeignKeyConstraint(
            ["crystal_id", "run_id"],
            [
                f"{main_schema_prefix}Diffractions.crystal_id",
                f"{main_schema_prefix}Diffractions.run_id",
            ],
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        schema=analysis_schema,
        mysql_collate=collation,
    )


def downgrade():
    pass
