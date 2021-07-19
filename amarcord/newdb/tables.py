import enum
from typing import Optional

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import ForeignKeyConstraint
from sqlalchemy import Integer
from sqlalchemy import JSON
from sqlalchemy import MetaData
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import func
from sqlalchemy.dialects.mysql import DOUBLE
from sqlalchemy.engine import Engine

from amarcord.newdb.beamline import Beamline
from amarcord.newdb.diffraction_type import DiffractionType
from amarcord.newdb.puck_type import PuckType
from amarcord.newdb.reduction_method import ReductionMethod
from amarcord.workflows.job_status import JobStatus


def table_pucks(metadata: MetaData, schema: Optional[str] = None) -> Table:
    return Table(
        "Pucks",
        metadata,
        Column("puck_id", String(length=255), nullable=False, primary_key=True),
        Column("created", DateTime, server_default=func.now()),
        Column("puck_type", Enum(PuckType)),
        Column("owner", String(length=255)),
        schema=schema,
    )


def table_job_to_diffraction(
    metadata: MetaData,
    _jobs: Table,
    _crystals: Table,
    _diffractions: Table,
    schema: Optional[str] = None,
) -> Table:
    return Table(
        "Job_To_Diffraction",
        metadata,
        Column("job_id", Integer(), ForeignKey("Jobs.id"), primary_key=True),
        Column(
            "crystal_id",
            String(length=255),
            ForeignKey("Crystals.crystal_id"),
            primary_key=True,
        ),
        Column(
            "run_id",
            Integer(),
            primary_key=True,
        ),
        ForeignKeyConstraint(
            ["crystal_id", "run_id"], ["Diffractions.crystal_id", "Diffractions.run_id"]
        ),
        schema=schema,
    )


def table_job_to_reduction(
    metadata: MetaData,
    _jobs: Table,
    _data_reduction: Table,
    schema: Optional[str] = None,
) -> Table:
    return Table(
        "Job_To_Data_Reduction",
        metadata,
        Column("job_id", Integer(), ForeignKey("Jobs.id"), primary_key=True),
        Column(
            "data_reduction_id",
            Integer(),
            ForeignKey("Data_Reduction.data_reduction_id"),
            primary_key=True,
        ),
        schema=schema,
    )


def table_unfinished_job_to_reduction(
    metadata: MetaData,
    _jobs: Table,
    _data_reduction: Table,
    schema: Optional[str] = None,
) -> Table:
    return Table(
        "Unfinished_Job_To_Data_Reduction",
        metadata,
        Column("job_id", Integer(), ForeignKey("Jobs.id"), primary_key=True),
        Column(
            "data_reduction_id",
            Integer(),
            ForeignKey("Data_Reduction.data_reduction_id"),
            primary_key=True,
        ),
        schema=schema,
    )


def table_job_to_refinement(
    metadata: MetaData,
    _jobs: Table,
    _refinement: Table,
    schema: Optional[str] = None,
) -> Table:
    return Table(
        "Job_To_Refinement",
        metadata,
        Column("job_id", Integer(), ForeignKey("Jobs.id"), primary_key=True),
        Column(
            "refinement_id",
            Integer(),
            ForeignKey("Data_Reduction.data_reduction_id"),
            primary_key=True,
        ),
        schema=schema,
    )


def table_jobs(
    metadata: MetaData,
    _tools: Table,
    schema: Optional[str] = None,
) -> Table:
    return Table(
        "Jobs",
        metadata,
        Column("id", Integer(), primary_key=True),
        Column("queued", DateTime, nullable=False),
        Column("started", DateTime, nullable=True),
        Column("stopped", DateTime, nullable=True),
        Column("status", Enum(JobStatus), nullable=False),
        Column("failure_reason", Text(), nullable=True),
        Column("output_directory", Text(), nullable=True),
        Column(
            "tool_id",
            Integer(),
            ForeignKey("Tools.id"),
            nullable=True,
        ),
        Column("tool_inputs", JSON(), nullable=True),
        Column("metadata", JSON(), nullable=True),
        schema=schema,
    )


def table_tools(metadata: MetaData, schema: Optional[str] = None) -> Table:
    return Table(
        "Tools",
        metadata,
        Column("id", Integer(), primary_key=True, autoincrement=True),
        Column("created", DateTime, server_default=func.now()),
        Column("name", String(length=255), nullable=False, unique=True),
        Column("executable_path", Text(), nullable=False),
        Column("extra_files", JSON(), nullable=False),
        Column("command_line", Text(), nullable=False),
        Column("description", Text(), nullable=False),
        schema=schema,
    )


def table_dewar_lut(
    metadata: MetaData, _pucks: Table, schema: Optional[str] = None
) -> Table:
    return Table(
        "Dewar_LUT",
        metadata,
        Column(
            "puck_id",
            String(length=255),
            ForeignKey("Pucks.puck_id", ondelete="CASCADE", onupdate="CASCADE"),
            nullable=False,
            primary_key=True,
        ),
        Column("dewar_position", Integer, nullable=False, primary_key=True),
        schema=schema,
    )


def table_crystals(
    metadata: MetaData, _pucks: Table, schema: Optional[str] = None
) -> Table:
    return Table(
        "Crystals",
        metadata,
        Column("crystal_id", String(length=255), primary_key=True, nullable=False),
        Column("created", DateTime, server_default=func.now()),
        Column("puck_id", String(length=255), ForeignKey("Pucks.puck_id"), index=True),
        Column("puck_position_id", SmallInteger),
        schema=schema,
    )


class RefinementMethod(enum.Enum):
    HZB = "hzb"
    DMPL = "dmpl"
    DMPL2 = "dmpl2"
    DMPL2_ALIGNED = "dmpl2-aligned"
    DMPL2_QFIT = "dmpl2-qfit"


def table_data_reduction(
    metadata: MetaData, _crystals: Table, schema: Optional[str] = None
) -> Table:
    return Table(
        "Data_Reduction",
        metadata,
        Column("data_reduction_id", Integer, nullable=False, primary_key=True),
        Column(
            "crystal_id",
            String(length=255),
            ForeignKey("Crystals.crystal_id"),
            nullable=False,
        ),
        Column("run_id", Integer, nullable=False),
        Column("analysis_time", DateTime, nullable=False),
        Column("folder_path", String(length=255), nullable=False, unique=True),
        Column("mtz_path", Text),
        Column("comment", Text),
        Column(
            "method",
            Enum(ReductionMethod, values_callable=lambda x: [e.value for e in x]),
            nullable=False,
        ),
        Column("resolution_cc", Float, comment="angstrom"),
        Column("resolution_isigma", Float, comment="angstrom"),
        Column("a", Float, comment="angstrom"),
        Column("b", Float, comment="angstrom"),
        Column("c", Float, comment="angstrom"),
        Column("alpha", Float, comment="degrees"),
        Column("beta", Float, comment="degrees"),
        Column("gamma", Float, comment="degrees"),
        Column(
            "space_group",
            SmallInteger,
            comment="https://it.iucr.org/Ac/ch2o3v0001/contents/",
        ),
        Column("isigi", Float, comment="sigma"),
        Column("rmeas", Float, comment="percent"),
        Column("cchalf", Float, comment="percent"),
        Column("rfactor", Float, comment="percent"),
        Column("Wilson_b", Float, comment="angstrom**2"),
        schema=schema,
    )


def table_refinement(
    metadata: MetaData, _crystals: Table, schema: Optional[str] = None
) -> Table:
    return Table(
        "Refinement",
        metadata,
        Column("refinement_id", Integer, primary_key=True),
        Column(
            "data_reduction_id", Integer, ForeignKey("Data_Reduction.data_reduction_id")
        ),
        Column("analysis_time", DateTime, nullable=False),
        Column("folder_path", Text),
        Column("initial_pdb_path", Text),
        Column("final_pdb_path", Text),
        Column("refinement_mtz_path", Text),
        Column(
            "method",
            Enum(RefinementMethod, values_callable=lambda x: [e.value for e in x]),
            nullable=False,
        ),
        Column("comment", Text),
        Column("resolution_cut", DOUBLE, comment="angstrom"),
        Column("rfree", DOUBLE, comment="percent"),
        Column("rwork", DOUBLE, comment="percent"),
        Column("rms_bond_length", DOUBLE, comment="angstrom"),
        Column("rms_bond_angle", DOUBLE, comment="angstrom"),
        Column("num_blobs", SmallInteger, comment="count"),
        Column("average_model_b", DOUBLE, comment="angstrom**2"),
        schema=schema,
    )


def table_diffractions(
    metadata: MetaData, _crystals: Table, schema: Optional[str] = None
) -> Table:
    return Table(
        "Diffractions",
        metadata,
        Column(
            "crystal_id",
            String(length=255),
            ForeignKey("Crystals.crystal_id", ondelete="CASCADE", onupdate="CASCADE"),
            nullable=False,
            primary_key=True,
        ),
        Column("run_id", Integer, nullable=False, server_default="1", primary_key=True),
        Column("created", DateTime, server_default=func.now()),
        Column("dewar_position", Integer),
        Column("beamline", Enum(Beamline)),
        Column("beam_intensity", String(length=255)),
        Column("pinhole", String(length=255)),
        Column("focusing", String(length=255)),
        Column(
            "diffraction",
            Enum(DiffractionType, values_callable=lambda x: [e.value for e in x]),
            nullable=False,
        ),
        Column("comment", Text),
        Column("estimated_resolution", Text),
        Column("metadata", String(length=255)),
        Column("angle_start", Float, comment="deg"),
        Column("number_of_frames", Integer),
        Column("angle_step", Float, comment="deg"),
        Column("exposure_time", Float, comment="ms"),
        Column("xray_energy", Float, comment="keV"),
        Column("xray_wavelength", Float, comment="angstrom"),
        Column("detector_name", String(length=255)),
        Column("detector_distance", Float, comment="mm"),
        Column("detector_edge_resolution", Float, comment="angstrom"),
        Column("aperture_radius", Float, comment="um"),
        Column("filter_transmission", Float, comment="%"),
        Column("ring_current", Float, comment="mA"),
        Column("data_raw_filename_pattern", Text, comment="regexp"),
        Column("microscope_image_filename_pattern", Text, comment="regexp"),
        Column("aperture_horizontal", Float, comment="um"),
        Column("aperture_vertical", Float, comment="um"),
        schema=schema,
    )


class DBTables:
    def __init__(
        self,
        metadata: MetaData,
        with_tools: bool,
        with_estimated_resolution: bool,
        normal_schema: Optional[str],
        analysis_schema: Optional[str],
        engine: Optional[Engine] = None,
    ) -> None:
        self.with_estimated_resolution = with_estimated_resolution
        self.pucks = table_pucks(metadata, normal_schema)
        self.dewar_lut = table_dewar_lut(metadata, self.pucks, normal_schema)
        self.crystals = (
            table_crystals(metadata, self.pucks, normal_schema)
            if engine is None
            else Table("Crystals", metadata, autoload_with=engine)
        )
        self.diffs = table_diffractions(metadata, self.crystals, normal_schema)
        self.reductions = table_data_reduction(metadata, self.crystals, analysis_schema)
        self.tools: Optional[Table]
        self.jobs: Optional[Table]
        self.reduction_jobs: Optional[Table]
        self.job_reductions: Optional[Table]
        if with_tools:
            self.tools = table_tools(metadata, analysis_schema)
            self.jobs = table_jobs(metadata, self.tools, analysis_schema)
            self.reduction_jobs = table_job_to_diffraction(
                metadata, self.jobs, self.crystals, self.diffs, analysis_schema
            )
            self.job_reductions = table_job_to_reduction(
                metadata, self.reduction_jobs, self.reductions, analysis_schema
            )
        else:
            self.tools = None
            self.jobs = None
            self.reduction_jobs = None
            self.job_reductions = None
