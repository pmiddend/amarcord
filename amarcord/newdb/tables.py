from dataclasses import dataclass
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
from sqlalchemy.engine import Engine
from sqlalchemy.sql import ColumnElement

from amarcord.newdb.beamline import Beamline
from amarcord.newdb.diffraction_type import DiffractionType
from amarcord.newdb.path import Path
from amarcord.newdb.path import VarcharPath
from amarcord.newdb.puck_type import PuckType
from amarcord.newdb.reduction_method import ReductionMethod
from amarcord.newdb.refinement_method import RefinementMethod
from amarcord.workflows.job_status import JobStatus


@dataclass(frozen=True)
class SeparateSchemata:
    main_schema: str
    analysis_schema: str

    @staticmethod
    def from_two_optionals(
        main_schema: Optional[str], analysis_schema: Optional[str]
    ) -> "Optional[SeparateSchemata]":
        if (main_schema is None) != (analysis_schema is None):
            if main_schema is None:
                raise Exception(
                    "received an analysis schema, but not a main schema; please specify both!"
                )
            raise Exception(
                "received a main schema, but not an analysis schema; please specify both!"
            )
        if main_schema is None or analysis_schema is None:
            return None
        return SeparateSchemata(main_schema, analysis_schema)


Schemata = Optional[SeparateSchemata]


def _analysis_schema(s: Schemata) -> Optional[str]:
    return s.analysis_schema if s is not None else None


def _main_schema(s: Schemata) -> Optional[str]:
    return s.main_schema if s is not None else None


def _fk_identifier(c: ColumnElement) -> str:
    if c.table.schema is not None:
        return f"{c.table.schema}.{c.table.name}.{c.name}"
    return f"{c.table.name}.{c.name}"


def table_pucks(metadata: MetaData, schemata: Schemata) -> Table:
    return Table(
        "Pucks",
        metadata,
        Column("puck_id", String(length=255), nullable=False, primary_key=True),
        Column("created", DateTime, server_default=func.now()),
        Column("puck_type", Enum(PuckType)),
        Column("owner", String(length=255)),
        schema=_main_schema(schemata),
    )


def table_job_working_on_diffraction(
    metadata: MetaData,
    jobs: Table,
    crystals: Table,
    diffractions: Table,
    schemata: Schemata,
) -> Table:
    return Table(
        "Job_Working_On_Diffraction",
        metadata,
        Column(
            "job_id", Integer(), ForeignKey(_fk_identifier(jobs.c.id)), primary_key=True
        ),
        Column(
            "crystal_id",
            String(length=255),
            ForeignKey(_fk_identifier(crystals.c.crystal_id)),
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
                _fk_identifier(diffractions.c.crystal_id),
                _fk_identifier(diffractions.c.run_id),
            ],
            onupdate="CASCADE",
            ondelete="CASCADE",
        ),
        schema=_analysis_schema(schemata),
    )


def table_job_has_reduction_result(
    metadata: MetaData,
    jobs: Table,
    data_reduction: Table,
    schemata: Schemata,
) -> Table:
    return Table(
        "Job_Has_Reduction_Result",
        metadata,
        Column(
            "job_id",
            Integer(),
            ForeignKey(
                _fk_identifier(jobs.c.id), onupdate="CASCADE", ondelete="CASCADE"
            ),
            primary_key=True,
        ),
        Column(
            "data_reduction_id",
            Integer(),
            ForeignKey(
                _fk_identifier(data_reduction.c.data_reduction_id),
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        schema=_analysis_schema(schemata),
    )


def table_job_working_on_reduction(
    metadata: MetaData,
    jobs: Table,
    data_reduction: Table,
    schemata: Schemata,
) -> Table:
    return Table(
        "Job_Working_On_Reduction",
        metadata,
        Column(
            "job_id",
            Integer(),
            ForeignKey(
                _fk_identifier(jobs.c.id), onupdate="CASCADE", ondelete="CASCADE"
            ),
            primary_key=True,
        ),
        Column(
            "data_reduction_id",
            Integer(),
            ForeignKey(
                _fk_identifier(data_reduction.c.data_reduction_id),
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        schema=_analysis_schema(schemata),
    )


def table_job_has_refinement_result(
    metadata: MetaData,
    jobs: Table,
    refinement: Table,
    schemata: Schemata,
) -> Table:
    return Table(
        "Job_Has_Refinement_Result",
        metadata,
        Column(
            "job_id",
            Integer(),
            ForeignKey(
                _fk_identifier(jobs.c.id), onupdate="CASCADE", ondelete="CASCADE"
            ),
            primary_key=True,
        ),
        Column(
            "refinement_id",
            Integer(),
            ForeignKey(
                _fk_identifier(refinement.c.refinement_id),
                onupdate="CASCADE",
                ondelete="CASCADE",
            ),
            primary_key=True,
        ),
        schema=_analysis_schema(schemata),
    )


def table_jobs(
    metadata: MetaData,
    tools: Table,
    schemata: Schemata,
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
        Column("comment", Text(), nullable=True),
        Column("output_directory", Path(), nullable=True),
        Column("last_stdout", Text(), nullable=True),
        Column("last_stderr", Text(), nullable=True),
        Column(
            "tool_id",
            Integer(),
            ForeignKey(
                _fk_identifier(tools.c.id), onupdate="CASCADE", ondelete="CASCADE"
            ),
            nullable=False,
        ),
        Column("tool_inputs", JSON(), nullable=False),
        Column("metadata", JSON(), nullable=True),
        schema=_analysis_schema(schemata),
    )


def table_tools(metadata: MetaData, schemata: Schemata) -> Table:
    return Table(
        "Tools",
        metadata,
        Column("id", Integer(), primary_key=True, autoincrement=True),
        Column("created", DateTime, server_default=func.now()),
        Column("name", String(length=255), nullable=False, unique=True),
        Column("executable_path", Path(), nullable=False),
        Column("extra_files", JSON(), nullable=False),
        Column("command_line", Text(), nullable=False),
        Column("description", Text(), nullable=False),
        schema=_analysis_schema(schemata),
    )


def table_dewar_lut(metadata: MetaData, pucks: Table, schemata: Schemata) -> Table:
    return Table(
        "Dewar_LUT",
        metadata,
        Column(
            "puck_id",
            String(length=255),
            ForeignKey(
                _fk_identifier(pucks.c.puck_id), ondelete="CASCADE", onupdate="CASCADE"
            ),
            nullable=False,
            primary_key=True,
        ),
        Column("dewar_position", Integer, nullable=False, primary_key=True),
        schema=_main_schema(schemata),
    )


def table_crystals(metadata: MetaData, pucks: Table, schemata: Schemata) -> Table:
    return Table(
        "Crystals",
        metadata,
        Column("crystal_id", String(length=255), primary_key=True, nullable=False),
        Column("created", DateTime, server_default=func.now()),
        Column(
            "puck_id",
            String(length=255),
            ForeignKey(_fk_identifier(pucks.c.puck_id)),
            index=True,
        ),
        Column("puck_position_id", SmallInteger),
        schema=_main_schema(schemata),
    )


def table_data_reduction(
    metadata: MetaData, crystals: Table, schemata: Schemata
) -> Table:
    return Table(
        "Data_Reduction",
        metadata,
        Column("data_reduction_id", Integer, nullable=False, primary_key=True),
        Column(
            "crystal_id",
            String(length=255),
            ForeignKey(_fk_identifier(crystals.c.crystal_id)),
            nullable=False,
        ),
        Column("run_id", Integer, nullable=False),
        Column("analysis_time", DateTime, nullable=False),
        Column("folder_path", VarcharPath(length=255), nullable=False, unique=True),
        Column("mtz_path", Path),
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
        schema=_analysis_schema(schemata),
    )


def table_refinement(
    metadata: MetaData,
    data_reduction: Table,
    schemata: Schemata,
) -> Table:
    return Table(
        "Refinement",
        metadata,
        Column("refinement_id", Integer, primary_key=True),
        Column(
            "data_reduction_id",
            Integer,
            ForeignKey(_fk_identifier(data_reduction.c.data_reduction_id)),
        ),
        Column("analysis_time", DateTime, nullable=False),
        Column("folder_path", Path),
        Column("initial_pdb_path", Path),
        Column("final_pdb_path", Path),
        Column("refinement_mtz_path", Path),
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
        schema=_analysis_schema(schemata),
    )


def table_diffractions(
    metadata: MetaData, crystals: Table, schemata: Schemata
) -> Table:
    return Table(
        "Diffractions",
        metadata,
        Column(
            "crystal_id",
            String(length=255),
            ForeignKey(
                _fk_identifier(crystals.c.crystal_id),
                ondelete="CASCADE",
                onupdate="CASCADE",
            ),
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
        Column("data_raw_filename_pattern", Path, comment="regexp"),
        Column("microscope_image_filename_pattern", Path, comment="regexp"),
        Column("aperture_horizontal", Float, comment="um"),
        Column("aperture_vertical", Float, comment="um"),
        schema=_main_schema(schemata),
    )


class DBToolTables:
    def __init__(
        self,
        metadata: MetaData,
        schemata: Schemata,
        crystals: Table,
        diffs: Table,
        reductions: Table,
        refinements: Table,
    ) -> None:
        self.tools = table_tools(metadata, schemata)
        self.jobs = table_jobs(metadata, self.tools, schemata)
        self.job_working_on_diffraction = table_job_working_on_diffraction(
            metadata, self.jobs, crystals, diffs, schemata
        )
        self.job_has_reduction_result = table_job_has_reduction_result(
            metadata,
            self.jobs,
            reductions,
            schemata,
        )
        self.job_working_on_reduction = table_job_working_on_reduction(
            metadata, self.jobs, reductions, schemata
        )
        self.job_has_refinement_result = table_job_has_refinement_result(
            metadata, self.jobs, refinements, schemata
        )


class DBTables:
    def __init__(
        self,
        metadata: MetaData,
        with_tools: bool,
        with_estimated_resolution: bool,
        schemata: Schemata,
        engine: Optional[Engine] = None,
    ) -> None:
        self.metadata = metadata
        self.with_estimated_resolution = with_estimated_resolution
        self.pucks = table_pucks(metadata, schemata)
        self.dewar_lut = table_dewar_lut(metadata, self.pucks, schemata)
        self.crystals = (
            table_crystals(metadata, self.pucks, schemata)
            if engine is None
            else Table("Crystals", metadata, autoload_with=engine)
        )
        self.diffs = table_diffractions(metadata, self.crystals, schemata)
        self.reductions = table_data_reduction(metadata, self.crystals, schemata)
        self.refinements = table_refinement(metadata, self.reductions, schemata)
        self.tools: Optional[Table]
        self.jobs: Optional[Table]
        self.job_working_on_diffraction: Optional[Table]
        self.job_has_reduction_result: Optional[Table]
        self.tool_tables: Optional[DBToolTables]
        if with_tools:
            self.tool_tables = DBToolTables(
                metadata,
                schemata,
                self.crystals,
                self.diffs,
                self.reductions,
                self.refinements,
            )
        else:
            self.tool_tables = None

    def load_from_engine(self, engine: Engine) -> None:
        self.crystals = Table(
            "Crystals", self.metadata, autoload_with=engine, extend_existing=True
        )
