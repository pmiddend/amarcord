from datetime import datetime
from typing import Any
from typing import ClassVar
from typing import Generator

import sqlalchemy as sa
from sqlalchemy import ForeignKey
from sqlalchemy import String
from sqlalchemy.dialects.mysql import LONGBLOB
from sqlalchemy.dialects.mysql import LONGTEXT
from sqlalchemy.ext.asyncio import AsyncAttrs
from sqlalchemy.orm import DeclarativeBase
from sqlalchemy.orm import Mapped
from sqlalchemy.orm import MappedAsDataclass
from sqlalchemy.orm import mapped_column
from sqlalchemy.orm import relationship
from sqlalchemy.schema import Column
from sqlalchemy.schema import Table
from sqlalchemy.types import JSON

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.db_job_status import DBJobStatus
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.merge_model import MergeModel
from amarcord.db.merge_negative_handling import MergeNegativeHandling
from amarcord.db.run_external_id import RunExternalId
from amarcord.db.run_internal_id import RunInternalId
from amarcord.db.scale_intensities import ScaleIntensities


# see
#
# https://stackoverflow.com/questions/54026174/proper-autogenerate-of-str-implementation-also-for-sqlalchemy-classes
def keyvalgen(obj: Any) -> Generator[tuple[str, Any], None, None]:
    """Generate attr name/val pairs, filtering out SQLA attrs."""
    excl = ("_sa_adapter", "_sa_instance_state")
    for k, v in vars(obj).items():
        if not k.startswith("_") and not any(hasattr(v, a) for a in excl):  # type: ignore
            yield k, v


class Base(AsyncAttrs, DeclarativeBase, MappedAsDataclass):
    # see
    #
    # https://stackoverflow.com/questions/54026174/proper-autogenerate-of-str-implementation-also-for-sqlalchemy-classes
    def __repr__(self) -> str:
        params = ", ".join(f"{k}={v}" for k, v in keyvalgen(self))
        return f"{self.__class__.__name__}({params})"

    # See https://stackoverflow.com/questions/75379948/what-is-correct-mapped-annotation-for-json-in-sqlalchemy-2-x-version
    type_annotation_map: ClassVar = {
        RunInternalId: sa.Integer,
        RunExternalId: sa.Integer,
        BeamtimeId: sa.Integer,
        AttributoId: sa.Integer,
        dict[str, Any]: JSON,
    }


class Geometry(Base):
    __tablename__ = "Geometry"

    # Real attributes
    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    beamtime_id: Mapped[BeamtimeId] = mapped_column(
        ForeignKey("Beamtime.id", ondelete="cascade"),
    )
    content: Mapped[str] = mapped_column(sa.Text)
    hash: Mapped[str] = mapped_column(sa.String(length=64))
    name: Mapped[str] = mapped_column(sa.String(length=255))
    created: Mapped[datetime] = mapped_column()
    # parent_geometry_id: Mapped[None | int] = mapped_column(
    #     ForeignKey("Geometry.id", ondelete="cascade"),
    # )

    # Relationships
    beamtime: Mapped["Beamtime"] = relationship(back_populates="geometries", init=False)
    indexing_parameters: Mapped[list["IndexingParameters"]] = relationship(
        back_populates="geometry",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    # These two just don't work, and we have no idea why that is (yet).
    # indexing_results: Mapped[list["IndexingResult"]] = relationship(
    #     backref="geometry",
    #     cascade="all, delete, delete-orphan",
    #     default_factory=list,
    # )
    # generated_indexing_results: Mapped[list["IndexingResult"]] = relationship(
    #     back_populates="generated_geometry",
    #     cascade="all, delete, delete-orphan",
    #     default_factory=list,
    # )
    # parent_geometry: Mapped["Geometry"] = relationship(back_populates="parent_geometry_id")


class Beamtime(Base):
    __tablename__ = "Beamtime"

    # Real attributes
    id: Mapped[BeamtimeId] = mapped_column(init=False, primary_key=True)
    external_id: Mapped[str] = mapped_column(sa.String(length=255))
    proposal: Mapped[str] = mapped_column(sa.String(length=255))
    beamline: Mapped[str] = mapped_column(sa.String(length=255))
    title: Mapped[str] = mapped_column(sa.String(length=255))
    comment: Mapped[str] = mapped_column(sa.Text)
    start: Mapped[datetime] = mapped_column()
    end: Mapped[datetime] = mapped_column()
    analysis_output_path: Mapped[str] = mapped_column(sa.Text)

    # Relationships
    geometries: Mapped[list["Geometry"]] = relationship(
        back_populates="beamtime",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    experiment_types: Mapped[list["ExperimentType"]] = relationship(
        back_populates="beamtime",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    configurations: Mapped[list["UserConfiguration"]] = relationship(
        back_populates="beamtime",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    attributi: Mapped[list["Attributo"]] = relationship(
        back_populates="beamtime",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    schedules: Mapped[list["BeamtimeSchedule"]] = relationship(
        back_populates="beamtime",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    chemicals: Mapped[list["Chemical"]] = relationship(
        back_populates="beamtime",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    events: Mapped[list["EventLog"]] = relationship(
        back_populates="beamtime",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    runs: Mapped[list["Run"]] = relationship(
        back_populates="beamtime",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )


class ExperimentType(Base):
    __tablename__ = "ExperimentType"

    # Real attributes
    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    beamtime_id: Mapped[BeamtimeId] = mapped_column(
        ForeignKey("Beamtime.id", ondelete="cascade"),
    )
    name: Mapped[str] = mapped_column(sa.String(length=255))

    # Relationship
    beamtime: Mapped[Beamtime] = relationship(
        back_populates="experiment_types",
        init=False,
    )
    runs: Mapped[list["Run"]] = relationship(
        back_populates="experiment_type",
        default_factory=list,
    )
    # an experiment type without attributes is pretty useless, so it makes sense to always load them together
    attributi: Mapped[list["ExperimentHasAttributo"]] = relationship(
        back_populates="experiment_type",
        lazy="selectin",
        default_factory=list,
    )
    data_sets: Mapped[list["DataSet"]] = relationship(
        back_populates="experiment_type",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    configurations: Mapped[list["UserConfiguration"]] = relationship(
        back_populates="current_experiment_type",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )


class Attributo(Base):
    __tablename__ = "Attributo"

    # Real attributes
    beamtime_id: Mapped[BeamtimeId] = mapped_column(
        ForeignKey("Beamtime.id", ondelete="cascade"),
    )
    id: Mapped[AttributoId] = mapped_column(init=False, primary_key=True)
    name: Mapped[str] = mapped_column(String(length=255))
    description: Mapped[str] = mapped_column(String(length=255))
    group: Mapped[str] = mapped_column(String(length=255))
    associated_table: Mapped[AssociatedTable] = mapped_column(sa.Enum(AssociatedTable))
    json_schema: Mapped[dict[str, Any]] = mapped_column(JSON)

    # Relationships
    beamtime: Mapped[Beamtime] = relationship(back_populates="attributi", init=False)
    experiment_types: Mapped[list["ExperimentHasAttributo"]] = relationship(
        back_populates="attributo",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    chemical_values: Mapped[list["ChemicalHasAttributoValue"]] = relationship(
        back_populates="attributo",
        cascade="all,delete,delete-orphan",
        default_factory=list,
    )
    run_values: Mapped[list["RunHasAttributoValue"]] = relationship(
        back_populates="attributo",
        cascade="all,delete,delete-orphan",
        default_factory=list,
    )
    data_set_values: Mapped[list["DataSetHasAttributoValue"]] = relationship(
        back_populates="attributo",
        cascade="all,delete,delete-orphan",
        default_factory=list,
    )


chemical_has_file = Table(
    "ChemicalHasFile",
    Base.metadata,
    Column[int]("chemical_id", ForeignKey("Chemical.id", ondelete="cascade")),
    Column[int]("file_id", ForeignKey("File.id", ondelete="cascade")),
)

event_has_file = Table(
    "EventHasFile",
    Base.metadata,
    Column[int]("event_id", ForeignKey("EventLog.id", ondelete="cascade")),
    Column[int]("file_id", ForeignKey("File.id", ondelete="cascade")),
)


class File(Base):
    __tablename__ = "File"

    # Real attributes
    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    type: Mapped[str] = mapped_column(sa.String(length=255))
    file_name: Mapped[str] = mapped_column(sa.String(length=255))
    size_in_bytes: Mapped[int] = mapped_column()
    size_in_bytes_compressed: Mapped[None | int] = mapped_column()
    original_path: Mapped[None | str] = mapped_column(sa.Text)
    sha256: Mapped[str] = mapped_column(sa.String(length=64))
    modified: Mapped[datetime] = mapped_column()
    # See https://stackoverflow.com/questions/43791725/sqlalchemy-how-to-make-a-longblob-column-in-mysql
    contents: Mapped[bytes] = mapped_column(
        sa.LargeBinary().with_variant(LONGBLOB, "mysql"),
        # we defer, since we don't want to accidentally load the contents of a huge file just because we want a list
        # of all files, or something, see
        #
        # https://docs.sqlalchemy.org/en/20/orm/queryguide/columns.html#configuring-column-deferral-on-mappings
        deferred=True,
    )
    description: Mapped[str] = mapped_column(sa.String(length=255))

    # Relationships
    chemicals: Mapped[list["Chemical"]] = relationship(
        secondary=chemical_has_file,
        back_populates="files",
        default_factory=list,
    )
    # This gives some weird errors, and the relationship isn't that important either
    # merge_result_mtz_files: Mapped[list["MergeResult"]] = relationship(
    #     back_populates="mtz_file"
    # )
    # refinement_result_mtz_files: Mapped[list["RefinementResult"]] = relationship(
    #     back_populates="mtz_file", foreign_keys=[id]
    # )
    # refinement_result_pdb_files: Mapped[list["RefinementResult"]] = relationship(
    #     back_populates="pdb_file"
    # )
    events: Mapped[list["EventLog"]] = relationship(
        back_populates="files",
        secondary=event_has_file,
        default_factory=list,
    )


beamtime_schedule_has_chemical = Table(
    "BeamtimeScheduleHasChemical",
    Base.metadata,
    Column[int](
        "beamtime_schedule_id",
        ForeignKey("BeamtimeSchedule.id", ondelete="cascade"),
    ),
    Column[int]("chemical_id", ForeignKey("Chemical.id", ondelete="cascade")),
    sa.PrimaryKeyConstraint(
        "beamtime_schedule_id",
        "chemical_id",
        name="BeamtimeScheduleHasChemical_pk",
    ),
)


class BeamtimeSchedule(Base):
    __tablename__ = "BeamtimeSchedule"

    # Real attributes
    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    beamtime_id: Mapped[BeamtimeId] = mapped_column(
        ForeignKey("Beamtime.id", ondelete="cascade"),
    )
    users: Mapped[str] = mapped_column(sa.String(length=255))
    td_support: Mapped[str] = mapped_column(sa.String(length=255))
    comment: Mapped[str] = mapped_column(sa.Text)
    shift: Mapped[str] = mapped_column(sa.String(length=255))
    date: Mapped[str] = mapped_column(sa.String(length=10))

    # Relationships
    chemicals: Mapped[list["Chemical"]] = relationship(
        secondary=beamtime_schedule_has_chemical,
        back_populates="schedule_items",
        lazy="selectin",
        default_factory=list,
    )
    beamtime: Mapped[Beamtime] = relationship(back_populates="schedules", init=False)


class Chemical(Base):
    __tablename__ = "Chemical"

    # Real attributes
    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    beamtime_id: Mapped[BeamtimeId] = mapped_column(
        ForeignKey("Beamtime.id", ondelete="cascade"),
    )
    name: Mapped[str] = mapped_column(sa.String(length=255))
    responsible_person: Mapped[str] = mapped_column(sa.String(length=255))
    modified: Mapped[datetime] = mapped_column()
    type: Mapped[ChemicalType] = mapped_column(sa.Enum(ChemicalType))

    # Relationships
    beamtime: Mapped[Beamtime] = relationship(back_populates="chemicals", init=False)
    files: Mapped[list[File]] = relationship(
        secondary=chemical_has_file,
        default_factory=list,
    )
    schedule_items: Mapped[list[BeamtimeSchedule]] = relationship(
        secondary=beamtime_schedule_has_chemical,
        back_populates="chemicals",
        default_factory=list,
    )
    attributo_values: Mapped[list["ChemicalHasAttributoValue"]] = relationship(
        back_populates="chemical",
        cascade="all, delete",
        lazy="selectin",
        default_factory=list,
    )


class ChemicalHasAttributoValue(Base):
    __tablename__ = "ChemicalHasAttributoValue"

    # Real attributes
    chemical_id: Mapped[int] = mapped_column(
        ForeignKey("Chemical.id", ondelete="cascade"),
        primary_key=True,
        init=False,
    )
    attributo_id: Mapped[int] = mapped_column(
        ForeignKey("Attributo.id", ondelete="cascade"),
        primary_key=True,
    )
    integer_value: Mapped[None | int] = mapped_column(nullable=True)
    float_value: Mapped[None | float] = mapped_column(nullable=True)
    string_value: Mapped[None | str] = mapped_column(sa.Text, nullable=True)
    bool_value: Mapped[None | bool] = mapped_column(nullable=True)
    datetime_value: Mapped[None | datetime] = mapped_column(nullable=True)
    list_value: Mapped[None | list[Any]] = mapped_column(sa.JSON, nullable=True)

    # Relationships
    chemical: Mapped[Chemical] = relationship(
        back_populates="attributo_values",
        init=False,
    )
    attributo: Mapped[Attributo] = relationship(
        back_populates="chemical_values",
        init=False,
    )


class Run(Base):
    __tablename__ = "Run"

    # Real attributes
    id: Mapped[RunInternalId] = mapped_column(primary_key=True, init=False)
    external_id: Mapped[RunExternalId] = mapped_column()
    beamtime_id: Mapped[BeamtimeId] = mapped_column(
        ForeignKey("Beamtime.id", ondelete="cascade"),
    )
    modified: Mapped[datetime] = mapped_column()
    started: Mapped[datetime] = mapped_column()
    stopped: Mapped[None | datetime] = mapped_column()
    experiment_type_id: Mapped[int] = mapped_column(
        ForeignKey("ExperimentType.id", name="run_has_experiment_type_fk"),
    )

    # Relationships
    beamtime: Mapped[Beamtime] = relationship(back_populates="runs", init=False)
    experiment_type: Mapped[ExperimentType] = relationship(
        back_populates="runs",
        init=False,
    )
    indexing_results: Mapped[list["IndexingResult"]] = relationship(
        back_populates="run",
        default_factory=list,
    )
    attributo_values: Mapped[list["RunHasAttributoValue"]] = relationship(
        back_populates="run",
        cascade="all, delete, delete-orphan",
        lazy="selectin",
        default_factory=list,
    )
    files: Mapped[list["RunHasFiles"]] = relationship(
        back_populates="run",
        default_factory=list,
    )


class RunHasFiles(Base):
    __tablename__ = "RunHasFiles"

    # Real attributes
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    run_id: Mapped[int] = mapped_column(
        ForeignKey("Run.id", ondelete="cascade"),
        init=False,
    )
    glob: Mapped[str] = mapped_column()
    source: Mapped[str] = mapped_column(String(length=255))

    # Relationships
    run: Mapped[Run] = relationship(back_populates="files", init=False)


class RunHasAttributoValue(Base):
    __tablename__ = "RunHasAttributoValue"

    # Real attributes
    run_id: Mapped[RunInternalId] = mapped_column(
        ForeignKey("Run.id", ondelete="cascade"),
        primary_key=True,
        init=False,
    )
    attributo_id: Mapped[AttributoId] = mapped_column(
        ForeignKey("Attributo.id", ondelete="cascade"),
        primary_key=True,
    )
    integer_value: Mapped[None | int] = mapped_column(nullable=True)
    float_value: Mapped[None | float] = mapped_column(nullable=True)
    string_value: Mapped[None | str] = mapped_column(sa.Text, nullable=True)
    bool_value: Mapped[None | bool] = mapped_column(nullable=True)
    datetime_value: Mapped[None | datetime] = mapped_column(nullable=True)
    list_value: Mapped[None | list[Any]] = mapped_column(sa.JSON, nullable=True)
    chemical_value: Mapped[None | int] = mapped_column(
        ForeignKey("Chemical.id", ondelete="cascade"),
    )

    # Relationships
    run: Mapped[Run] = relationship(back_populates="attributo_values", init=False)
    attributo: Mapped[Attributo] = relationship(back_populates="run_values", init=False)
    chemical: Mapped[None | Chemical] = relationship(init=False)


class ExperimentHasAttributo(Base):
    __tablename__ = "ExperimentHasAttributo"

    # Real attributes
    experiment_type_id: Mapped[int] = mapped_column(
        ForeignKey("ExperimentType.id"),
        primary_key=True,
        init=False,
    )
    attributo_id: Mapped[int] = mapped_column(
        ForeignKey("Attributo.id"),
        primary_key=True,
    )
    chemical_role: Mapped[ChemicalType] = mapped_column(sa.Enum(ChemicalType))

    # Relationships
    experiment_type: Mapped[ExperimentType] = relationship(
        back_populates="attributi",
        foreign_keys=[experiment_type_id],
        init=False,
    )
    attributo: Mapped["Attributo"] = relationship(
        back_populates="experiment_types",
        init=False,
    )


class DataSet(Base):
    __tablename__ = "DataSet"

    # Real attributes
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    experiment_type_id: Mapped[int] = mapped_column(ForeignKey("ExperimentType.id"))

    # Relationships
    experiment_type: Mapped[ExperimentType] = relationship(
        back_populates="data_sets",
        init=False,
    )
    attributo_values: Mapped[list["DataSetHasAttributoValue"]] = relationship(
        back_populates="data_set",
        cascade="all,delete,delete-orphan",
        lazy="selectin",
        default_factory=list,
    )


class DataSetHasAttributoValue(Base):
    __tablename__ = "DataSetHasAttributoValue"

    # Real attributes
    data_set_id: Mapped[int] = mapped_column(
        ForeignKey("DataSet.id", ondelete="cascade"),
        primary_key=True,
        init=False,
    )
    attributo_id: Mapped[AttributoId] = mapped_column(
        ForeignKey("Attributo.id", ondelete="cascade"),
        primary_key=True,
    )
    integer_value: Mapped[None | int] = mapped_column(nullable=True)
    # no idea why pyright doesn't simply infer Column[float] as the type
    float_value: Mapped[None | float] = mapped_column(nullable=True)
    string_value: Mapped[None | str] = mapped_column(sa.Text, nullable=True)
    bool_value: Mapped[None | bool] = mapped_column(nullable=True)
    datetime_value: Mapped[None | datetime] = mapped_column(nullable=True)
    list_value: Mapped[None | list[Any]] = mapped_column(sa.JSON, nullable=True)
    chemical_value: Mapped[None | int] = mapped_column(
        ForeignKey("Chemical.id", ondelete="cascade"),
    )

    def is_value_equal(self, b: "DataSetHasAttributoValue") -> bool:
        return (
            self.integer_value == b.integer_value
            and self.float_value == b.float_value
            and self.string_value == b.string_value
            and self.bool_value == b.bool_value
            and self.datetime_value == b.datetime_value
            and self.list_value == b.list_value
            and self.chemical_value == b.chemical_value
        )

    # Relationships
    data_set: Mapped[DataSet] = relationship(
        back_populates="attributo_values",
        init=False,
    )
    attributo: Mapped[Attributo] = relationship(
        back_populates="data_set_values",
        init=False,
    )
    chemical: Mapped[None | Chemical] = relationship(init=False)


class EventLog(Base):
    __tablename__ = "EventLog"

    # Real attributes
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    beamtime_id: Mapped[BeamtimeId] = mapped_column(
        ForeignKey("Beamtime.id", ondelete="cascade"),
    )
    created: Mapped[datetime] = mapped_column()
    level: Mapped[EventLogLevel] = mapped_column(sa.Enum(EventLogLevel))
    source: Mapped[str] = mapped_column(sa.String(length=255))
    text: Mapped[str] = mapped_column(sa.Text)

    # Relationships
    beamtime: Mapped[Beamtime] = relationship(back_populates="events", init=False)
    files: Mapped[list[File]] = relationship(
        back_populates="events",
        secondary=event_has_file,
        default_factory=list,
    )


merge_result_has_indexing_result = Table(
    "MergeResultHasIndexingResult",
    Base.metadata,
    Column[int]("merge_result_id", ForeignKey("MergeResult.id", ondelete="cascade")),
    Column[int](
        "indexing_result_id",
        ForeignKey("IndexingResult.id", ondelete="cascade"),
    ),
)


class IndexingParameters(Base):
    __tablename__ = "IndexingParameters"

    # Real attributes
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    is_online: Mapped[bool] = mapped_column()
    cell_description: Mapped[None | str] = mapped_column(sa.String(length=255))
    geometry_id: Mapped[None | int] = mapped_column(
        ForeignKey("Geometry.id", ondelete="cascade"),
    )
    command_line: Mapped[str] = mapped_column(sa.Text)
    source: Mapped[str] = mapped_column(sa.String(length=255))

    # Relationships
    indexing_results: Mapped[list["IndexingResult"]] = relationship(
        back_populates="indexing_parameters",
        default_factory=list,
    )
    configurations: Mapped[list["UserConfiguration"]] = relationship(
        back_populates="current_online_indexing_parameters",
        default_factory=list,
    )
    geometry: Mapped[None | Geometry] = relationship(
        back_populates="indexing_parameters", init=False
    )


def are_indexing_parameters_equal(a: IndexingParameters, b: IndexingParameters) -> bool:
    return (a.id is not None and a.id == b.id) or (  # type: ignore
        a.is_online == b.is_online
        and a.cell_description == b.cell_description
        and a.command_line == b.command_line
        and a.geometry_file == b.geometry_file
        and a.source == b.source
    )


class UserConfiguration(Base):
    __tablename__ = "UserConfiguration"

    # Real attributes
    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    beamtime_id: Mapped[BeamtimeId] = mapped_column(
        ForeignKey("Beamtime.id", ondelete="cascade"),
    )
    created: Mapped[datetime] = mapped_column()
    auto_pilot: Mapped[bool] = mapped_column()
    use_online_crystfel: Mapped[bool] = mapped_column()
    current_experiment_type_id: Mapped[None | int] = mapped_column(
        ForeignKey("ExperimentType.id", ondelete="cascade"),
    )
    current_online_indexing_parameters_id: Mapped[None | int] = mapped_column(
        ForeignKey("IndexingParameters.id", ondelete="cascade"),
    )

    # Relationships
    current_experiment_type: Mapped[None | ExperimentType] = relationship(
        back_populates="configurations",
        init=False,
    )
    beamtime: Mapped[Beamtime] = relationship(
        back_populates="configurations",
        init=False,
    )
    current_online_indexing_parameters: Mapped[None | IndexingParameters] = (
        relationship(back_populates="configurations", init=False)
    )


class AlignDetectorGroup(Base):
    __tablename__ = "AlignDetectorGroup"

    # Real attributes
    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    indexing_result_id: Mapped[int] = mapped_column(
        ForeignKey("IndexingResult.id", ondelete="cascade"), init=False
    )
    group: Mapped[str] = mapped_column()
    x_translation_mm: Mapped[float] = mapped_column()
    y_translation_mm: Mapped[float] = mapped_column()
    z_translation_mm: Mapped[None | float] = mapped_column()
    x_rotation_deg: Mapped[None | float] = mapped_column()
    y_rotation_deg: Mapped[None | float] = mapped_column()

    # Relationships
    indexing_result: Mapped["IndexingResult"] = relationship(
        back_populates="align_detector_groups", init=False
    )


class IndexingResult(Base):
    __tablename__ = "IndexingResult"

    # Real attributes
    id: Mapped[int] = mapped_column(init=False, primary_key=True)
    created: Mapped[datetime] = mapped_column()
    run_id: Mapped[RunInternalId] = mapped_column(
        ForeignKey("Run.id", ondelete="cascade"),
    )
    stream_file: Mapped[None | str] = mapped_column(sa.Text)
    program_version: Mapped[None | str] = mapped_column(sa.String(length=255))
    frames: Mapped[None | int] = mapped_column()
    hits: Mapped[None | int] = mapped_column()
    indexed_frames: Mapped[int] = mapped_column()
    geometry_id: Mapped[None | int] = mapped_column(
        ForeignKey("Geometry.id", ondelete="cascade")
    )
    generated_geometry_id: Mapped[None | int] = mapped_column(
        ForeignKey("Geometry.id", ondelete="cascade")
    )
    # In some of AMARCORD's code, this will be called "workload
    # manager job ID" (see workload managers in the docs)
    job_id: Mapped[None | int] = mapped_column()
    job_status: Mapped[DBJobStatus] = mapped_column(sa.Enum(DBJobStatus))
    job_error: Mapped[None | str] = mapped_column(
        sa.Text().with_variant(LONGTEXT, "mysql"),
    )
    job_latest_log: Mapped[None | str] = mapped_column(sa.Text)
    job_started: Mapped[None | datetime] = mapped_column()
    job_stopped: Mapped[None | datetime] = mapped_column()
    indexing_parameters_id: Mapped[int] = mapped_column(
        ForeignKey("IndexingParameters.id", ondelete="cascade"),
    )
    unit_cell_histograms_file_id: Mapped[None | int] = mapped_column(
        ForeignKey("File.id", ondelete="cascade"),
    )

    # Relationships
    align_detector_groups: Mapped[list["AlignDetectorGroup"]] = relationship(
        back_populates="indexing_result",
        lazy="selectin",
        cascade="all, delete",
        default_factory=list,
    )
    merge_results: Mapped[list["MergeResult"]] = relationship(
        back_populates="indexing_results",
        secondary=merge_result_has_indexing_result,
        cascade="all, delete",
        default_factory=list,
    )
    run: Mapped[Run] = relationship(back_populates="indexing_results", init=False)
    statistics: Mapped[list["IndexingResultHasStatistic"]] = relationship(
        back_populates="indexing_result",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    indexing_parameters: Mapped[IndexingParameters] = relationship(
        back_populates="indexing_results",
        init=False,
    )


class IndexingResultHasStatistic(Base):
    __tablename__ = "IndexingResultHasStatistic"

    # Real attributes
    indexing_result_id: Mapped[int] = mapped_column(
        ForeignKey("IndexingResult.id", ondelete="cascade"),
        primary_key=True,
    )
    time: Mapped[datetime] = mapped_column(primary_key=True)
    frames: Mapped[int] = mapped_column()
    hits: Mapped[int] = mapped_column()
    indexed_frames: Mapped[int] = mapped_column()
    indexed_crystals: Mapped[int] = mapped_column()

    # Relationships
    indexing_result: Mapped[IndexingResult] = relationship(
        back_populates="statistics",
        init=False,
    )


class MergeResult(Base):
    __tablename__ = "MergeResult"

    # Real attributes
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    created: Mapped[datetime] = mapped_column()
    recent_log: Mapped[str] = mapped_column(sa.Text)
    negative_handling: Mapped[None | MergeNegativeHandling] = mapped_column(
        sa.Enum(MergeNegativeHandling),
    )
    job_status: Mapped[DBJobStatus] = mapped_column(sa.Enum(DBJobStatus))
    started: Mapped[None | datetime] = mapped_column()
    stopped: Mapped[None | datetime] = mapped_column()
    point_group: Mapped[str] = mapped_column(sa.String(length=32))
    space_group: Mapped[None | str] = mapped_column(sa.String(length=32))
    cell_description: Mapped[str] = mapped_column(sa.String(length=255))
    job_id: Mapped[None | int] = mapped_column()
    job_error: Mapped[None | str] = mapped_column(sa.Text)
    mtz_file_id: Mapped[None | int] = mapped_column(
        ForeignKey("File.id", ondelete="cascade"),
    )
    input_merge_model: Mapped[MergeModel] = mapped_column(sa.Enum(MergeModel))
    input_scale_intensities: Mapped[ScaleIntensities] = mapped_column(
        sa.Enum(ScaleIntensities),
    )
    input_post_refinement: Mapped[bool] = mapped_column()
    input_iterations: Mapped[int] = mapped_column()
    input_polarisation_angle: Mapped[None | int] = mapped_column()
    input_polarisation_percent: Mapped[None | int] = mapped_column()
    input_start_after: Mapped[None | int] = mapped_column()
    input_stop_after: Mapped[None | int] = mapped_column()
    input_rel_b: Mapped[float] = mapped_column()
    input_no_pr: Mapped[None | bool] = mapped_column()
    input_force_bandwidth: Mapped[None | float] = mapped_column()
    input_force_radius: Mapped[None | float] = mapped_column()
    input_force_lambda: Mapped[None | float] = mapped_column()
    input_no_delta_cc_half: Mapped[bool] = mapped_column()
    input_max_adu: Mapped[None | float] = mapped_column()
    input_min_measurements: Mapped[int] = mapped_column()
    input_logs: Mapped[bool] = mapped_column()
    input_min_res: Mapped[None | float] = mapped_column()
    input_push_res: Mapped[None | float] = mapped_column()
    input_w: Mapped[None | str] = mapped_column(sa.String(length=255))
    fom_snr: Mapped[None | float] = mapped_column(init=False)
    fom_wilson: Mapped[None | float] = mapped_column(init=False)
    fom_ln_k: Mapped[None | float] = mapped_column(init=False)
    fom_discarded_reflections: Mapped[None | int] = mapped_column(init=False)
    fom_one_over_d_from: Mapped[None | float] = mapped_column(init=False)
    fom_one_over_d_to: Mapped[None | float] = mapped_column(init=False)
    fom_redundancy: Mapped[None | float] = mapped_column(init=False)
    fom_completeness: Mapped[None | float] = mapped_column(init=False)
    fom_measurements_total: Mapped[None | int] = mapped_column(init=False)
    fom_reflections_total: Mapped[None | int] = mapped_column(init=False)
    fom_reflections_possible: Mapped[None | int] = mapped_column(init=False)
    fom_r_split: Mapped[None | float] = mapped_column(init=False)
    fom_r1i: Mapped[None | float] = mapped_column(init=False)
    fom_2: Mapped[None | float] = mapped_column(init=False)
    fom_cc: Mapped[None | float] = mapped_column(init=False)
    fom_ccstar: Mapped[None | float] = mapped_column(init=False)
    fom_ccano: Mapped[None | float] = mapped_column(init=False)
    fom_crdano: Mapped[None | float] = mapped_column(init=False)
    fom_rano: Mapped[None | float] = mapped_column(init=False)
    fom_rano_over_r_split: Mapped[None | float] = mapped_column(init=False)
    fom_d1sig: Mapped[None | float] = mapped_column(init=False)
    fom_d2sig: Mapped[None | float] = mapped_column(init=False)
    fom_outer_resolution: Mapped[None | float] = mapped_column(init=False)
    fom_outer_ccstar: Mapped[None | float] = mapped_column(init=False)
    fom_outer_r_split: Mapped[None | float] = mapped_column(init=False)
    fom_outer_cc: Mapped[None | float] = mapped_column(init=False)
    fom_outer_unique_reflections: Mapped[None | int] = mapped_column(init=False)
    fom_outer_completeness: Mapped[None | float] = mapped_column(init=False)
    fom_outer_redundancy: Mapped[None | float] = mapped_column(init=False)
    fom_outer_snr: Mapped[None | float] = mapped_column(init=False)
    fom_outer_min_res: Mapped[None | float] = mapped_column(init=False)
    fom_outer_max_res: Mapped[None | float] = mapped_column(init=False)
    ambigator_command_line: Mapped[None | str] = mapped_column()
    ambigator_fg_graph_file_id: Mapped[None | int] = mapped_column(
        ForeignKey("File.id", ondelete="cascade"),
    )
    cutoff_lowres: Mapped[None | float] = mapped_column()
    cutoff_highres: Mapped[None | str] = mapped_column()

    # Relationships
    mtz_file: Mapped[None | File] = relationship(init=False, foreign_keys=[mtz_file_id])
    refinement_results: Mapped[list["RefinementResult"]] = relationship(
        back_populates="merge_result",
        cascade="all, delete, delete-orphan",
        default_factory=list,
    )
    # These belong together, always
    shell_foms: Mapped[list["MergeResultShellFom"]] = relationship(
        back_populates="merge_result",
        cascade="all, delete, delete-orphan",
        lazy="selectin",
        default_factory=list,
    )
    indexing_results: Mapped[list["IndexingResult"]] = relationship(
        secondary=merge_result_has_indexing_result,
        back_populates="merge_results",
        default_factory=list,
    )


class MergeResultShellFom(Base):
    __tablename__ = "MergeResultShellFom"

    # Real attributes
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    merge_result_id: Mapped[int] = mapped_column(
        ForeignKey("MergeResult.id", ondelete="cascade"),
        init=False,
    )
    one_over_d_centre: Mapped[float] = mapped_column()
    nref: Mapped[int] = mapped_column()
    d_over_a: Mapped[float] = mapped_column()
    min_res: Mapped[float] = mapped_column()
    max_res: Mapped[float] = mapped_column()
    cc: Mapped[float] = mapped_column()
    ccstar: Mapped[float] = mapped_column()
    r_split: Mapped[float] = mapped_column()
    reflections_possible: Mapped[int] = mapped_column()
    completeness: Mapped[float] = mapped_column()
    measurements: Mapped[int] = mapped_column()
    redundancy: Mapped[float] = mapped_column()
    snr: Mapped[float] = mapped_column()
    mean_i: Mapped[float] = mapped_column()

    # Relationships
    merge_result: Mapped["MergeResult"] = relationship(
        back_populates="shell_foms",
        init=False,
    )


class RefinementResult(Base):
    __tablename__ = "RefinementResult"

    # Real attributes
    id: Mapped[int] = mapped_column(primary_key=True, init=False)
    merge_result_id: Mapped[int] = mapped_column(
        ForeignKey("MergeResult.id", ondelete="cascade"),
    )
    pdb_file_id: Mapped[int] = mapped_column(ForeignKey("File.id", ondelete="cascade"))
    mtz_file_id: Mapped[int] = mapped_column(ForeignKey("File.id", ondelete="cascade"))
    r_free: Mapped[float] = mapped_column()
    r_work: Mapped[float] = mapped_column()
    rms_bond_angle: Mapped[float] = mapped_column()
    rms_bond_length: Mapped[float] = mapped_column()

    # Relationships
    merge_result: Mapped["MergeResult"] = relationship(
        back_populates="refinement_results",
        init=False,
    )
    pdb_file: Mapped[File] = relationship(foreign_keys=[pdb_file_id], init=False)
    mtz_file: Mapped[File] = relationship(foreign_keys=[mtz_file_id], init=False)
