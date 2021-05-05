import enum
from typing import Optional

from sqlalchemy import Column
from sqlalchemy import DateTime
from sqlalchemy import Enum
from sqlalchemy import Float
from sqlalchemy import ForeignKey
from sqlalchemy import Integer
from sqlalchemy import MetaData
from sqlalchemy import SmallInteger
from sqlalchemy import String
from sqlalchemy import Table
from sqlalchemy import Text
from sqlalchemy import func

ANALYSIS_SCHEMA = "SARS_COV_2_Analysis_v2"


class Beamline(enum.Enum):
    p11 = "p11"
    p13 = "p13"
    p14 = "p14"


class DiffractionType(enum.Enum):
    no_diffraction = "no diffraction"
    no_crystal = "no crystal"
    ice_salt = "ice / salt"
    success = "success"


class ReductionMethod(enum.Enum):
    XDS_PRE = "xds_pre"
    XDS_FULL = "xds_full"
    XDS_REINDEX1 = "xds_reindex1"
    XDS_REINDER1_NOICE = "xds_reindex1_noice"
    DIALS_DIALS = "DIALS-dials"
    DIALS_1P7A_DIALS = "DIALS_1p7A-dials"
    STARANISO = "staraniso"
    OTHER = "other"


def table_crystals(metadata: MetaData, schema: Optional[str] = None) -> Table:
    return Table(
        "Crystals",
        metadata,
        Column("crystal_id", String(length=255), primary_key=True, nullable=False),
        Column("puck_id", String(length=255), index=True),
        Column("puck_position_id", SmallInteger),
        schema=schema,
    )


def table_data_reduction(metadata: MetaData, schema: Optional[str] = None) -> Table:
    return Table(
        "Data_Reduction",
        metadata,
        Column("data_reduction_id", Integer, nullable=False, primary_key=True),
        Column("crystal_id", String(length=255), nullable=False),
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


def table_diffractions(metadata: MetaData, schema: Optional[str] = None) -> Table:
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
