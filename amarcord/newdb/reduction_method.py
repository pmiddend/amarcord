import enum


class ReductionMethod(enum.Enum):
    XDS_PRE = "xds_pre"
    XDS_FULL = "xds_full"
    XDS_REINDEX1 = "xds_reindex1"
    XDS_REINDER1_NOICE = "xds_reindex1_noice"
    DIALS_DIALS = "DIALS-dials"
    DIALS_1P7A_DIALS = "DIALS_1p7A-dials"
    PLPRO_DIALS_1P6A_DIALS = "plpro_DIALS_1p6A-dials"
    PLPRO_DIALS_DIALS = "plpro_DIALS-dials"
    AGAL_DIALS = "AGAL-dials"
    STARANISO = "staraniso"
    OTHER = "other"
