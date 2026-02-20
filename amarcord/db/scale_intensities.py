from enum import StrEnum


# str to make it JSON serializable
class ScaleIntensities(StrEnum):
    OFF = "off"
    NORMAL = "normal"
    DEBYE_WALLER = "debyewaller"
