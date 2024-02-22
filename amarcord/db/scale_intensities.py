from enum import Enum


# str to make it JSON serializable
class ScaleIntensities(str, Enum):
    OFF = "off"
    NORMAL = "normal"
    DEBYE_WALLER = "debyewaller"
