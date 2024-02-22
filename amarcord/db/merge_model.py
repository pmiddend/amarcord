from enum import Enum


# str to make it JSON serializable
class MergeModel(str, Enum):
    UNITY = "unity"
    XSPHERE = "xsphere"
    OFFSET = "offset"
    GGPM = "ggpm"
