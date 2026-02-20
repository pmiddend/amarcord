from enum import StrEnum


# str to make it JSON serializable
class MergeModel(StrEnum):
    UNITY = "unity"
    XSPHERE = "xsphere"
    OFFSET = "offset"
    GGPM = "ggpm"
