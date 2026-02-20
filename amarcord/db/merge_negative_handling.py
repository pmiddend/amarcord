from enum import StrEnum


# str to make it JSON serializable
class MergeNegativeHandling(StrEnum):
    IGNORE = "ignore"
    ZERO = "zero"
