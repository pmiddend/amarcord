from enum import Enum


# str to make it JSON serializable
class MergeNegativeHandling(str, Enum):
    IGNORE = "ignore"
    ZERO = "zero"
