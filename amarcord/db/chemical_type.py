from enum import Enum


# str to make it JSON serializable
class ChemicalType(str, Enum):
    CRYSTAL = "crystal"
    SOLUTION = "solution"
