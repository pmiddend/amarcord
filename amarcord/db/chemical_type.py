from enum import StrEnum


# str to make it JSON serializable
class ChemicalType(StrEnum):
    CRYSTAL = "crystal"
    SOLUTION = "solution"
