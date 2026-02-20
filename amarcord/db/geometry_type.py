# str to make it JSON serializable
from enum import StrEnum
from enum import unique


@unique
class GeometryType(StrEnum):
    CRYSTFEL_FILE = "crystfel_file"
    CRYSTFEL_STRING = "crystfel_string"
