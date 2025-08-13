# str to make it JSON serializable
from enum import Enum
from enum import unique


@unique
class GeometryType(str, Enum):
    CRYSTFEL_FILE = "crystfel_file"
    CRYSTFEL_STRING = "crystfel_string"
