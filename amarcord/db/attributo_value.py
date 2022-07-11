import datetime
from typing import List

AttributoValue = (
    str
    | int
    | float
    | datetime.datetime
    | bool
    | List[int]
    | List[str]
    | List[float]
    | None
)
