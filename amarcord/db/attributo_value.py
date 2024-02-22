import datetime

AttributoValue = (
    str
    | int
    | float
    | datetime.datetime
    | bool
    | list[str]
    | list[float]
    | list[bool]
    | None
)
