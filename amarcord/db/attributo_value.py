import datetime

AttributoValue = (
    str
    | int
    | float
    | datetime.datetime
    | bool
    | list[int]
    | list[str]
    | list[float]
    | None
)
