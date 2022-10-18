from dataclasses import dataclass


@dataclass(frozen=True)
class BeamtimeScheduleEntry:
    users: str
    td_support: str
    date: str
    shift: str
    comment: str
    chemicals: list[int]
