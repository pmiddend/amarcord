from dataclasses import dataclass


@dataclass(frozen=True)
class BeamtimeScheduleEntry:
    users: str
    td_support: str
    sample_id: None | int
    date: str
    shift: str
    comment: str
