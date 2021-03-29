from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
class DBMiniSample:
    sample_id: int
    sample_name: str
