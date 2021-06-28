from dataclasses import dataclass


@dataclass(frozen=True, eq=True)
class RunKey:
    crystal_id: str
    run_id: int
