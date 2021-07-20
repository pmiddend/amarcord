from dataclasses import dataclass


@dataclass(frozen=True)
class DBDewarLUT:
    dewar_position: int
    puck_id: str
