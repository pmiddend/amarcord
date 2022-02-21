from dataclasses import dataclass
from typing import List


@dataclass(frozen=True)
class DBExperimentType:
    name: str
    attributo_names: List[str]
