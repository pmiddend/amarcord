import datetime
from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class DBComment:
    id: Optional[int]
    run_id: int
    author: str
    text: str
    created: datetime.datetime
