import datetime
from dataclasses import dataclass
from typing import List
from typing import Optional

from amarcord.db.comment import DBComment
from amarcord.db.event_log_level import EventLogLevel
from amarcord.db.proposal_id import ProposalId
from amarcord.db.raw_attributi_map import RawAttributiMap


@dataclass(frozen=True)
class DBEvent:
    id: Optional[int]
    created: datetime.datetime
    level: EventLogLevel
    source: str
    text: str


@dataclass(frozen=True)
class DBSample:
    id: Optional[int]
    proposal_id: ProposalId
    name: str
    attributi: RawAttributiMap
    compounds: Optional[List[int]] = None
    micrograph: Optional[str] = None
    protocol: Optional[str] = None


@dataclass(frozen=True)
class DBRun:
    attributi: RawAttributiMap
    id: int
    sample_id: Optional[int]
    proposal_id: int
    modified: datetime.datetime
    comments: List[DBComment]
