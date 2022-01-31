import datetime
from dataclasses import dataclass
from typing import Optional

from amarcord.db.job_status import DBJobStatus
from amarcord.json import JSONDict


@dataclass(frozen=True)
class DBIndexingJob:
    id: int
    run_id: int
    status: DBJobStatus
    started_utc: datetime.datetime
    stopped_utc: Optional[datetime.datetime]
    metadata: JSONDict
