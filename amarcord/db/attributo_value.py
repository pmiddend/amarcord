import datetime
from typing import List, Union

from amarcord.db.comment import DBComment

AttributoValue = Union[
    List[DBComment],
    str,
    int,
    float,
    datetime.datetime,
    datetime.timedelta,
    List[str],
    None,
]
