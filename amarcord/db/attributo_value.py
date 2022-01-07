import datetime
from typing import List
from typing import Union

from amarcord.db.comment import DBComment

AttributoValue = Union[
    List[DBComment],
    str,
    int,
    float,
    datetime.datetime,
    datetime.timedelta,
    List[int],
    List[str],
    List[float],
    None,
]
