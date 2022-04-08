import datetime
from typing import List
from typing import Union
from numpy import ndarray

AttributoValue = Union[
    str,
    int,
    float,
    datetime.datetime,
    bool,
    List[int],
    List[str],
    List[float],
    ndarray,
    None,
]
