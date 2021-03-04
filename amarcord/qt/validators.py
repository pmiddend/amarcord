import re
import datetime
from typing import List, Union, cast

from amarcord.util import str_to_float


def parse_date_time(input_: str, dtformat: str) -> Union[str, None, datetime.datetime]:
    try:
        return datetime.datetime.strptime(input_, dtformat)
    except:
        return input_


def parse_float_list(input_: str, elements: int) -> Union[str, None, List[float]]:
    parts = re.split(", *", input_)

    if parts and parts[-1] == "":
        return input_

    if len(parts) < elements:
        return input_

    if len(parts) > elements:
        return None

    floats = [str_to_float(f) for f in parts]
    return cast(List[float], floats) if all(f is not None for f in floats) else None
