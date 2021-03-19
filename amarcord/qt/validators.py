import datetime
import re
from dataclasses import dataclass
from pathlib import Path
from typing import Callable
from typing import List
from typing import Optional
from typing import TypeVar
from typing import Union
from typing import cast

from amarcord.util import str_to_float


@dataclass(frozen=True)
class Partial:
    returned_input: str


def parse_date_time(
    input_: str, dtformat: str
) -> Union[Partial, None, datetime.datetime]:
    try:
        return datetime.datetime.strptime(input_, dtformat)
    except:
        return Partial(input_)


T = TypeVar("T")


def parse_list(
    input_: str,
    elements: Optional[int],
    f: Callable[[str], Union[str, None, T]],
) -> Union[Partial, None, List[T]]:
    parts: List[str] = re.split(", *", input_)

    if parts and parts[-1] == "":
        return Partial(input_)

    if elements is not None and len(parts) < elements:
        return Partial(input_)

    if elements is not None and len(parts) > elements:
        return None

    result: List[T] = []
    for p in parts:
        part_result = f(p)
        if part_result is None:
            return None
        if not isinstance(part_result, str):
            result.append(part_result)
    return result if result else Partial(input_)


def parse_string_list(
    input_: str, elements: Optional[int]
) -> Union[Partial, None, List[str]]:
    parts: List[str] = re.split(", *", input_)

    if parts and parts[-1] == "":
        return Partial(input_)

    if elements is not None and len(parts) < elements:
        return Partial(input_)

    if elements is not None and len(parts) > elements:
        return None

    return parts


def parse_float_list(
    input_: str, min_elements: Optional[int], max_elements: Optional[int]
) -> Union[Partial, None, List[float]]:
    parts = re.split(", *", input_)

    if parts and parts[-1] == "":
        return Partial(input_)

    if min_elements is not None and len(parts) < min_elements:
        return Partial(input_)

    if max_elements is not None and len(parts) > max_elements:
        return Partial(input_)

    floats = [str_to_float(f) for f in parts]
    return cast(List[float], floats) if all(f is not None for f in floats) else None


def parse_existing_filename(input_: str) -> Union[Partial, None, str]:
    if Path(input_).exists():
        return input_
    return Partial(input_)
