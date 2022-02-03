import datetime
import hashlib
import re
from pathlib import Path
from typing import Callable
from typing import Dict
from typing import Generator
from typing import Iterable
from typing import List
from typing import Optional
from typing import Sequence
from typing import Tuple
from typing import TypeVar
from typing import Union

import pytz

T = TypeVar("T")
U = TypeVar("U")

K = TypeVar("K")
V = TypeVar("V")


def str_to_int(s: str) -> Optional[int]:
    try:
        return int(s)
    except:
        return None


def find_regex(s: str, regex: str, start: int) -> int:
    r = re.search(regex, s[start:])
    if r is None:
        return -1
    return r.start() + start


def rfind_regex(s: str, regex: str, start: int) -> int:
    r = find_regex(s[::-1], regex, len(s) - start - 1)
    if r < 0:
        return r
    return len(s) - r - 1


# See https://stackoverflow.com/a/17016257
def remove_duplicates_stable(seq: Iterable[T]) -> List[T]:
    return list(dict.fromkeys(seq))


def dict_union(a: Sequence[Dict[K, V]]) -> Dict[K, V]:
    if not a:
        return {}
    result = a[0].copy()
    for v in a[1:]:
        result.update(v)
    return result


def str_to_float(s: str) -> Optional[float]:
    try:
        return float(s)
    except:
        return None


W = TypeVar("W")
X = TypeVar("X")


def retupled_keys(d: Dict[K, Dict[V, W]], f: Callable[[K, V], X]) -> List[X]:
    return [
        f(table, attributo_id)
        for table, attributi in d.items()
        for attributo_id, values in attributi.items()
    ]


def retuple_dict(d: Dict[K, Dict[V, W]], f: Callable[[K, V], X]) -> Dict[X, W]:
    return {
        f(table, attributo_id): values
        for table, attributi in d.items()
        for attributo_id, values in attributi.items()
    }


def create_intervals(xs: List[int]) -> Generator[Tuple[int, int], None, None]:
    if not xs:
        return
    sorted_xs = sorted(xs)
    interval_start = sorted_xs[0]
    last_element = interval_start
    for x in sorted_xs[1:]:
        if x != last_element + 1:
            yield interval_start, last_element
            interval_start = x
        last_element = x
    yield interval_start, sorted_xs[-1]


class UnexpectedEOF(Exception):
    def __init__(self) -> None:
        super().__init__("Unexpected EOF")


def find_by(xs: List[T], by: Callable[[T], bool]) -> Optional[T]:
    return next((x for x in xs if by(x)), None)


def contains(xs: List[T], by: Callable[[T], bool]) -> bool:
    for x in xs:
        if by(x):
            return True
    return False


def natural_key(string_: str) -> List[Union[int, str]]:
    """See https://blog.codinghorror.com/sorting-for-humans-natural-sort-order/"""
    return [int(s) if s.isdigit() else s for s in re.split(r"(\d+)", string_)]


def path_mtime(p: Path) -> datetime.datetime:
    return datetime.datetime.fromtimestamp(
        p.stat().st_mtime,
        tz=datetime.timezone.utc,
    )


def deglob_path(x: Path) -> Path:
    return Path(re.sub(r"\*.*$", "", str(x)))


class DontUpdate:
    pass


TriOptional = Union[T, None, DontUpdate]


def sha256_file(p: Path) -> str:
    with p.open("rb") as f:
        return hashlib.sha256(f.read()).hexdigest()


def sha256_file_bytes(p: Path) -> bytes:
    with p.open("rb") as f:
        return hashlib.sha256(f.read()).digest()


def sha256_combination(hashes: Iterable[bytes]) -> str:
    return hashlib.sha256(b"".join(hashes)).hexdigest()


def sha256_files(ps: Iterable[Path]) -> str:
    return sha256_combination(sha256_file_bytes(p) for p in ps)


def read_file_to_string(p: Path) -> str:
    with p.open("r") as f:
        return f.read()


# see https://stackoverflow.com/questions/79797/how-to-convert-local-time-string-to-utc
def local_time_to_utc(
    d: datetime.datetime, current_time_zone: Optional[str] = None
) -> datetime.datetime:
    tzname = (
        current_time_zone
        if current_time_zone
        else datetime.datetime.now().astimezone().tzname()
    )
    if tzname is None:
        raise Exception(
            "couldn't figure out the current system time zone, and none was given"
        )

    local = pytz.timezone(tzname)
    return local.localize(d).astimezone(pytz.utc)


def last_line_of_file(p: Path) -> str:
    with p.open("r") as f:
        # Be dumb for now, probably use this solution if the need arises:
        # https://stackoverflow.com/questions/3346430/what-is-the-most-efficient-way-to-get-first-and-last-line-of-a-text-file/3346788
        lines = f.readlines()
        if lines:
            return lines[-1]
        return ""


def safe_max(xs: Iterable[T], key: Callable[[T], U]) -> Optional[T]:
    try:
        # mypy wants Callable[[T], Union[SupportsDunderLT, SupportsDunderGT]] but that's internal
        return max(xs, key=key)  # type: ignore
    except ValueError:
        return None
