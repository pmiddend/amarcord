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

from lark import Token
from lark import Tree
from lark import exceptions as le
from lark.lark import Lark

T = TypeVar("T")

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


def word_under_cursor(s: str, pos: int) -> str:
    eow = r"[><!& /]"
    before_ws = rfind_regex(s, eow, max(0, pos - 1))
    after_ws = find_regex(s, eow, pos)
    if after_ws < 0:
        after_ws = len(s)
    return s[before_ws + 1 : after_ws]


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


def capitalized_decamelized(s: str) -> str:
    return s.replace("_", " ").capitalize()


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


def print_natural_delta(td: datetime.timedelta) -> str:
    def maybe_pluralize(t: Union[float, int], s: str) -> str:
        return str(t) + " " + s + ("s" if t > 1 else "")

    hours = td.seconds // (60 * 60)
    minutes = (td.seconds % (60 * 60)) // 60
    seconds = td.seconds % 60
    milliseconds = td.microseconds // 1000
    parts: List[str] = []
    if td.days > 0:
        parts.append(maybe_pluralize(td.days, "day"))
    if hours > 0:
        parts.append(maybe_pluralize(hours, "hour"))
    if minutes > 0:
        parts.append(maybe_pluralize(minutes, "minute"))
    if seconds > 0:
        parts.append(maybe_pluralize(seconds, "second"))
    if milliseconds > 0:
        parts.append(maybe_pluralize(milliseconds, "millisecond"))
    return ", ".join(parts)


natural_delta_parser = Lark(
    r"""
start: atom ("," atom)*
atom: NUMBER delta_keyword
delta_keyword: DAYS | HOURS | MINUTES | MILLISECONDS | SECONDS

DAYS : /days?/
HOURS : /hours?/
MINUTES : /minutes?/
MILLISECONDS : /milliseconds?/
SECONDS : /seconds?/

%import common.NUMBER    -> NUMBER
%ignore " "
  """
)


class UnexpectedEOF(Exception):
    def __init__(self) -> None:
        super().__init__("Unexpected EOF")


def parse_natural_delta(s: str) -> Union[None, str, datetime.timedelta]:
    try:
        if s == "":
            return datetime.timedelta()
        atoms = natural_delta_parser.parse(s).children
        days: Optional[int] = None
        hours: Optional[int] = None
        minutes: Optional[int] = None
        seconds: Optional[int] = None
        milliseconds: Optional[int] = None
        for atom_pair in atoms:
            assert isinstance(atom_pair, Tree)
            atom_value = atom_pair.children[0]
            keyword = atom_pair.children[1]
            assert isinstance(atom_value, Token)
            assert isinstance(keyword, Tree)
            assert isinstance(keyword.children[0], Token)

            if keyword.children[0].type == "DAYS":
                if days is not None:
                    return None
                days = int(atom_value.value)
            elif keyword.children[0].type == "HOURS":
                if hours is not None:
                    return None
                hours = int(atom_value.value)
            elif keyword.children[0].type == "MINUTES":
                if minutes is not None:
                    return None
                minutes = int(atom_value.value)
            elif keyword.children[0].type == "SECONDS":
                if seconds is not None:
                    return None
                seconds = int(atom_value.value)
            elif keyword.children[0].type == "MILLISECONDS":
                if milliseconds is not None:
                    return None
                milliseconds = int(atom_value.value)
            else:
                return None
        return datetime.timedelta(
            days=days if days is not None else 0,
            hours=hours if hours is not None else 0,
            minutes=minutes if minutes is not None else 0,
            seconds=seconds if seconds is not None else 0,
            milliseconds=milliseconds if milliseconds is not None else 0,
        )
    except le.UnexpectedEOF:
        return s
    except Exception:
        return None


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


def sha256_files(ps: Iterable[Path]) -> str:
    return hashlib.sha256(b"".join(sha256_file_bytes(p) for p in ps)).hexdigest()


def read_file_to_string(p: Path) -> str:
    with p.open("r") as f:
        return f.read()
