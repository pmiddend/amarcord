import datetime
from typing import Callable, Dict, List, Optional, Sequence, Union
from typing import List
from typing import Iterable
from typing import TypeVar
import re

from lark import Token, Tree, exceptions as le
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
