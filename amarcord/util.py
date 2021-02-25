from typing import Dict, Optional, Sequence
from typing import List
from typing import Iterable
from typing import TypeVar
import re

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
