import datetime
from typing import List, Optional, Union

from PyQt5.QtCore import QDateTime, Qt
from lark import Lark, Token, Tree
import lark.exceptions as le


def qt_from_isoformat(s: str) -> QDateTime:
    return QDateTime.fromString(s, Qt.ISODate)


def qt_to_isoformat(q: QDateTime) -> str:
    return q.toString(Qt.ISODate)


def to_qt_datetime(data: datetime.datetime) -> QDateTime:
    return QDateTime.fromString(
        data.strftime("%Y-%m-%d %H:%M:%S"), "yyyy-MM-dd HH:mm:ss"
    )


def to_iso_8601_datetime(data: datetime.datetime) -> str:
    return data.isoformat()


def from_qt_datetime(data: QDateTime) -> datetime.datetime:
    return data.toPyDateTime()


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
