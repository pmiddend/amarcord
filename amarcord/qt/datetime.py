import datetime

from PyQt5.QtCore import QDateTime
from PyQt5.QtCore import Qt


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
