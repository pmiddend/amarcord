import datetime
from PyQt5 import QtCore


def to_qt_datetime(data: datetime.datetime) -> QtCore.QDateTime:
    return QtCore.QDateTime.fromString(
        data.strftime("%Y-%m-%d %H:%M:%S"), "yyyy-MM-dd HH:mm:ss"
    )


def from_qt_datetime(data: QtCore.QDateTime) -> datetime.datetime:
    return data.toPyDateTime()
