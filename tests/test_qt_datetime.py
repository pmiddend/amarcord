import datetime
from PyQt5 import QtCore
from amarcord.qt.datetime import from_qt_datetime, to_qt_datetime


def test_from_python() -> None:
    now = datetime.datetime.utcnow()
    now_qt = to_qt_datetime(now)

    assert now.year == now_qt.date().year()
    assert now.month == now_qt.date().month()
    assert now.day == now_qt.date().day()
    assert now.hour == now_qt.time().hour()
    assert now.minute == now_qt.time().minute()
    assert now.second == now_qt.time().second()


def test_to_python() -> None:
    now_qt = QtCore.QDateTime(QtCore.QDate(1987, 8, 21), QtCore.QTime(13, 37))
    now = from_qt_datetime(now_qt)

    assert now.year == now_qt.date().year()
    assert now.month == now_qt.date().month()
    assert now.day == now_qt.date().day()
    assert now.hour == now_qt.time().hour()
    assert now.minute == now_qt.time().minute()
    assert now.second == now_qt.time().second()
