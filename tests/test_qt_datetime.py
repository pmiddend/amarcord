import datetime
from PyQt5 import QtCore
from amarcord.qt.datetime import (
    from_qt_datetime,
    parse_natural_delta,
    print_natural_delta,
    to_qt_datetime,
)


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


def test_parse_natural_timedelta() -> None:
    result = parse_natural_delta("1 day, 4 hours, 5 minutes")
    assert isinstance(result, datetime.timedelta)
    assert result.total_seconds() == 1 * 86400 + 4 * 60 * 60 + 5 * 60


def test_to_natural_timedelta() -> None:
    input_delta = "2 days, 4 hours, 5 minutes"
    assert print_natural_delta(parse_natural_delta(input_delta)) == input_delta
