import datetime


class Clock:
    # pylint: disable=no-self-use
    def now(self) -> datetime.datetime:
        ...


class RealClock(Clock):
    # pylint: disable=no-self-use
    def now(self) -> datetime.datetime:
        return datetime.datetime.utcnow()


class MockClock(Clock):
    def __init__(self, now: datetime.datetime) -> None:
        super().__init__()
        self._now = now

    def now(self) -> datetime.datetime:
        return self._now

    def advance_minutes(self, minutes: int) -> None:
        self._now += datetime.timedelta(minutes=minutes)
