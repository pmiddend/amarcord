import datetime

from amarcord.db.db import DB
from amarcord.db.event_log_level import EventLogLevel

MY_ATTRIBUTO = "my_attributo"


def test_add_retrieve_event(db: DB) -> None:
    with db.connect() as conn:
        created = datetime.datetime(1987, 8, 21, 0, 0, 0, 0)
        db.add_event(conn, EventLogLevel.INFO, "mysource", "sample text", created)

        events = db.retrieve_events(conn)

        assert len(events) == 1
        event = events[0]

        assert event.level == EventLogLevel.INFO
        assert event.source == "mysource"
        assert event.text == "sample text"
        assert event.created == created


def test_add_retrieve_event_with_since_time(db: DB) -> None:
    with db.connect() as conn:
        created = datetime.datetime(1987, 8, 21, 0, 0, 0, 0)
        db.add_event(conn, EventLogLevel.INFO, "mysource", "sample text", created)

        events = db.retrieve_events(
            conn, since=datetime.datetime(1986, 8, 21, 0, 0, 0, 0)
        )

        assert len(events) == 1

        events = db.retrieve_events(
            conn, since=datetime.datetime(1988, 8, 21, 0, 0, 0, 0)
        )

        assert not events


def test_add_retrieve_event_with_since_id(db: DB) -> None:
    with db.connect() as conn:
        created = datetime.datetime(1987, 8, 21, 0, 0, 0, 0)
        db.add_event(conn, EventLogLevel.INFO, "mysource", "sample text", created)
        db.add_event(
            conn,
            EventLogLevel.INFO,
            "mysource",
            "sample text 2",
            created + datetime.timedelta(minutes=1),
        )

        event_id = db.retrieve_events(conn, since=None)[0].id

        events = db.retrieve_events(conn, since=event_id)

        assert len(events) == 1
        assert events[0].text == "sample text 2"
