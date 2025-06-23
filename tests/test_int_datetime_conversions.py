import os

from hypothesis import given
from hypothesis import strategies as st

from amarcord.db.attributi import local_int_to_utc_datetime
from amarcord.db.attributi import utc_datetime_to_local_int


# The max_value argument is 200 years after 1970
@given(st.integers(min_value=0, max_value=6307200000))
def test_local_int_to_utc_datetime(timestamp: int) -> None:
    # Whatever the local time in the testing server is
    assert utc_datetime_to_local_int(local_int_to_utc_datetime(timestamp)) == timestamp

    os.environ["AMARCORD_TZ"] = "UTC"
    assert utc_datetime_to_local_int(local_int_to_utc_datetime(timestamp)) == timestamp


# Special case, where rounding comes into play
def test_local_int_to_utc_datetime_65323() -> None:
    os.environ["AMARCORD_TZ"] = "UTC"
    assert utc_datetime_to_local_int(local_int_to_utc_datetime(65323)) == 65323
