from hypothesis import given
from hypothesis import strategies as st

from amarcord.util import create_intervals


def test_create_no_gaps() -> None:
    assert list(create_intervals([1, 2, 3])) == [(1, 3)]


def test_create_one_gap() -> None:
    assert list(create_intervals([1, 2, 3, 5, 6])) == [(1, 3), (5, 6)]


@given(st.lists(st.integers()))
def test_sorting_doesnt_matter(xs: list[int]) -> None:
    assert list(create_intervals(xs)) == list(create_intervals(sorted(xs)))
