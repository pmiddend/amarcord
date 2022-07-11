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


# This attempt at a test was unsuccessful, but with some more work we might
# have an axiom for intervals. I'm keeping this here for now (mainly for the strategy below and its usage).

# @st.composite
# def list_and_index(draw, elements=st.integers()):
#     xs = draw(st.lists(elements, min_size=2, unique_by=lambda x: x))
#     i = draw(st.integers(min_value=0, max_value=len(xs) - 1))
#     return xs, i
#
#
# @given(list_and_index())
# def test_removing_element_increases_count(xs_and_idx: Tuple[list[int], int]) -> None:
#     xs, idx = xs_and_idx
#     original_intervals = list(create_intervals(xs))
#     new_xs = xs.copy()
#     new_xs.pop(idx)
#     new_intervals = list(create_intervals(new_xs))
#     assert len(original_intervals) <= len(new_intervals)
