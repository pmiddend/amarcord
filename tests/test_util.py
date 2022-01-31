from amarcord.util import remove_duplicates_stable, dict_union


def test_remove_duplicates_stable():
    assert remove_duplicates_stable([4, 3, 4, 5, 6, 6]) == [4, 3, 5, 6]


def test_dict_union():
    assert dict_union([{"a": 1}, {"b": 2}, {"a": 3}]) == {"a": 3, "b": 2}
