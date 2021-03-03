from amarcord.query_parser import (
    SemanticError,
    parse_query,
    filter_by_query,
    FieldNameError,
    UnexpectedEOF,
)


def test_single_less_than() -> None:
    q = parse_query("foo < 3", {"foo"})
    assert not filter_by_query(q, {"foo": 4})
    assert filter_by_query(q, {"foo": 2})


def test_single_greater_than() -> None:
    q = parse_query("foo > 3", {"foo"})
    assert filter_by_query(q, {"foo": 4})
    assert not filter_by_query(q, {"foo": 2})


def test_single_less_equal() -> None:
    q = parse_query("foo <= 3", {"foo"})
    assert filter_by_query(q, {"foo": 3})
    assert not filter_by_query(q, {"foo": 4})


def test_single_greater_equal() -> None:
    q = parse_query("foo >= 3", {"foo"})
    assert filter_by_query(q, {"foo": 3})
    assert not filter_by_query(q, {"foo": 2})


def test_single_not_equal() -> None:
    q = parse_query("foo != 3", {"foo"})
    assert filter_by_query(q, {"foo": 4})
    assert not filter_by_query(q, {"foo": 3})


def test_single_equal() -> None:
    q = parse_query("foo = 3", {"foo"})
    assert filter_by_query(q, {"foo": 3})
    assert not filter_by_query(q, {"foo": 4})


def test_single_has() -> None:
    q = parse_query("foo has value", {"foo"})
    assert filter_by_query(q, {"foo": "myvalue"})
    assert not filter_by_query(q, {"foo": "foo"})


def test_single_not_equal_with_prefix() -> None:
    q = parse_query("not foo = 3", {"foo"})
    assert filter_by_query(q, {"foo": 4})
    assert not filter_by_query(q, {"foo": 3})


def test_and() -> None:
    q = parse_query("foo = 3 and bar = 4", {"foo", "bar"})
    assert filter_by_query(q, {"foo": 3, "bar": 4})
    assert not filter_by_query(q, {"foo": 4, "bar": 3})


def test_or() -> None:
    q = parse_query("foo = 3 or bar = 4", {"foo", "bar"})
    assert filter_by_query(q, {"foo": 3, "bar": 5})
    assert not filter_by_query(q, {"foo": 12, "bar": 13})


def test_empty_is_true() -> None:
    q = parse_query("", {"foo", "bar"})
    assert filter_by_query(q, {"foo": 3, "bar": 5})
    assert filter_by_query(q, {"foo": 12, "bar": 13})


def test_invalid_field_error_is_simple() -> None:
    try:
        parse_query("bar < 5", {"foo"})
    except Exception as e:
        assert isinstance(e, FieldNameError)


def test_unexpected_eof_is_expicit() -> None:
    try:
        parse_query("bar <", {"foo"})
    except Exception as e:
        assert isinstance(e, UnexpectedEOF)


def test_single_negative_number() -> None:
    q = parse_query("foo <= -3", {"foo"})
    assert filter_by_query(q, {"foo": -10})


def test_float_comparison() -> None:
    q = parse_query("foo <= 3", {"foo"})
    assert filter_by_query(q, {"foo": 2.5})


def test_smaller_than_for_lists() -> None:
    q = parse_query("foo < 3", {"foo"})
    try:
        filter_by_query(q, {"foo": ["a", "b"]})
    except Exception as e:
        assert isinstance(e, SemanticError)


def test_invalid_type() -> None:
    q = parse_query("foo < 3", {"foo"})
    try:
        filter_by_query(q, {"foo": {"x": "u"}})
    except Exception as e:
        assert isinstance(e, SemanticError)
