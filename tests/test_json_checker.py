from pathlib import Path

import pytest

from amarcord.json_checker import JSONChecker


def test_retrieve_optional_str() -> None:
    assert JSONChecker({"foo": "bar"}, "description").optional_str("foo") == "bar"
    assert JSONChecker({"foo2": "bar"}, "description").optional_str("foo") is None
    assert JSONChecker({}, "description").optional_str("foo") is None

    with pytest.raises(Exception):
        JSONChecker({"foo": 3}, "description").optional_str("foo")


def test_retrieve_optional_path() -> None:
    assert JSONChecker({"foo": "/bar"}, "description").optional_path("foo") == Path(
        "/bar"
    )
    assert JSONChecker({"foo2": "bar"}, "description").optional_path("foo") is None
    assert JSONChecker({}, "description").optional_path("foo") is None

    with pytest.raises(Exception):
        JSONChecker({"foo": 3}, "description").optional_path("foo")


def test_retrieve_optional_float() -> None:
    assert JSONChecker({"foo": 3.0}, "description").optional_float("foo") == 3.0
    assert JSONChecker({"foo": 3}, "description").optional_float("foo") == 3
    assert JSONChecker({"foo2": "bar"}, "description").optional_float("foo") is None
    assert JSONChecker({}, "description").optional_float("foo") is None

    with pytest.raises(Exception):
        JSONChecker({"foo": "bar"}, "description").optional_float("foo")


def test_retrieve_optional_int() -> None:
    assert JSONChecker({"foo": 3}, "description").optional_int("foo") == 3
    assert JSONChecker({"foo2": "bar"}, "description").optional_int("foo") is None
    assert JSONChecker({}, "description").optional_int("foo") is None

    with pytest.raises(Exception):
        JSONChecker({"foo": "bar"}, "description").optional_int("foo")


def test_safe_path() -> None:
    assert JSONChecker({"foo": "/bar"}, "description").safe_path("foo") == Path("/bar")

    with pytest.raises(Exception):
        JSONChecker({"foo2": "/bar"}, "description").safe_path("foo")


def test_retrieve_safe() -> None:
    assert JSONChecker({"foo": "/bar"}, "description").retrieve_safe("foo") is not None
    assert JSONChecker({"foo": 3}, "description").retrieve_safe("foo") is not None

    with pytest.raises(Exception):
        JSONChecker({"foo2": "/bar"}, "description").retrieve_safe("foo")


def test_retrieve_safe_float() -> None:
    assert JSONChecker({"foo": 3.0}, "description").retrieve_safe("foo") == 3.0

    with pytest.raises(Exception):
        JSONChecker({"foo2": "/bar"}, "description").retrieve_safe_float("foo")
