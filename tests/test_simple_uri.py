import pytest

from amarcord.simple_uri import SimpleURI
from amarcord.simple_uri import parse_simple_uri


@pytest.mark.parametrize(
    "input_type_and_value",
    [
        ("scheme:", SimpleURI("scheme", {})),
        ("scheme", None),
        ("scheme:key=value", SimpleURI("scheme", {"key": "value"})),
        ("scheme:key=", SimpleURI("scheme", {"key": ""})),
        ("scheme:key=value|key=value", None),
        (
            "scheme:key=value|key2=value",
            SimpleURI("scheme", {"key": "value", "key2": "value"}),
        ),
        ("scheme:=value", None),
        ("scheme:keyvalue", None),
    ],
)
def test_parse_simple_uri(input_type_and_value: tuple[str, None | SimpleURI]) -> None:
    result = parse_simple_uri(input_type_and_value[0])
    assert (
        isinstance(result, str)
        and input_type_and_value[1] is None
        or not isinstance(result, str)
        and result == input_type_and_value[1]
    )
