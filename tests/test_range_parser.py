from amarcord.numeric_range import NumericRange
from amarcord.qt.numeric_range_format_widget import parse_range


def test_simple_parse() -> None:
    result = parse_range("(oo,4]")
    assert isinstance(result, NumericRange)
