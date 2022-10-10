import pytest

from amarcord.json_schema import JSONSchemaArray
from amarcord.json_schema import JSONSchemaBoolean
from amarcord.json_schema import JSONSchemaCustomIntegerFormat
from amarcord.json_schema import JSONSchemaInteger
from amarcord.json_schema import JSONSchemaIntegerFormat
from amarcord.json_schema import JSONSchemaNumber
from amarcord.json_schema import JSONSchemaNumberFormat
from amarcord.json_schema import JSONSchemaString
from amarcord.json_schema import JSONSchemaType
from amarcord.json_schema import coparse_schema_type
from amarcord.json_schema import parse_schema_type
from amarcord.json_types import JSONDict


def test_parse_schema_type_no_type() -> None:
    with pytest.raises(Exception):
        parse_schema_type({"type_": "number"})


@pytest.mark.parametrize(
    "input_json,expected",
    [
        (
            {"type": "number"},
            JSONSchemaNumber(
                minimum=None,
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix=None,
                format_=None,
                tolerance=None,
                tolerance_is_absolute=False,
            ),
        ),
        (
            {"type": "number", "suffix": "suffix", "format": "standard-unit"},
            JSONSchemaNumber(
                minimum=None,
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix="suffix",
                format_=JSONSchemaNumberFormat.STANDARD_UNIT,
                tolerance=None,
                tolerance_is_absolute=False,
            ),
        ),
        (
            {
                "type": "number",
                "suffix": "suffix",
                "format": "standard-unit",
                "tolerance": 0.5,
            },
            JSONSchemaNumber(
                minimum=None,
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix="suffix",
                format_=JSONSchemaNumberFormat.STANDARD_UNIT,
                tolerance=0.5,
                tolerance_is_absolute=False,
            ),
        ),
        (
            {
                "type": "number",
                "suffix": "suffix",
                "format": "standard-unit",
                "tolerance": 0.5,
                "toleranceIsAbsolute": True,
            },
            JSONSchemaNumber(
                minimum=None,
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix="suffix",
                format_=JSONSchemaNumberFormat.STANDARD_UNIT,
                tolerance=0.5,
                tolerance_is_absolute=True,
            ),
        ),
        (
            {
                "type": "number",
                "suffix": "suffix",
                "format": "standard-unit",
                "tolerance": 3,
                "toleranceIsAbsolute": True,
            },
            JSONSchemaNumber(
                minimum=None,
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix="suffix",
                format_=JSONSchemaNumberFormat.STANDARD_UNIT,
                tolerance=3,
                tolerance_is_absolute=True,
            ),
        ),
        ({"type": "string"}, JSONSchemaString(enum_=None)),
        (
            {"type": "array", "items": {"type": "integer"}, "minItems": 10},
            JSONSchemaArray(
                value_type=JSONSchemaInteger(format_=None), min_items=10, max_items=None
            ),
        ),
        ({"type": "boolean"}, JSONSchemaBoolean()),
        ({"type": "integer"}, JSONSchemaInteger(format_=None)),
        (
            {"type": "integer", "format": "chemical-id"},
            JSONSchemaInteger(format_=JSONSchemaCustomIntegerFormat("chemical-id")),
        ),
        (
            {"type": "integer", "format": "date-time"},
            JSONSchemaInteger(format_=JSONSchemaIntegerFormat.DATE_TIME),
        ),
    ],
)
def test_parse_schema_type(input_json: JSONDict, expected: JSONSchemaType) -> None:
    assert parse_schema_type(input_json) == expected


@pytest.mark.parametrize(
    "expected,input_schema",
    [
        (
            {"type": "number"},
            JSONSchemaNumber(
                minimum=None,
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix=None,
                format_=None,
                tolerance=None,
                tolerance_is_absolute=False,
            ),
        ),
        (
            {"type": "number", "minimum": 1.0, "maximum": 3.0},
            JSONSchemaNumber(
                minimum=1.0,
                maximum=3.0,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix=None,
                format_=None,
                tolerance=None,
                tolerance_is_absolute=False,
            ),
        ),
        (
            {"type": "number", "exclusiveMinimum": 1.0, "exclusiveMaximum": 3.0},
            JSONSchemaNumber(
                minimum=None,
                maximum=None,
                exclusiveMinimum=1.0,
                exclusiveMaximum=3.0,
                suffix=None,
                format_=None,
                tolerance=None,
                tolerance_is_absolute=False,
            ),
        ),
        (
            {
                "type": "array",
                "minItems": 1,
                "maxItems": 3,
                "items": {"type": "string"},
            },
            JSONSchemaArray(
                min_items=1, max_items=3, value_type=JSONSchemaString(enum_=None)
            ),
        ),
    ],
)
def test_coparse_schema_type(expected: JSONDict, input_schema: JSONSchemaType) -> None:
    assert coparse_schema_type(input_schema) == expected
