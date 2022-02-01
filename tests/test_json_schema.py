import pytest

from amarcord.json_schema import (
    parse_schema_type,
    JSONSchemaInteger,
    JSONSchemaBoolean,
    JSONSchemaNumber,
    JSONSchemaNumberFormat,
    JSONSchemaString,
    JSONSchemaStringFormat,
    JSONSchemaArray,
)


def test_parse_schema_type_no_type() -> None:
    with pytest.raises(Exception):
        parse_schema_type({"type_": "number"})


def test_parse_schema_type_integer() -> None:
    assert parse_schema_type({"type": "integer"}) == JSONSchemaInteger(format=None)
    assert parse_schema_type(
        {"type": "integer", "format": "sample-id"}
    ) == JSONSchemaInteger(format="sample-id")


def test_parse_schema_type_boolean() -> None:
    assert parse_schema_type({"type": "boolean"}) == JSONSchemaBoolean()


def test_parse_schema_type_number() -> None:
    assert parse_schema_type({"type": "number"}) == JSONSchemaNumber(
        minimum=None,
        maximum=None,
        exclusiveMaximum=None,
        exclusiveMinimum=None,
        suffix=None,
        format_=None,
    )

    assert parse_schema_type(
        {"type": "number", "suffix": "suffix", "format": "standard-unit"}
    ) == JSONSchemaNumber(
        minimum=None,
        maximum=None,
        exclusiveMaximum=None,
        exclusiveMinimum=None,
        suffix="suffix",
        format_=JSONSchemaNumberFormat.STANDARD_UNIT,
    )


def test_parse_schema_type_string() -> None:
    assert parse_schema_type({"type": "string"}) == JSONSchemaString(
        enum_=None, format_=None
    )
    assert parse_schema_type(
        {"type": "string", "format": "date-time"}
    ) == JSONSchemaString(enum_=None, format_=JSONSchemaStringFormat.DATE_TIME)


def test_parse_schema_type_array() -> None:
    assert parse_schema_type(
        {"type": "array", "items": {"type": "integer"}, "minItems": 10}
    ) == JSONSchemaArray(
        value_type=JSONSchemaInteger(format=None), min_items=10, max_items=None
    )
