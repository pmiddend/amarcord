from typing import Any

import pytest

from amarcord.db.attributi import coparse_schema_type
from amarcord.db.attributi import parse_schema_type
from amarcord.json_schema import JSONSchemaArray
from amarcord.json_schema import JSONSchemaArraySubtype
from amarcord.json_schema import JSONSchemaBoolean
from amarcord.json_schema import JSONSchemaInteger
from amarcord.json_schema import JSONSchemaNumber
from amarcord.json_schema import JSONSchemaString
from amarcord.json_schema import JSONSchemaUnion


@pytest.mark.parametrize(
    "input_json,expected",
    [
        (
            {"type": "number"},
            JSONSchemaNumber(
                type="number",
                minimum=None,
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix=None,
                format=None,
                tolerance=None,
                toleranceIsAbsolute=False,
            ),
        ),
        (
            {"type": "number", "suffix": "suffix", "format": "standard-unit"},
            JSONSchemaNumber(
                type="number",
                minimum=None,
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix="suffix",
                format="standard-unit",
                tolerance=None,
                toleranceIsAbsolute=False,
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
                type="number",
                minimum=None,
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix="suffix",
                format="standard-unit",
                tolerance=0.5,
                toleranceIsAbsolute=False,
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
                type="number",
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix="suffix",
                format="standard-unit",
                tolerance=0.5,
                toleranceIsAbsolute=True,
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
                type="number",
                minimum=None,
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix="suffix",
                format="standard-unit",
                tolerance=3,
                toleranceIsAbsolute=True,
            ),
        ),
        ({"type": "string"}, JSONSchemaString(type="string", enum=None)),
        (
            {"type": "array", "item_type": "bool", "minItems": 10},
            JSONSchemaArray(
                type="array",
                minItems=10,
                maxItems=None,
                item_type=JSONSchemaArraySubtype.ARRAY_BOOL,
            ),
        ),
        ({"type": "boolean"}, JSONSchemaBoolean(type="boolean")),
        ({"type": "integer"}, JSONSchemaInteger(type="integer", format=None)),
        (
            {"type": "integer", "format": "chemical-id"},
            JSONSchemaInteger(type="integer", format="chemical-id"),
        ),
        (
            {"type": "integer", "format": "date-time"},
            JSONSchemaInteger(type="integer", format="date-time"),
        ),
    ],
)
def test_parse_schema_type(
    input_json: dict[str, Any], expected: JSONSchemaUnion
) -> None:
    assert parse_schema_type(input_json) == expected


@pytest.mark.parametrize(
    "expected,input_schema",
    [
        (
            {
                "type": "number",
                "maximum": None,
                "minimum": None,
                "exclusiveMaximum": None,
                "exclusiveMinimum": None,
                "format": None,
                "suffix": None,
                "tolerance": None,
                "toleranceIsAbsolute": False,
            },
            JSONSchemaNumber(
                type="number",
                minimum=None,
                maximum=None,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix=None,
                format=None,
                tolerance=None,
                toleranceIsAbsolute=False,
            ),
        ),
        (
            {
                "type": "number",
                "minimum": 1.0,
                "maximum": 3.0,
                "exclusiveMaximum": None,
                "exclusiveMinimum": None,
                "format": None,
                "suffix": None,
                "tolerance": None,
                "toleranceIsAbsolute": False,
            },
            JSONSchemaNumber(
                type="number",
                minimum=1.0,
                maximum=3.0,
                exclusiveMaximum=None,
                exclusiveMinimum=None,
                suffix=None,
                format=None,
                tolerance=None,
                toleranceIsAbsolute=False,
            ),
        ),
        (
            {
                "type": "number",
                "exclusiveMinimum": 1.0,
                "exclusiveMaximum": 3.0,
                "maximum": None,
                "minimum": None,
                "format": None,
                "suffix": None,
                "tolerance": None,
                "toleranceIsAbsolute": False,
            },
            JSONSchemaNumber(
                type="number",
                minimum=None,
                maximum=None,
                exclusiveMinimum=1.0,
                exclusiveMaximum=3.0,
                suffix=None,
                format=None,
                tolerance=None,
                toleranceIsAbsolute=False,
            ),
        ),
        (
            {
                "type": "array",
                "minItems": 1,
                "maxItems": 3,
                "item_type": "string",
            },
            JSONSchemaArray(
                type="array",
                minItems=1,
                maxItems=3,
                item_type=JSONSchemaArraySubtype.ARRAY_STRING,
            ),
        ),
    ],
)
def test_coparse_schema_type(
    expected: dict[str, Any], input_schema: JSONSchemaUnion
) -> None:
    assert coparse_schema_type(input_schema) == expected
