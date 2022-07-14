from dataclasses import dataclass
from enum import Enum
from typing import Any, Final

from amarcord.json_types import JSONDict, JSONValue

_JSON_SCHEMA_TYPE_BOOLEAN: Final = "boolean"

_JSON_SCHEMA_ARRAY_MAX_ITEMS: Final = "maxItems"

_JSON_SCHEMA_ARRAY_MIN_ITEMS: Final = "minItems"

_JSON_SCHEMA_ARRAY_ITEMS: Final = "items"

_JSON_SCHEMA_TYPE_ARRAY: Final = "array"

_JSON_SCHEMA_STRING_ENUM: Final = "enum"

_JSON_SCHEMA_TYPE_STRING: Final = "string"

_JSON_SCHEMA_NUMBER_SUFFIX: Final = "suffix"

_JSON_SCHEMA_NUMBER_TOLERANCE_IS_ABSOLUTE: Final = "toleranceIsAbsolute"

_JSON_SCHEMA_NUMBER_TOLERANCE: Final = "tolerance"

_JSON_SCHEMA_NUMBER_EXCLUSIVE_MAXIMUM: Final = "exclusiveMaximum"

_JSON_SCHEMA_NUMBER_EXCLUSIVE_MINIMUM: Final = "exclusiveMinimum"

_JSON_SCHEMA_NUMBER_MAXIMUM: Final = "maximum"

_JSON_SCHEMA_NUMBER_MINIMUM: Final = "minimum"

_JSON_SCHEMA_TYPE_NUMBER: Final = "number"

_JSON_SCHEMA_FORMAT: Final = "format"

_JSON_SCHEMA_TYPE_INTEGER: Final = "integer"

_JSON_SCHEMA_TYPE: Final = "type"


class JSONSchemaIntegerFormat(Enum):
    DATE_TIME = "date-time"


@dataclass(frozen=True, eq=True)
class JSONSchemaCustomIntegerFormat:
    format_: str


@dataclass(frozen=True, eq=True)
class JSONSchemaInteger:
    format_: JSONSchemaIntegerFormat | JSONSchemaCustomIntegerFormat | None


@dataclass(frozen=True, eq=True)
class JSONSchemaBoolean:
    pass


class JSONSchemaNumberFormat(Enum):
    STANDARD_UNIT = "standard-unit"


@dataclass(frozen=True, eq=True)
class JSONSchemaNumber:
    minimum: float | None
    maximum: float | None
    exclusiveMinimum: float | None
    exclusiveMaximum: float | None
    suffix: str | None
    format_: JSONSchemaNumberFormat | None
    tolerance: float | None
    tolerance_is_absolute: bool


@dataclass(frozen=True, eq=True)
class JSONSchemaString:
    enum_: list[str] | None


@dataclass(frozen=True, eq=True)
class JSONSchemaArray:
    value_type: "JSONSchemaType"
    min_items: int | None
    max_items: int | None


JSONSchemaType = (
    JSONSchemaInteger
    | JSONSchemaNumber
    | JSONSchemaString
    | JSONSchemaArray
    | JSONSchemaBoolean
)


def coparse_schema_type(schema: JSONSchemaType) -> JSONDict:
    match schema:
        case JSONSchemaInteger(format_=None):
            return {_JSON_SCHEMA_TYPE: _JSON_SCHEMA_TYPE_INTEGER}
        case JSONSchemaInteger(
            format_=JSONSchemaCustomIntegerFormat(format_=int_format)
        ):
            return {
                _JSON_SCHEMA_TYPE: _JSON_SCHEMA_TYPE_INTEGER,
                _JSON_SCHEMA_FORMAT: int_format,
            }
        case JSONSchemaInteger(format_=JSONSchemaIntegerFormat.DATE_TIME as dt):
            return {
                _JSON_SCHEMA_TYPE: _JSON_SCHEMA_TYPE_INTEGER,
                _JSON_SCHEMA_FORMAT: dt.value,
            }
        case JSONSchemaNumber(
            minimum=minimum,
            maximum=maximum,
            exclusiveMinimum=exclusiveMinimum,
            exclusiveMaximum=exclusiveMaximum,
            suffix=suffix,
            format_=format_,
            tolerance=tolerance,
            tolerance_is_absolute=tolerance_is_absolute,
        ):
            result_double: dict[str, JSONValue] = {
                _JSON_SCHEMA_TYPE: _JSON_SCHEMA_TYPE_NUMBER
            }
            if minimum is not None:
                result_double[_JSON_SCHEMA_NUMBER_MINIMUM] = minimum
            if maximum is not None:
                result_double[_JSON_SCHEMA_NUMBER_MAXIMUM] = maximum
            if exclusiveMinimum is not None:
                result_double[_JSON_SCHEMA_NUMBER_EXCLUSIVE_MINIMUM] = exclusiveMinimum
            if exclusiveMaximum is not None:
                result_double[_JSON_SCHEMA_NUMBER_EXCLUSIVE_MAXIMUM] = exclusiveMaximum
            if tolerance is not None:
                result_double[_JSON_SCHEMA_NUMBER_TOLERANCE] = tolerance
                result_double[
                    _JSON_SCHEMA_NUMBER_TOLERANCE_IS_ABSOLUTE
                ] = tolerance_is_absolute
            if suffix is not None:
                result_double[_JSON_SCHEMA_NUMBER_SUFFIX] = suffix
            if format_ is not None:
                result_double[_JSON_SCHEMA_FORMAT] = format_.value
            return result_double
        # see https://github.com/PyCQA/pylint/issues/5327
        # pylint: disable=used-before-assignment
        case JSONSchemaString(enum_=enum_) if enum_ is not None:
            return {
                _JSON_SCHEMA_TYPE: _JSON_SCHEMA_TYPE_STRING,
                _JSON_SCHEMA_STRING_ENUM: enum_,
            }
        case JSONSchemaString():
            return {_JSON_SCHEMA_TYPE: _JSON_SCHEMA_TYPE_STRING}
        case JSONSchemaArray(value_type, min_items, max_items):
            result_array: dict[str, JSONValue] = {
                _JSON_SCHEMA_TYPE: _JSON_SCHEMA_TYPE_ARRAY,
                _JSON_SCHEMA_ARRAY_ITEMS: coparse_schema_type(value_type),
            }
            if min_items is not None:
                result_array[_JSON_SCHEMA_ARRAY_MIN_ITEMS] = min_items
            if max_items is not None:
                result_array[_JSON_SCHEMA_ARRAY_MAX_ITEMS] = max_items
            return result_array
        case JSONSchemaBoolean():
            return {_JSON_SCHEMA_TYPE: _JSON_SCHEMA_TYPE_BOOLEAN}
    raise Exception(f"invalid schema type {schema}")


def parse_schema_type(s: dict[str, Any]) -> JSONSchemaType:
    type_ = s.get(_JSON_SCHEMA_TYPE, None)
    if type_ is None:
        raise Exception("json schema has no type attribute")
    if type_ == _JSON_SCHEMA_TYPE_INTEGER:
        raw_format_ = s.get(_JSON_SCHEMA_FORMAT, None)
        format_: JSONSchemaIntegerFormat | JSONSchemaCustomIntegerFormat | None
        if raw_format_ is not None:
            try:
                format_ = JSONSchemaIntegerFormat(raw_format_)
            except ValueError:
                # Not sure how this is caused, but the types are definitely correct
                # pylint: disable=redefined-variable-type
                format_ = JSONSchemaCustomIntegerFormat(raw_format_)
        else:
            format_ = None
        return JSONSchemaInteger(format_=format_)

    if type_ == _JSON_SCHEMA_TYPE_BOOLEAN:
        return JSONSchemaBoolean()

    if type_ == _JSON_SCHEMA_TYPE_NUMBER:
        format_ = s.get(_JSON_SCHEMA_FORMAT, None)
        tolerance = s.get(_JSON_SCHEMA_NUMBER_TOLERANCE, None)
        assert tolerance is None or isinstance(
            tolerance, (float, int)
        ), f"{_JSON_SCHEMA_NUMBER_TOLERANCE} is not int/float: {tolerance}"
        tolerance_absolute = s.get(_JSON_SCHEMA_NUMBER_TOLERANCE_IS_ABSOLUTE, False)
        assert tolerance_absolute is None or isinstance(
            tolerance_absolute, bool
        ), f"{_JSON_SCHEMA_NUMBER_TOLERANCE_IS_ABSOLUTE} is not bool: {tolerance_absolute}"
        return JSONSchemaNumber(
            minimum=s.get(_JSON_SCHEMA_NUMBER_MINIMUM, None),
            maximum=s.get(_JSON_SCHEMA_NUMBER_MAXIMUM, None),
            exclusiveMinimum=s.get(_JSON_SCHEMA_NUMBER_EXCLUSIVE_MINIMUM, None),
            exclusiveMaximum=s.get(_JSON_SCHEMA_NUMBER_EXCLUSIVE_MAXIMUM, None),
            suffix=s.get(_JSON_SCHEMA_NUMBER_SUFFIX, None),
            format_=JSONSchemaNumberFormat.STANDARD_UNIT
            if format_ == JSONSchemaNumberFormat.STANDARD_UNIT.value
            else None,
            tolerance=tolerance,
            tolerance_is_absolute=tolerance_absolute
            if tolerance_absolute is not None
            else False,
        )

    if type_ == _JSON_SCHEMA_TYPE_STRING:
        enum_ = s.get(_JSON_SCHEMA_STRING_ENUM, None)
        assert enum_ is None or isinstance(
            enum_, list
        ), f"{_JSON_SCHEMA_STRING_ENUM} has wrong type {type(enum_)}"
        return JSONSchemaString(enum_=enum_)

    if type_ == _JSON_SCHEMA_TYPE_ARRAY:
        items = s.get(_JSON_SCHEMA_ARRAY_ITEMS, None)
        assert items is not None, f'array without "{_JSON_SCHEMA_ARRAY_ITEMS}" property'
        assert isinstance(
            items, dict
        ), f"array {_JSON_SCHEMA_ARRAY_ITEMS} type is {type(items)}"
        return JSONSchemaArray(
            parse_schema_type(items),
            s.get(_JSON_SCHEMA_ARRAY_MIN_ITEMS, None),
            s.get(_JSON_SCHEMA_ARRAY_MAX_ITEMS, None),
        )

    raise Exception(f'invalid schema type "{type_}"')
