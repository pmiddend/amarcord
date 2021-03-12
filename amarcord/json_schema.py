from dataclasses import dataclass
from enum import Enum
from typing import Any, Dict, List, Optional, Union


@dataclass(frozen=True)
class JSONSchemaInteger:
    minimum: Optional[int]
    maximum: Optional[int]
    exclusiveMinimum: Optional[int]
    exclusiveMaximum: Optional[int]


@dataclass(frozen=True)
class JSONSchemaNumber:
    minimum: Optional[float]
    maximum: Optional[float]
    exclusiveMinimum: Optional[float]
    exclusiveMaximum: Optional[float]
    suffix: Optional[str]


class JSONSchemaStringFormat(Enum):
    DATE_TIME = "date-time"
    DURATION = "duration"


@dataclass(frozen=True)
class JSONSchemaString:
    enum_: Optional[List[str]]
    format_: Optional[JSONSchemaStringFormat]


@dataclass(frozen=True)
class JSONSchemaArray:
    value_type: "JSONSchemaType"


JSONSchemaType = Union[
    JSONSchemaInteger, JSONSchemaNumber, JSONSchemaString, JSONSchemaArray
]


def parse_schema_type(s: Dict[str, Any]) -> JSONSchemaType:
    type_ = s.get("type", None)
    if type_ is None:
        raise Exception("json schema has no type attribute")
    if type_ == "integer":
        return JSONSchemaInteger(
            minimum=s.get("minimum", None),
            maximum=s.get("maximum", None),
            exclusiveMinimum=s.get("exclusiveMinimum", None),
            exclusiveMaximum=s.get("exclusiveMaximum", None),
        )

    if type_ == "number":
        return JSONSchemaNumber(
            minimum=s.get("minimum", None),
            maximum=s.get("maximum", None),
            exclusiveMinimum=s.get("exclusiveMinimum", None),
            exclusiveMaximum=s.get("exclusiveMaximum", None),
            suffix=s.get("suffix", None),
        )

    if type_ == "string":
        enum_ = s.get("enum", None)
        assert enum_ is None or isinstance(
            enum_, list
        ), f"enum has wrong type {type(enum_)}"
        format_ = s.get("format", None)
        return JSONSchemaString(
            enum_=enum_,
            format_=JSONSchemaStringFormat.DATE_TIME
            if format_ == "date-time"
            else JSONSchemaStringFormat.DURATION
            if format_ == "duration"
            else None,
        )

    if type_ == "array":
        items = s.get("items", None)
        assert items is not None, 'array without "items" property'
        assert isinstance(items, dict), f"array items type is {type(items)}"
        return JSONSchemaArray(parse_schema_type(items))

    raise Exception(f'invalid schema type "{type_}"')
