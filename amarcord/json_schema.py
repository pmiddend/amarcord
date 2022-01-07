from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union


@dataclass(frozen=True)
class JSONSchemaInteger:
    minimum: Optional[int]
    maximum: Optional[int]
    exclusiveMinimum: Optional[int]
    exclusiveMaximum: Optional[int]


class JSONSchemaNumberFormat(Enum):
    STANDARD_UNIT = "standard-unit"


@dataclass(frozen=True)
class JSONSchemaNumber:
    minimum: Optional[float]
    maximum: Optional[float]
    exclusiveMinimum: Optional[float]
    exclusiveMaximum: Optional[float]
    suffix: Optional[str]
    format_: Optional[JSONSchemaNumberFormat]


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
    min_items: Optional[int]
    max_items: Optional[int]


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
        format_ = s.get("format", None)
        return JSONSchemaNumber(
            minimum=s.get("minimum", None),
            maximum=s.get("maximum", None),
            exclusiveMinimum=s.get("exclusiveMinimum", None),
            exclusiveMaximum=s.get("exclusiveMaximum", None),
            suffix=s.get("suffix", None),
            format_=JSONSchemaNumberFormat.STANDARD_UNIT
            if format_ == "standard-unit"
            else None,
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
        return JSONSchemaArray(
            parse_schema_type(items), s.get("minItems", None), s.get("maxItems", None)
        )

    raise Exception(f'invalid schema type "{type_}"')
