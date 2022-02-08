from dataclasses import dataclass
from enum import Enum
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Union


@dataclass(frozen=True)
class JSONSchemaInteger:
    format: Optional[str]


@dataclass(frozen=True)
class JSONSchemaBoolean:
    pass


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


@dataclass(frozen=True)
class JSONSchemaString:
    enum_: Optional[List[str]]


@dataclass(frozen=True)
class JSONSchemaArray:
    value_type: "JSONSchemaType"
    min_items: Optional[int]
    max_items: Optional[int]


JSONSchemaType = Union[
    JSONSchemaInteger,
    JSONSchemaNumber,
    JSONSchemaString,
    JSONSchemaArray,
    JSONSchemaBoolean,
]


def parse_schema_type(s: Dict[str, Any]) -> JSONSchemaType:
    type_ = s.get("type", None)
    if type_ is None:
        raise Exception("json schema has no type attribute")
    if type_ == "integer":
        format_ = s.get("format", None)
        assert format_ is None or isinstance(format_, str)
        return JSONSchemaInteger(format=format_)

    if type_ == "boolean":
        return JSONSchemaBoolean()

    if type_ == "number":
        format_ = s.get("format", None)
        return JSONSchemaNumber(
            minimum=s.get("minimum", None),
            maximum=s.get("maximum", None),
            exclusiveMinimum=s.get("exclusiveMinimum", None),
            exclusiveMaximum=s.get("exclusiveMaximum", None),
            suffix=s.get("suffix", None),
            format_=JSONSchemaNumberFormat.STANDARD_UNIT
            if format_ == JSONSchemaNumberFormat.STANDARD_UNIT.value
            else None,
        )

    if type_ == "string":
        enum_ = s.get("enum", None)
        assert enum_ is None or isinstance(
            enum_, list
        ), f"enum has wrong type {type(enum_)}"
        return JSONSchemaString(enum_=enum_)

    if type_ == "array":
        items = s.get("items", None)
        assert items is not None, 'array without "items" property'
        assert isinstance(items, dict), f"array items type is {type(items)}"
        return JSONSchemaArray(
            parse_schema_type(items), s.get("minItems", None), s.get("maxItems", None)
        )

    raise Exception(f'invalid schema type "{type_}"')
