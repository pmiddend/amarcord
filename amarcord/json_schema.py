from dataclasses import dataclass
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


@dataclass(frozen=True)
class JSONSchemaString:
    enum_: Optional[List[str]]


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
        return JSONSchemaArray(parse_schema_type(items))

    raise Exception(f'invalid schema type "{type_}"')
