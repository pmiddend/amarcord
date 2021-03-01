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


JSONSchemaType = Union[JSONSchemaInteger, JSONSchemaNumber, JSONSchemaString]


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

    raise Exception(f'invalid schema type "{type_}"')
