from enum import Enum
from typing import Literal

from pydantic import BaseModel


class JSONSchemaInteger(BaseModel):
    type: Literal["integer"]
    format: None | Literal["date-time", "chemical-id"] = None


class JSONSchemaNumber(BaseModel):
    type: Literal["number"]
    minimum: float | None = None
    maximum: float | None = None
    # Note: camelCase here, but that's just because it's JSON schema (and I was too lazy to map this properly to a pydantic alias)
    exclusiveMinimum: float | None = None  # noqa: N815
    exclusiveMaximum: float | None = None  # noqa: N815
    suffix: str | None = None
    format: None | Literal["standard-unit"] = None
    tolerance: float | None = None
    toleranceIsAbsolute: bool = False  # noqa: N815


class JSONSchemaString(BaseModel):
    type: Literal["string"]
    enum: list[str] | None = None


class JSONSchemaBoolean(BaseModel):
    type: Literal["boolean"]


class JSONSchemaArraySubtype(str, Enum):
    ARRAY_STRING = "string"
    ARRAY_BOOL = "bool"
    ARRAY_NUMBER = "number"


class JSONSchemaArray(BaseModel):
    type: Literal["array"]

    item_type: JSONSchemaArraySubtype
    # Here, we are using an "abbreviated" JSON schema, as we are not allowing nested arrays
    # items: JSONSchemaString | JSONSchemaBoolean | JSONSchemaNumber = Field(
    #     discriminator="type"
    # )
    minItems: int | None = None  # noqa: N815
    maxItems: int | None = None  # noqa: N815


JSONSchemaUnion = (
    JSONSchemaInteger
    | JSONSchemaNumber
    | JSONSchemaString
    | JSONSchemaArray
    | JSONSchemaBoolean
)
