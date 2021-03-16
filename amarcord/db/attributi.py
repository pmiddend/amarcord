import datetime
import logging
from typing import Any, Dict, Optional

from amarcord.db.attributo_value import AttributoValue
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.rich_attributo_type import (
    PropertyChoice,
    PropertyComments,
    PropertyDateTime,
    PropertyDouble,
    PropertyDuration,
    PropertyInt,
    PropertyList,
    PropertySample,
    PropertyString,
    PropertyTags,
    PropertyUserName,
    RichAttributoType,
)
from amarcord.json_schema import (
    JSONSchemaArray,
    JSONSchemaInteger,
    JSONSchemaNumber,
    JSONSchemaString,
    JSONSchemaStringFormat,
    JSONSchemaType,
    parse_schema_type,
)
from amarcord.modules.json import JSONDict, JSONValue
from amarcord.qt.datetime import print_natural_delta
from amarcord.qt.numeric_range_format_widget import NumericRange

logger = logging.getLogger(__name__)


def schema_json_to_property_type(json_schema: JSONDict) -> RichAttributoType:
    return schema_to_property_type(parse_schema_type(json_schema))


def schema_to_property_type(parsed_schema: JSONSchemaType) -> RichAttributoType:
    if isinstance(parsed_schema, JSONSchemaNumber):
        return PropertyDouble(
            range=None
            if parsed_schema.minimum is None
            and parsed_schema.maximum is None
            and parsed_schema.exclusiveMaximum is None
            and parsed_schema.exclusiveMinimum is None
            else NumericRange(
                parsed_schema.minimum
                if parsed_schema.minimum is not None
                else parsed_schema.exclusiveMinimum,
                parsed_schema.exclusiveMinimum is None,
                parsed_schema.maximum
                if parsed_schema.maximum is not None
                else parsed_schema.exclusiveMaximum,
                parsed_schema.exclusiveMaximum is None,
            ),
            suffix=parsed_schema.suffix,
        )
    if isinstance(parsed_schema, JSONSchemaInteger):
        return PropertyInt(range=None)
    if isinstance(parsed_schema, JSONSchemaArray):
        if isinstance(parsed_schema.value_type, JSONSchemaNumber):
            return PropertyList(
                schema_to_property_type(parsed_schema.value_type),
                min_length=parsed_schema.min_items,
                max_length=parsed_schema.max_items,
            )
        assert isinstance(
            parsed_schema.value_type, JSONSchemaString
        ), "arrays of non-strings aren't supported yet"
        assert (
            parsed_schema.value_type.enum_ is None
        ), "arrays of enum strings aren't supported yet"
        if parsed_schema.value_type.format_ == JSONSchemaStringFormat.TAG:
            return PropertyTags()
        return PropertyList(
            schema_to_property_type(parsed_schema.value_type),
            min_length=parsed_schema.min_items,
            max_length=parsed_schema.max_items,
        )
    if isinstance(parsed_schema, JSONSchemaString):
        if parsed_schema.enum_ is not None:
            return PropertyChoice([(s, s) for s in parsed_schema.enum_])
        if parsed_schema.format_ == JSONSchemaStringFormat.DATE_TIME:
            return PropertyDateTime()
        if parsed_schema.format_ == JSONSchemaStringFormat.DURATION:
            return PropertyDuration()
        if parsed_schema.format_ == JSONSchemaStringFormat.USER_NAME:
            return PropertyUserName()
        return PropertyString()
    raise Exception(f'invalid schema type "{type(parsed_schema)}"')


def property_type_to_schema(rp: RichAttributoType) -> JSONDict:
    if isinstance(rp, PropertyInt):
        result_int: Dict[str, JSONValue] = {"type": "integer"}
        if rp.range is not None:
            result_int["minimum"] = rp.range[0]
            result_int["maximum"] = rp.range[1]
        return result_int
    if isinstance(rp, PropertyDouble):
        result_double: Dict[str, JSONValue] = {"type": "number"}
        if rp.range is not None:
            if rp.range.minimum is not None:
                if rp.range.minimum_inclusive:
                    result_double["minimum"] = rp.range.minimum
                else:
                    result_double["exclusiveMinimum"] = rp.range.minimum
            if rp.range.maximum is not None:
                if rp.range.maximum_inclusive:
                    result_double["maximum"] = rp.range.maximum
                else:
                    result_double["exclusiveMaximum"] = rp.range.maximum
        if rp.suffix is not None:
            assert isinstance(rp.suffix, str)
            result_double["suffix"] = rp.suffix
        return result_double
    if isinstance(rp, PropertyString):
        return {"type": "string"}
    if isinstance(rp, PropertyUserName):
        return {"type": "string", "format": "user-name"}
    if isinstance(rp, PropertySample):
        return {"type": "integer"}
    if isinstance(rp, PropertyChoice):
        return {"type": "string", "enum": [v[1] for v in rp.values]}
    if isinstance(rp, PropertyDateTime):
        return {"type": "string", "format": "date-time"}
    if isinstance(rp, PropertyDuration):
        return {"type": "string", "format": "duration"}
    if isinstance(rp, PropertyList):
        base: JSONDict = {
            "type": "array",
            "items": property_type_to_schema(rp.sub_property),
        }
        if rp.min_length is not None:
            base["minItems"] = rp.min_length
        if rp.max_length is not None:
            base["maxItems"] = rp.max_length
        return base
    if isinstance(rp, PropertyTags):
        return {"type": "array", "items": {"type": "string"}}
    if isinstance(rp, PropertyComments):
        return {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "id": {"type": "integer"},
                    "text": {"type": "string"},
                    "created": {"type": "string", "format": "date-time"},
                    "author": {"type": "string"},
                },
            },
        }
    raise Exception(f"invalid property type {type(rp)}")


def pretty_print_attributo(
    attributo_metadata: Optional[DBAttributo], value: AttributoValue
) -> str:
    if value is None:
        return ""
    rpt = (
        attributo_metadata.rich_property_type
        if attributo_metadata is not None
        else None
    )
    if rpt is not None:
        if isinstance(rpt, PropertyDuration):
            # FIXME
            assert isinstance(
                value, datetime.timedelta
            ), f'expected timedelta for "{attributo_metadata.name}", got {type(value)}'
            return print_natural_delta(value)
        if isinstance(rpt, PropertyComments):
            assert isinstance(
                value, list
            ), f"Comment column isn't a list but {type(value)}"
            return "\n".join(f"{c.author}: {c.text}" for c in value)
    if isinstance(value, list):
        if not value:
            return ""
        if isinstance(value[0], float):
            return ", ".join(f"{s:.2f}" for s in value)
        return ", ".join(str(s) for s in value)
    if isinstance(value, float):
        return f"{value:.2f}"
    return str(value) if value is not None else ""


def sortable_attributo(attributo_metadata: Optional[DBAttributo], value: Any) -> Any:
    if attributo_metadata is not None and isinstance(
        attributo_metadata.rich_property_type, PropertyComments
    ):
        return len(value)
    if isinstance(value, list):
        return len(value)
    return value if value is not None else ""


def attributo_type_to_string(pt: RichAttributoType, plural: bool = False) -> str:
    if isinstance(pt, PropertyInt):
        return "integers" if plural else "integer"
    if isinstance(pt, PropertyChoice):
        return "choices" if plural else "choice"
    if isinstance(pt, PropertyDouble):
        if pt.suffix:
            return f"{pt.suffix} ∈ {pt.range}" if pt.range is not None else pt.suffix
        word = "numbers" if plural else "number"
        return f"{word} ∈ {pt.range}" if pt.range is not None else word
    if isinstance(pt, PropertyTags):
        return "tags"
    if isinstance(pt, PropertySample):
        return "Sample IDs" if plural else "Sample ID"
    if isinstance(pt, PropertyString):
        return "texts" if plural else "text"
    if isinstance(pt, PropertyComments):
        return "comments"
    if isinstance(pt, PropertyDateTime):
        return "date and time"
    if isinstance(pt, PropertyDuration):
        return "durations" if plural else "duration"
    if isinstance(pt, PropertyUserName):
        return "user names" if plural else "user name"
    if isinstance(pt, PropertyList):
        return "list of " + attributo_type_to_string(pt.sub_property, plural=True)
    raise Exception(f"invalid property type {type(pt)}")
