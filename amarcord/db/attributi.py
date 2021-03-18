import datetime
import logging
from typing import Any, Dict, Optional

from amarcord.db.attributo_value import AttributoValue
from amarcord.db.comment import DBComment
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.attributo_type import (
    AttributoTypeChoice,
    AttributoTypeComments,
    AttributoTypeDateTime,
    AttributoTypeDouble,
    AttributoTypeDuration,
    AttributoTypeInt,
    AttributoTypeList,
    AttributoTypeSample,
    AttributoTypeString,
    AttributoTypeTags,
    AttributoTypeUserName,
    AttributoType,
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
from amarcord.numeric_range import NumericRange
from amarcord.util import print_natural_delta

logger = logging.getLogger(__name__)


def schema_json_to_attributo_type(json_schema: JSONDict) -> AttributoType:
    return schema_to_attributo_type(parse_schema_type(json_schema))


def schema_to_attributo_type(parsed_schema: JSONSchemaType) -> AttributoType:
    if isinstance(parsed_schema, JSONSchemaNumber):
        return AttributoTypeDouble(
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
        return AttributoTypeInt(range=None)
    if isinstance(parsed_schema, JSONSchemaArray):
        if isinstance(parsed_schema.value_type, JSONSchemaNumber):
            return AttributoTypeList(
                schema_to_attributo_type(parsed_schema.value_type),
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
            return AttributoTypeTags()
        return AttributoTypeList(
            schema_to_attributo_type(parsed_schema.value_type),
            min_length=parsed_schema.min_items,
            max_length=parsed_schema.max_items,
        )
    if isinstance(parsed_schema, JSONSchemaString):
        if parsed_schema.enum_ is not None:
            return AttributoTypeChoice([(s, s) for s in parsed_schema.enum_])
        if parsed_schema.format_ == JSONSchemaStringFormat.DATE_TIME:
            return AttributoTypeDateTime()
        if parsed_schema.format_ == JSONSchemaStringFormat.DURATION:
            return AttributoTypeDuration()
        if parsed_schema.format_ == JSONSchemaStringFormat.USER_NAME:
            return AttributoTypeUserName()
        return AttributoTypeString()
    raise Exception(f'invalid schema type "{type(parsed_schema)}"')


def attributo_type_to_schema(rp: AttributoType) -> JSONDict:
    if isinstance(rp, AttributoTypeInt):
        result_int: Dict[str, JSONValue] = {"type": "integer"}
        if rp.range is not None:
            result_int["minimum"] = rp.range[0]
            result_int["maximum"] = rp.range[1]
        return result_int
    if isinstance(rp, AttributoTypeDouble):
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
    if isinstance(rp, AttributoTypeString):
        return {"type": "string"}
    if isinstance(rp, AttributoTypeUserName):
        return {"type": "string", "format": "user-name"}
    if isinstance(rp, AttributoTypeSample):
        return {"type": "integer"}
    if isinstance(rp, AttributoTypeChoice):
        return {"type": "string", "enum": [v[1] for v in rp.values]}
    if isinstance(rp, AttributoTypeDateTime):
        return {"type": "string", "format": "date-time"}
    if isinstance(rp, AttributoTypeDuration):
        return {"type": "string", "format": "duration"}
    if isinstance(rp, AttributoTypeList):
        base: JSONDict = {
            "type": "array",
            "items": attributo_type_to_schema(rp.sub_type),
        }
        if rp.min_length is not None:
            base["minItems"] = rp.min_length
        if rp.max_length is not None:
            base["maxItems"] = rp.max_length
        return base
    if isinstance(rp, AttributoTypeTags):
        return {"type": "array", "items": {"type": "string"}}
    if isinstance(rp, AttributoTypeComments):
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
    attributo_metadata: DBAttributo, value: AttributoValue
) -> str:
    if value is None:
        return ""
    rpt = attributo_metadata.attributo_type if attributo_metadata is not None else None
    if rpt is not None:
        if isinstance(rpt, AttributoTypeDuration):
            # FIXME
            assert isinstance(
                value, datetime.timedelta
            ), f'expected timedelta for "{attributo_metadata.name}", got {type(value)}'
            return print_natural_delta(value)
        if isinstance(rpt, AttributoTypeComments):
            assert isinstance(
                value, list
            ), f"Comment column isn't a list but {type(value)}"
            if not value:
                return ""
            last_comment = value[-1]
            assert isinstance(last_comment, DBComment)
            last_comment_text = f"{last_comment.author}: {last_comment.text}"
            return (
                last_comment_text
                if len(value) == 1
                else f"{last_comment_text} [+{len(value)} more]"
            )
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
        attributo_metadata.attributo_type, AttributoTypeComments
    ):
        return len(value)
    if isinstance(value, list):
        return len(value)
    return value if value is not None else ""


def attributo_type_to_string(pt: AttributoType, plural: bool = False) -> str:
    if isinstance(pt, AttributoTypeInt):
        return "integers" if plural else "integer"
    if isinstance(pt, AttributoTypeChoice):
        return "choices" if plural else "choice"
    if isinstance(pt, AttributoTypeDouble):
        if pt.suffix:
            return f"{pt.suffix} ∈ {pt.range}" if pt.range is not None else pt.suffix
        word = "numbers" if plural else "number"
        return f"{word} ∈ {pt.range}" if pt.range is not None else word
    if isinstance(pt, AttributoTypeTags):
        return "tags"
    if isinstance(pt, AttributoTypeSample):
        return "Sample IDs" if plural else "Sample ID"
    if isinstance(pt, AttributoTypeString):
        return "texts" if plural else "text"
    if isinstance(pt, AttributoTypeComments):
        return "comments"
    if isinstance(pt, AttributoTypeDateTime):
        return "date and time"
    if isinstance(pt, AttributoTypeDuration):
        return "durations" if plural else "duration"
    if isinstance(pt, AttributoTypeUserName):
        return "user names" if plural else "user name"
    if isinstance(pt, AttributoTypeList):
        return "list of " + attributo_type_to_string(pt.sub_type, plural=True)
    raise Exception(f"invalid property type {type(pt)}")
