import datetime
import logging
from typing import Any, Dict, List, Optional

from PyQt5 import QtCore, QtWidgets

from amarcord.db.attributo_value import AttributoValue
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.rich_attributo_type import (
    PropertyChoice,
    PropertyComments,
    PropertyDateTime,
    PropertyDouble,
    PropertyDuration,
    PropertyInt,
    PropertySample,
    PropertyString,
    PropertyTags,
    RichAttributoType,
)
from amarcord.json_schema import (
    JSONSchemaArray,
    JSONSchemaInteger,
    JSONSchemaNumber,
    JSONSchemaString,
    JSONSchemaStringFormat,
    parse_schema_type,
)
from amarcord.modules.json import JSONDict, JSONValue
from amarcord.qt.datetime import print_natural_delta
from amarcord.qt.numeric_range_format_widget import NumericRange
from amarcord.qt.table_delegates import (
    ComboItemDelegate,
    DateTimeItemDelegate,
    DoubleItemDelegate,
    DurationItemDelegate,
    IntItemDelegate,
    TagsItemDelegate,
)

logger = logging.getLogger(__name__)


def delegate_for_property_type(
    proptype: RichAttributoType,
    sample_ids: List[int],
    parent: Optional[QtCore.QObject] = None,
) -> QtWidgets.QAbstractItemDelegate:
    if isinstance(proptype, PropertyInt):
        return IntItemDelegate(proptype.nonNegative, proptype.range, parent)
    if isinstance(proptype, PropertyDouble):
        return DoubleItemDelegate(proptype.range, proptype.suffix, parent)
    if isinstance(proptype, PropertyDuration):
        return DurationItemDelegate(parent)
    if isinstance(proptype, PropertyString):
        return QtWidgets.QStyledItemDelegate(parent=parent)
    if isinstance(proptype, PropertyChoice):
        return ComboItemDelegate(values=proptype.values, parent=parent)
    if isinstance(proptype, PropertySample):
        return ComboItemDelegate(
            values=[(str(v), v) for v in sample_ids], parent=parent
        )
    if isinstance(proptype, PropertyTags):
        return TagsItemDelegate(available_tags=[], parent=parent)
    if isinstance(proptype, PropertyDateTime):
        return DateTimeItemDelegate(parent=parent)
    raise Exception(f"invalid property type {proptype}")


def schema_to_property_type(json_schema: JSONDict) -> RichAttributoType:
    parsed_schema = parse_schema_type(json_schema)
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
        assert isinstance(
            parsed_schema.value_type, JSONSchemaString
        ), "arrays of non-strings aren't supported yet"
        assert (
            parsed_schema.value_type.enum_ is None
        ), "arrays of enum strings aren't supported yet"
        return PropertyTags()
    if isinstance(parsed_schema, JSONSchemaString):
        if parsed_schema.enum_ is not None:
            return PropertyChoice([(s, s) for s in parsed_schema.enum_])
        if parsed_schema.format_ == JSONSchemaStringFormat.DATE_TIME:
            return PropertyDateTime()
        if parsed_schema.format_ == JSONSchemaStringFormat.DURATION:
            return PropertyDuration()
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
    if isinstance(rp, PropertySample):
        return {"type": "integer"}
    if isinstance(rp, PropertyChoice):
        return {"type": "string", "enum": [v[1] for v in rp.values]}
    if isinstance(rp, PropertyDateTime):
        return {"type": "string", "format": "date-time"}
    if isinstance(rp, PropertyDuration):
        return {"type": "string", "format": "duration"}
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
        return ", ".join(value)
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


def attributo_type_to_string(attributo: DBAttributo) -> str:
    pt = attributo.rich_property_type
    if isinstance(pt, PropertyInt):
        return "integer"
    if isinstance(pt, PropertyChoice):
        return "choice"
    if isinstance(pt, PropertyDouble):
        if pt.suffix:
            return (
                f"{pt.suffix} (range {pt.range})" if pt.range is not None else pt.suffix
            )
        return f"number in {pt.range}" if pt is not None else "number"
    if isinstance(pt, PropertyTags):
        return "tags"
    if isinstance(pt, PropertySample):
        return "Sample ID"
    if isinstance(pt, PropertyString):
        return "text"
    if isinstance(pt, PropertyComments):
        return "comments"
    if isinstance(pt, PropertyDateTime):
        return "date and time"
    if isinstance(pt, PropertyDuration):
        return "duration"
    raise Exception(f"invalid property type {type(pt)}")
