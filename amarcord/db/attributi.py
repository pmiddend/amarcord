import datetime
import logging
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Dict
from typing import Final
from typing import List
from typing import Optional
from typing import Tuple
from typing import Type

from dateutil import tz
from pint import UnitRegistry

from amarcord.db.attributo_type import AttributoType, AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeComments
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeSample
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.comment import DBComment
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.mini_sample import DBMiniSample
from amarcord.json import JSONDict
from amarcord.json import JSONValue
from amarcord.json_schema import JSONSchemaArray, JSONSchemaBoolean
from amarcord.json_schema import JSONSchemaInteger
from amarcord.json_schema import JSONSchemaNumber
from amarcord.json_schema import JSONSchemaNumberFormat
from amarcord.json_schema import JSONSchemaString
from amarcord.json_schema import JSONSchemaStringFormat
from amarcord.json_schema import JSONSchemaType
from amarcord.json_schema import parse_schema_type
from amarcord.numeric_range import NumericRange
from amarcord.util import str_to_float
from amarcord.util import str_to_int

_SAMPLE_ID: Final = "sample-id"

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
            standard_unit=parsed_schema.format_ == JSONSchemaNumberFormat.STANDARD_UNIT,
        )
    if isinstance(parsed_schema, JSONSchemaBoolean):
        return AttributoTypeBoolean()
    if isinstance(parsed_schema, JSONSchemaInteger):
        if parsed_schema.format is not None:
            if parsed_schema.format == _SAMPLE_ID:
                return AttributoTypeSample()
            raise Exception(f'integer with format "{parsed_schema.format}" invalid')
        return AttributoTypeInt()
    if isinstance(parsed_schema, JSONSchemaArray):
        if isinstance(parsed_schema.value_type, JSONSchemaNumber):
            return AttributoTypeList(
                schema_to_attributo_type(parsed_schema.value_type),
                min_length=parsed_schema.min_items,
                max_length=parsed_schema.max_items,
            )
        if isinstance(parsed_schema.value_type, JSONSchemaInteger):
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
        return AttributoTypeList(
            schema_to_attributo_type(parsed_schema.value_type),
            min_length=parsed_schema.min_items,
            max_length=parsed_schema.max_items,
        )
    if isinstance(parsed_schema, JSONSchemaString):
        if parsed_schema.enum_ is not None:
            return AttributoTypeChoice(parsed_schema.enum_)
        if parsed_schema.format_ == JSONSchemaStringFormat.DATE_TIME:
            return AttributoTypeDateTime()
        return AttributoTypeString()
    raise Exception(f'invalid schema type "{type(parsed_schema)}"')


def attributo_type_to_schema(rp: AttributoType) -> JSONDict:
    if isinstance(rp, AttributoTypeInt):
        return {"type": "integer"}
    if isinstance(rp, AttributoTypeBoolean):
        return {"type": "boolean"}
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
        if rp.standard_unit:
            result_double["format"] = "standard-unit"
        return result_double
    if isinstance(rp, AttributoTypeString):
        return {"type": "string"}
    if isinstance(rp, AttributoTypeSample):
        return {"type": "integer", "format": _SAMPLE_ID}
    if isinstance(rp, AttributoTypeChoice):
        return {"type": "string", "enum": rp.values}
    if isinstance(rp, AttributoTypeDateTime):
        return {"type": "string", "format": "date-time"}
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
    if isinstance(rp, AttributoTypeComments):
        return {
            "type": "array",
            "format": "comments",
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
    attributo_metadata: DBAttributo, value: AttributoValue, samples: List[DBMiniSample]
) -> str:
    if value is None:
        return ""
    rpt = attributo_metadata.attributo_type if attributo_metadata is not None else None
    if rpt is not None:
        if isinstance(rpt, AttributoTypeSample):
            return next(
                iter(x.sample_name for x in samples if x.sample_id == value), ""
            )
        if isinstance(rpt, AttributoTypeDateTime):
            assert isinstance(
                value, datetime.datetime
            ), f'expected datetime for "{attributo_metadata.name}", got {type(value)}'
            # correct for UTC time
            return (
                value.replace(tzinfo=tz.tzutc())
                .astimezone(tz.tzlocal())
                .strftime("%Y-%m-%d %H:%M:%S")
            )
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
    return value


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
    if isinstance(pt, AttributoTypeSample):
        return "Sample IDs" if plural else "Sample ID"
    if isinstance(pt, AttributoTypeString):
        return "texts" if plural else "text"
    if isinstance(pt, AttributoTypeComments):
        return "comments"
    if isinstance(pt, AttributoTypeDateTime):
        return "date and time"
    if isinstance(pt, AttributoTypeList):
        return "list of " + attributo_type_to_string(pt.sub_type, plural=True)
    raise Exception(f"invalid property type {type(pt)}")


@dataclass(frozen=True)
class AttributoConversionFlags:
    ignore_units: bool


_AttributoTypeConverter = Callable[
    [AttributoType, AttributoType, AttributoConversionFlags, AttributoValue],
    AttributoValue,
]

_conversion_matrix: Dict[Tuple[Type, Type], _AttributoTypeConverter] = {}


def convert_attributo_value(
    before_type: AttributoType,
    after_type: AttributoType,
    conversion_flags: AttributoConversionFlags,
    value: AttributoValue,
) -> AttributoValue:
    converter = _conversion_matrix.get((type(before_type), type(after_type)), None)

    if converter is None:
        raise Exception(
            f"cannot convert from {before_type} to {after_type}: no converter found"
        )

    return converter(before_type, after_type, conversion_flags, value)


def _convert_int_to_int_list(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(after_type, AttributoTypeList)
    assert isinstance(v, int)
    if not isinstance(after_type.sub_type, AttributoTypeInt):
        raise Exception(
            f"cannot convert from {before_type} to {after_type} (maybe convert to the list value type "
            "first, and then to list?)"
        )
    if after_type.min_length is not None and after_type.min_length > 1:
        raise Exception(
            f"cannot convert from {before_type} to {after_type} because we don't have enough elements"
        )
    return [v]


def _convert_double_to_double_list(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(after_type, AttributoTypeList)
    assert isinstance(v, float)
    if not isinstance(after_type.sub_type, AttributoTypeDouble):
        raise Exception(
            f"cannot convert from {before_type} to {after_type} (maybe convert to the list value type "
            "first, and then to list?)"
        )
    if after_type.min_length is not None and after_type.min_length > 1:
        raise Exception(
            f"cannot convert from {before_type} to {after_type} because we don't have enough elements"
        )
    return [v]


def _convert_string_to_string_list(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(after_type, AttributoTypeList)
    assert isinstance(v, str)
    if not isinstance(after_type.sub_type, AttributoTypeString):
        raise Exception(
            f"cannot convert from {before_type} to {after_type} (maybe convert to the list value type "
            "first, and then to list?)"
        )
    if after_type.min_length is not None and after_type.min_length > 1:
        raise Exception(
            f"cannot convert from {before_type} to {after_type} because we don't have enough elements"
        )
    return [v]


def _convert_int_to_int(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeInt)
    assert isinstance(after_type, AttributoTypeInt)
    assert isinstance(v, int)

    return v


def _convert_int_to_double(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeInt)
    assert isinstance(after_type, AttributoTypeDouble)
    assert isinstance(v, int)

    vd = float(v)

    if after_type.range is not None and not after_type.range.value_is_inside(vd):
        raise Exception(
            f"cannot convert integer {v} to double because it's not in the range {after_type.range}"
        )

    return v


def _convert_int_to_boolean(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeInt)
    assert isinstance(after_type, AttributoTypeBoolean)
    assert isinstance(v, int)

    return v != 0


def _convert_double_to_boolean(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeDouble)
    assert isinstance(after_type, AttributoTypeBoolean)
    assert isinstance(v, (float, int))

    return v != 0


def _convert_boolean_to_int(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeBoolean)
    assert isinstance(after_type, AttributoTypeInt)
    assert isinstance(v, bool)

    return 1 if v else 0


def _convert_boolean_to_double(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeBoolean)
    assert isinstance(after_type, AttributoTypeDouble)
    assert isinstance(v, bool)

    result = 1.0 if v else 0.0
    if after_type.range is not None and not after_type.range.value_is_inside(result):
        raise Exception(
            f"cannot convert boolean {v} to double: resulting value {result} is not in range {after_type.range}"
        )
    return result


def _convert_string_to_boolean(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeString)
    assert isinstance(after_type, AttributoTypeBoolean)
    assert isinstance(v, str)

    trues = ["true", "1", "yes"]
    falses = ["false", "0", "no"]

    if v.strip().lower() in trues:
        return True
    if v.strip().lower() in falses:
        return False

    raise Exception(f"cannot convert string {v.strip()} to boolean")


def _convert_string_to_int(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeString)
    assert isinstance(after_type, AttributoTypeInt)
    assert isinstance(v, str)

    vi = str_to_int(v.strip())

    if vi is None:
        raise Exception(f'cannot convert string "{v.strip()}" to integer')

    return _convert_int_to_int(AttributoTypeInt(), after_type, _conversion_flags, vi)


def _convert_double_to_int(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeDouble)
    assert isinstance(after_type, AttributoTypeInt)
    assert isinstance(v, (int, float))

    return int(v)


def _convert_double_to_double(
    before_type: AttributoType,
    after_type: AttributoType,
    conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeDouble)
    assert isinstance(after_type, AttributoTypeDouble)
    assert isinstance(v, (int, float))

    # If we ignore units, the range has to be correct afterwards as well
    if (
        (conversion_flags.ignore_units or not after_type.standard_unit)
        and after_type.range is not None
        and not after_type.range.value_is_inside(v)
    ):
        raise Exception(
            f"cannot convert decimal number {v} because it's not in the range {after_type.range}"
        )

    # If we want to convert units, the range needs to fit after conversion.
    # Think about a conversion from MHz to Hz.
    if (
        after_type.standard_unit
        and before_type.standard_unit
        and not conversion_flags.ignore_units
    ):
        magnitude_after = (
            UnitRegistry().Quantity(v, before_type.suffix).to(after_type.suffix).m
        )
        if after_type.range is not None and not after_type.range.value_is_inside(
            magnitude_after
        ):
            raise Exception(
                f"cannot convert decimal number {v} because after unit conversion, the value {magnitude_after} it's "
                f"not in the range {after_type.range} "
            )
        return UnitRegistry().Quantity(v, before_type.suffix).to(after_type.suffix).m

    return v


def _convert_string_to_datetime(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeString)
    assert isinstance(after_type, AttributoTypeDateTime)
    assert isinstance(v, str)

    try:
        return datetime.datetime.fromisoformat(v)
    except:
        raise Exception(f'cannot convert string "{v}" to datetime (not ISO format)')


def _convert_string_to_choice(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeString)
    assert isinstance(after_type, AttributoTypeChoice)
    assert isinstance(v, str)

    if v not in after_type.values:
        raise Exception(
            f'cannot convert string "{v}" to choice with choices '
            + ",".join(after_type.values)
        )
    return v


def _convert_string_to_double(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeString)
    assert isinstance(after_type, AttributoTypeDouble)
    assert isinstance(v, str)

    vi = str_to_float(v.strip())

    if vi is None:
        raise Exception(f'cannot convert string "{v.strip()}" to float')

    return _convert_double_to_double(
        AttributoTypeDouble(), after_type, _conversion_flags, vi
    )


def _convert_list_to_list(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    value: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeList)
    assert isinstance(after_type, AttributoTypeList)
    assert isinstance(value, list)

    if after_type.max_length is not None and len(value) > after_type.max_length:
        raise Exception(
            f"cannot convert {before_type} to {after_type} because {value} has too many elements"
        )

    if after_type.min_length is not None and len(value) < after_type.min_length:
        raise Exception(
            f"cannot convert {before_type} to {after_type} because {value} has too little elements"
        )

    return [
        # type error is expected here, since we don't have lists of AttributoValue yet, since that
        # would mean mypy has support for recursive types.
        convert_attributo_value(before_type.sub_type, after_type.sub_type, _conversion_flags, x)  # type: ignore
        for x in value
    ]


def _convert_choice_to_choice(
    before_type: AttributoType,
    after_type: AttributoType,
    _conversion_flags: AttributoConversionFlags,
    value: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeChoice)
    assert isinstance(after_type, AttributoTypeChoice)
    assert isinstance(value, str)

    if value not in after_type.values:
        raise Exception(
            f"cannot convert choice value, {value} is not in choices "
            + ",".join(after_type.values)
        )

    return value


_conversion_matrix.update(
    {
        # start int
        (AttributoTypeInt, AttributoTypeInt): _convert_int_to_int,
        (AttributoTypeInt, AttributoTypeList): _convert_int_to_int_list,
        (AttributoTypeInt, AttributoTypeDouble): _convert_int_to_double,
        (AttributoTypeInt, AttributoTypeString): lambda before, after, flags, v: str(v),
        # start list
        (AttributoTypeList, AttributoTypeList): _convert_list_to_list,
        # start string
        (AttributoTypeString, AttributoTypeString): lambda before, after, flags, v: v,
        (AttributoTypeString, AttributoTypeInt): _convert_string_to_int,
        (AttributoTypeString, AttributoTypeDateTime): _convert_string_to_datetime,
        (AttributoTypeString, AttributoTypeChoice): _convert_string_to_choice,
        (AttributoTypeString, AttributoTypeDouble): _convert_string_to_double,
        (AttributoTypeString, AttributoTypeList): _convert_string_to_string_list,
        # start double
        (AttributoTypeDouble, AttributoTypeDouble): _convert_double_to_double,
        (AttributoTypeDouble, AttributoTypeInt): _convert_double_to_int,  # type: ignore
        (AttributoTypeDouble, AttributoTypeList): _convert_double_to_double_list,
        (AttributoTypeDouble, AttributoTypeString): lambda before, after, flags, v: str(
            v
        ),
        # start bool
        (AttributoTypeBoolean, AttributoTypeBoolean): lambda before, after, flags, v: v,
        (
            AttributoTypeBoolean,
            AttributoTypeString,
        ): lambda before, after, flags, v: str(v),
        (
            AttributoTypeString,
            AttributoTypeBoolean,
        ): _convert_string_to_boolean,
        (
            AttributoTypeInt,
            AttributoTypeBoolean,
        ): _convert_int_to_boolean,
        (
            AttributoTypeDouble,
            AttributoTypeBoolean,
        ): _convert_double_to_boolean,
        (
            AttributoTypeBoolean,
            AttributoTypeInt,
        ): _convert_boolean_to_int,
        (
            AttributoTypeBoolean,
            AttributoTypeDouble,
        ): _convert_boolean_to_double,
        # start sample
        (AttributoTypeSample, AttributoTypeSample): lambda before, after, flags, v: v,
        # start datetime
        (
            AttributoTypeDateTime,
            AttributoTypeDateTime,
        ): lambda before, after, flags, v: v,
        (
            AttributoTypeDateTime,
            AttributoTypeString,
        ): lambda before, after, flags, v: v.isoformat(),  # type: ignore
        # start choice
        (AttributoTypeChoice, AttributoTypeChoice): _convert_choice_to_choice,
        (AttributoTypeChoice, AttributoTypeString): lambda before, after, flags, v: v,
    }
)
