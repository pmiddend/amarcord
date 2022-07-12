import datetime
import logging
from dataclasses import dataclass
from typing import Callable
from typing import Final
from typing import Type

from pint import UnitRegistry

from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoType, AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeSample
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.dbattributo import DBAttributo
from amarcord.json import JSONDict
from amarcord.json_schema import (
    JSONSchemaArray,
    JSONSchemaBoolean,
    JSONSchemaIntegerFormat,
    JSONSchemaCustomIntegerFormat,
)
from amarcord.json_schema import JSONSchemaInteger
from amarcord.json_schema import JSONSchemaNumber
from amarcord.json_schema import JSONSchemaNumberFormat
from amarcord.json_schema import JSONSchemaString
from amarcord.json_schema import JSONSchemaType
from amarcord.json_schema import parse_schema_type
from amarcord.numeric_range import NumericRange
from amarcord.util import str_to_float
from amarcord.util import str_to_int

_ATTRIBUTO_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

_JSON_SCHEMA_INTEGER_SAMPLE_ID: Final = JSONSchemaCustomIntegerFormat("sample-id")

logger = logging.getLogger(__name__)

ATTRIBUTO_STARTED: Final = AttributoId("started")
ATTRIBUTO_STOPPED: Final = AttributoId("stopped")


def schema_json_to_attributo_type(json_schema: JSONDict) -> AttributoType:
    return schema_to_attributo_type(parse_schema_type(json_schema))


def schema_to_attributo_type(parsed_schema: JSONSchemaType) -> AttributoType:
    if isinstance(parsed_schema, JSONSchemaNumber):
        return AttributoTypeDecimal(
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
            tolerance=parsed_schema.tolerance,
            tolerance_is_absolute=parsed_schema.tolerance_is_absolute,
        )
    if isinstance(parsed_schema, JSONSchemaBoolean):
        return AttributoTypeBoolean()
    if isinstance(parsed_schema, JSONSchemaInteger):
        if parsed_schema.format_ is not None:
            if parsed_schema.format_ == _JSON_SCHEMA_INTEGER_SAMPLE_ID:
                return AttributoTypeSample()
            if parsed_schema.format_ == JSONSchemaIntegerFormat.DATE_TIME:
                return AttributoTypeDateTime()
            raise Exception(f'integer with format "{parsed_schema.format_}" invalid')
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
        return AttributoTypeString()
    raise Exception(f'invalid schema type "{type(parsed_schema)}"')


def datetime_to_attributo_int(d: datetime.datetime) -> int:
    return int(d.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)


def datetime_from_attributo_int(d: int) -> datetime.datetime:
    # see
    # https://stackoverflow.com/questions/748491/how-do-i-create-a-datetime-in-python-from-milliseconds
    return datetime.datetime.utcfromtimestamp(d // 1000).replace(
        microsecond=d % 1000 * 1000
    )


def datetime_to_attributo_string(d: datetime.datetime) -> str:
    return d.strftime(_ATTRIBUTO_DATETIME_FORMAT)


def datetime_from_attributo_string(d: str) -> datetime.datetime:
    return datetime.datetime.strptime(d, _ATTRIBUTO_DATETIME_FORMAT)


def attributo_type_to_string(pt: AttributoType) -> str:
    if isinstance(pt, AttributoTypeInt):
        return "integer"
    if isinstance(pt, AttributoTypeBoolean):
        return "yes/no"
    if isinstance(pt, AttributoTypeString):
        return "string"
    if isinstance(pt, AttributoTypeSample):
        return "Sample ID"
    if isinstance(pt, AttributoTypeChoice):
        return "one of " + ", ".join(pt.values)
    if isinstance(pt, AttributoTypeDecimal):
        tolerance_string = (
            " ("
            + ("absolute " if pt.tolerance_is_absolute else "")
            + f"tolerance {pt.tolerance})"
            if pt.tolerance is not None
            else ""
        )
        if pt.suffix:
            return (
                f"{pt.suffix} ∈ {pt.range}{tolerance_string}"
                if pt.range is not None
                else pt.suffix
            )
        word = "number"
        return (
            f"{word} ∈ {pt.range}{tolerance_string}" if pt.range is not None else word
        )
    if isinstance(pt, AttributoTypeDateTime):
        return "date-time"
    if isinstance(pt, AttributoTypeList):
        return "list of " + attributo_type_to_string(pt.sub_type)
    raise Exception(f"invalid property type {type(pt)}")


def attributo_type_to_schema(rp: AttributoType) -> JSONSchemaType:
    if isinstance(rp, AttributoTypeInt):
        return JSONSchemaInteger(format_=None)
    if isinstance(rp, AttributoTypeBoolean):
        return JSONSchemaBoolean()
    if isinstance(rp, AttributoTypeDecimal):
        minimum: float | None
        maximum: float | None
        exclusiveMinimum: float | None
        exclusiveMaximum: float | None
        if rp.range is not None:
            if rp.range.minimum is not None:
                if rp.range.minimum_inclusive:
                    minimum = rp.range.minimum
                    exclusiveMinimum = None
                else:
                    minimum = None
                    exclusiveMinimum = rp.range.minimum
            else:
                minimum = None
                exclusiveMinimum = None
            if rp.range.maximum is not None:
                if rp.range.maximum_inclusive:
                    maximum = rp.range.maximum
                    exclusiveMaximum = None
                else:
                    maximum = None
                    exclusiveMaximum = rp.range.maximum
            else:
                maximum = None
                exclusiveMaximum = None
        else:
            minimum = None
            maximum = None
            exclusiveMinimum = None
            exclusiveMaximum = None
        return JSONSchemaNumber(
            format_=JSONSchemaNumberFormat.STANDARD_UNIT if rp.standard_unit else None,
            suffix=rp.suffix,
            tolerance=rp.tolerance,
            tolerance_is_absolute=rp.tolerance_is_absolute,
            minimum=minimum,
            exclusiveMinimum=exclusiveMinimum,
            maximum=maximum,
            exclusiveMaximum=exclusiveMaximum,
        )
    if isinstance(rp, AttributoTypeString):
        return JSONSchemaString(enum_=None)
    if isinstance(rp, AttributoTypeSample):
        return JSONSchemaInteger(format_=_JSON_SCHEMA_INTEGER_SAMPLE_ID)
    if isinstance(rp, AttributoTypeChoice):
        return JSONSchemaString(enum_=rp.values)
    if isinstance(rp, AttributoTypeDateTime):
        return JSONSchemaInteger(format_=JSONSchemaIntegerFormat.DATE_TIME)
    if isinstance(rp, AttributoTypeList):
        return JSONSchemaArray(
            value_type=attributo_type_to_schema(rp.sub_type),
            min_items=rp.min_length,
            max_items=rp.max_length,
        )
    raise Exception(f"invalid property type {type(rp)}")


@dataclass(frozen=True)
class AttributoConversionFlags:
    ignore_units: bool


_AttributoTypeConverter = Callable[
    [AttributoType, AttributoType, AttributoConversionFlags, AttributoValue],
    AttributoValue,
]

_conversion_matrix: dict[tuple[Type, Type], _AttributoTypeConverter] = {}


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

    if value is None:
        return None
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
    if not isinstance(after_type.sub_type, AttributoTypeDecimal):
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
    assert isinstance(after_type, AttributoTypeDecimal)
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
    assert isinstance(before_type, AttributoTypeDecimal)
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
    assert isinstance(after_type, AttributoTypeDecimal)
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

    trues = ("true", "1", "yes")
    falses = ("false", "0", "no")

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
    assert isinstance(before_type, AttributoTypeDecimal)
    assert isinstance(after_type, AttributoTypeInt)
    assert isinstance(v, (int, float))

    return int(v)


def _convert_double_to_double(
    before_type: AttributoType,
    after_type: AttributoType,
    conversion_flags: AttributoConversionFlags,
    v: AttributoValue,
) -> AttributoValue:
    assert isinstance(before_type, AttributoTypeDecimal)
    assert isinstance(after_type, AttributoTypeDecimal)
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
        return UnitRegistry().Quantity(v, before_type.suffix).to(after_type.suffix).m  # type: ignore

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
        return datetime_to_attributo_int(datetime_from_attributo_string(v))
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
    assert isinstance(after_type, AttributoTypeDecimal)
    assert isinstance(v, str)

    vi = str_to_float(v.strip())

    if vi is None:
        raise Exception(f'cannot convert string "{v.strip()}" to float')

    return _convert_double_to_double(
        AttributoTypeDecimal(), after_type, _conversion_flags, vi
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

    # Empty value is special for "not set"
    if value == "":
        return value

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
        (AttributoTypeInt, AttributoTypeDecimal): _convert_int_to_double,
        (AttributoTypeInt, AttributoTypeString): lambda before, after, flags, v: str(v),
        # start list
        (AttributoTypeList, AttributoTypeList): _convert_list_to_list,
        # start string
        (AttributoTypeString, AttributoTypeString): lambda before, after, flags, v: v,
        (AttributoTypeString, AttributoTypeInt): _convert_string_to_int,
        (AttributoTypeString, AttributoTypeDateTime): _convert_string_to_datetime,
        (AttributoTypeString, AttributoTypeChoice): _convert_string_to_choice,
        (AttributoTypeString, AttributoTypeDecimal): _convert_string_to_double,
        (AttributoTypeString, AttributoTypeList): _convert_string_to_string_list,
        # start double
        (AttributoTypeDecimal, AttributoTypeDecimal): _convert_double_to_double,
        (AttributoTypeDecimal, AttributoTypeInt): _convert_double_to_int,
        (AttributoTypeDecimal, AttributoTypeList): _convert_double_to_double_list,
        (
            AttributoTypeDecimal,
            AttributoTypeString,
        ): lambda before, after, flags, v: str(v),
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
            AttributoTypeDecimal,
            AttributoTypeBoolean,
        ): _convert_double_to_boolean,
        (
            AttributoTypeBoolean,
            AttributoTypeInt,
        ): _convert_boolean_to_int,
        (
            AttributoTypeBoolean,
            AttributoTypeDecimal,
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
        ): lambda before, after, flags, v: datetime_to_attributo_string(
            v  # type: ignore
        ),
        # start choice
        (AttributoTypeChoice, AttributoTypeChoice): _convert_choice_to_choice,
        (AttributoTypeChoice, AttributoTypeString): lambda before, after, flags, v: v,
    }
)

# This function is not really needed, but it's just nicer to have the attributo in the runs table sorted by...
# 0. ID (not an attributo)
# 1. "started time"
# 2. "stopped time"
# _. "the rest"
def attributo_sort_key(r: DBAttributo) -> tuple[int, str]:
    return (
        0 if r.name == ATTRIBUTO_STARTED else 1 if r.name == ATTRIBUTO_STOPPED else 2,
        r.name,
    )


def attributo_types_semantically_equivalent(a: AttributoType, b: AttributoType) -> bool:
    if isinstance(a, AttributoTypeDecimal) and isinstance(b, AttributoTypeDecimal):
        if a.standard_unit and b.standard_unit:
            if UnitRegistry()(a.suffix) != UnitRegistry()(b.suffix):
                return False
            return a.range == b.range
        return a.range == b.range and a.suffix == b.suffix
    return a == b
