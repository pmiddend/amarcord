import datetime
import logging
from dataclasses import dataclass
from typing import Any
from typing import Callable
from typing import Type

from pint import UnitRegistry
from pydantic import BaseModel
from pydantic import Field

from amarcord.db.attributo_type import ArrayAttributoType
from amarcord.db.attributo_type import AttributoType
from amarcord.db.attributo_type import AttributoTypeBoolean
from amarcord.db.attributo_type import AttributoTypeChemical
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDecimal
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.dbattributo import DBAttributo
from amarcord.json_schema import JSONSchemaArray
from amarcord.json_schema import JSONSchemaArraySubtype
from amarcord.json_schema import JSONSchemaBoolean
from amarcord.json_schema import JSONSchemaInteger
from amarcord.json_schema import JSONSchemaNumber
from amarcord.json_schema import JSONSchemaString
from amarcord.json_schema import JSONSchemaUnion
from amarcord.numeric_range import NumericRange
from amarcord.util import str_to_float
from amarcord.util import str_to_int

# Little registry for semantic comparisons (see below)
_UNIT_REGISTRY = UnitRegistry()
_ATTRIBUTO_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%SZ"

logger = logging.getLogger(__name__)


class SchemaPydanticWrapper(BaseModel):
    content: JSONSchemaUnion = Field(discriminator="type")


def parse_schema_type(json_schema: dict[str, Any]) -> JSONSchemaUnion:
    return SchemaPydanticWrapper(content=json_schema).content  # type: ignore


def coparse_schema_type(s: JSONSchemaUnion) -> dict[str, Any]:
    return s.dict()


def schema_union_to_attributo_type(s: JSONSchemaUnion) -> AttributoType:
    return schema_to_attributo_type(
        schema_number=s if isinstance(s, JSONSchemaNumber) else None,
        schema_boolean=s if isinstance(s, JSONSchemaBoolean) else None,
        schema_integer=s if isinstance(s, JSONSchemaInteger) else None,
        schema_array=s if isinstance(s, JSONSchemaArray) else None,
        schema_string=s if isinstance(s, JSONSchemaString) else None,
    )


def schema_to_attributo_type(
    schema_number: None | JSONSchemaNumber,
    schema_boolean: None | JSONSchemaBoolean,
    schema_integer: None | JSONSchemaInteger,
    schema_array: None | JSONSchemaArray,
    schema_string: None | JSONSchemaString,
) -> AttributoType:
    if schema_number is not None:
        return AttributoTypeDecimal(
            range=(
                None
                if schema_number.minimum is None
                and schema_number.maximum is None
                and schema_number.exclusiveMaximum is None
                and schema_number.exclusiveMinimum is None
                else NumericRange(
                    (
                        schema_number.minimum
                        if schema_number.minimum is not None
                        else schema_number.exclusiveMinimum
                    ),
                    schema_number.exclusiveMinimum is None,
                    (
                        schema_number.maximum
                        if schema_number.maximum is not None
                        else schema_number.exclusiveMaximum
                    ),
                    schema_number.exclusiveMaximum is None,
                )
            ),
            suffix=schema_number.suffix,
            standard_unit=schema_number.format == "standard-unit",
            tolerance=schema_number.tolerance,
            tolerance_is_absolute=schema_number.toleranceIsAbsolute,
        )
    if schema_boolean is not None:
        return AttributoTypeBoolean()
    if schema_integer is not None:
        if schema_integer.format == "chemical-id":
            return AttributoTypeChemical()
        if schema_integer.format == "date-time":
            return AttributoTypeDateTime()
        return AttributoTypeInt()
    if schema_array is not None:
        return AttributoTypeList(
            sub_type=ArrayAttributoType(schema_array.item_type.value),
            min_length=schema_array.minItems,
            max_length=schema_array.maxItems,
        )
    assert isinstance(
        schema_string, JSONSchemaString
    ), f"unknown schema type {schema_string}"
    if schema_string.enum is not None:
        return AttributoTypeChoice(schema_string.enum)
    return AttributoTypeString()


def datetime_to_attributo_int(d: datetime.datetime) -> int:
    return int(d.replace(tzinfo=datetime.timezone.utc).timestamp() * 1000)


def datetime_from_float_in_seconds(d: float) -> datetime.datetime:
    return datetime.datetime.utcfromtimestamp(d)


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
    if isinstance(pt, AttributoTypeChemical):
        return "chemical ID"
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
    assert isinstance(pt, AttributoTypeList)
    return "list of " + pt.sub_type


def attributo_type_to_schema(
    rp: AttributoType,
) -> (
    JSONSchemaInteger
    | JSONSchemaNumber
    | JSONSchemaString
    | JSONSchemaArray
    | JSONSchemaBoolean
):
    if isinstance(rp, AttributoTypeInt):
        return JSONSchemaInteger(type="integer", format=None)
    if isinstance(rp, AttributoTypeBoolean):
        return JSONSchemaBoolean(type="boolean")
    if isinstance(rp, AttributoTypeDecimal):
        minimum: float | None
        maximum: float | None
        exclusiveMinimum: float | None
        exclusiveMaximum: float | None
        if rp.range is not None:
            if rp.range.minimum is not None:
                print(f"minimum is: {rp.range.minimum}, {rp.range.minimum_inclusive}")
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
            type="number",
            format="standard-unit" if rp.standard_unit else None,
            suffix=rp.suffix,
            tolerance=rp.tolerance,
            toleranceIsAbsolute=rp.tolerance_is_absolute,
            minimum=minimum,
            exclusiveMinimum=exclusiveMinimum,
            maximum=maximum,
            exclusiveMaximum=exclusiveMaximum,
        )
    if isinstance(rp, AttributoTypeString):
        return JSONSchemaString(type="string", enum=None)
    if isinstance(rp, AttributoTypeChemical):
        return JSONSchemaInteger(type="integer", format="chemical-id")
    if isinstance(rp, AttributoTypeChoice):
        return JSONSchemaString(type="string", enum=rp.values)
    if isinstance(rp, AttributoTypeDateTime):
        return JSONSchemaInteger(type="integer", format="date-time")
    assert isinstance(rp, AttributoTypeList)
    return JSONSchemaArray(
        type="array",
        item_type=JSONSchemaArraySubtype(rp.sub_type.value),
        minItems=rp.min_length,
        maxItems=rp.max_length,
    )


@dataclass(frozen=True)
class AttributoConversionFlags:
    ignore_units: bool


_AttributoTypeConverter = Callable[
    [AttributoType, AttributoType, AttributoConversionFlags, AttributoValue],
    AttributoValue,
]

_conversion_matrix: dict[tuple[Type[Any], Type[Any]], _AttributoTypeConverter] = {}


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
    if after_type.sub_type != ArrayAttributoType.ARRAY_NUMBER:
        raise Exception(
            f"cannot convert from {before_type} to {after_type} (maybe convert to the list value type "
            + "first, and then to list?)"
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
    if after_type.sub_type != ArrayAttributoType.ARRAY_NUMBER:
        raise Exception(
            f"cannot convert from {before_type} to {after_type} (maybe convert to the list value type "
            + "first, and then to list?)"
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
    if after_type.sub_type != ArrayAttributoType.ARRAY_STRING:
        raise Exception(
            f"cannot convert from {before_type} to {after_type} (maybe convert to the list value type "
            + "first, and then to list?)"
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
            _UNIT_REGISTRY.Quantity(v, before_type.suffix).to(after_type.suffix).m
        )
        if after_type.range is not None and not after_type.range.value_is_inside(
            magnitude_after  # pyright: ignore [reportUnknownArgumentType]
        ):
            raise Exception(
                f"cannot convert decimal number {v} because after unit conversion, the value {magnitude_after} it's "
                + f"not in the range {after_type.range} "
            )
        return _UNIT_REGISTRY.Quantity(v, before_type.suffix).to(after_type.suffix).m  # type: ignore

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

    if before_type.sub_type == after_type.sub_type:
        return value.copy()

    if (
        before_type.sub_type == ArrayAttributoType.ARRAY_STRING
        and after_type.sub_type == ArrayAttributoType.ARRAY_NUMBER
    ):
        result_float: list[float] = []
        for i, v in enumerate(value):
            try:
                result_float.append(float(v))
            except:
                raise Exception(
                    f'cannot convert element {i} of list from string "{v}" to number'
                )
        return result_float
    if (
        before_type.sub_type == ArrayAttributoType.ARRAY_STRING
        and after_type.sub_type == ArrayAttributoType.ARRAY_BOOL
    ):
        result_bool: list[bool] = []
        for i, v in enumerate(value):
            try:
                result_bool.append(bool(v))
            except:
                raise Exception(
                    f'cannot convert element {i} of list from string "{v}" to bool'
                )
        return result_bool
    if after_type.sub_type == ArrayAttributoType.ARRAY_STRING:
        result_str: list[str] = []
        for i, v in enumerate(value):
            result_str.append(str(v))
        return result_str
    # Remaining conversion are: number -> bool and bool -> number which frankly don't make much sense
    raise Exception("conversion doesn't make much sense")


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
        # start chemical
        (
            AttributoTypeChemical,
            AttributoTypeChemical,
        ): lambda before, after, flags, v: v,
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


# This function is not really needed, but it's just nicer to have the attributo in the runs table sorted by...something predefined
def attributo_sort_key(r: DBAttributo) -> tuple[int, str]:
    return (0, r.name)


def attributo_types_semantically_equivalent(a: AttributoType, b: AttributoType) -> bool:
    if isinstance(a, AttributoTypeDecimal) and isinstance(b, AttributoTypeDecimal):
        if a.standard_unit and b.standard_unit:
            if not a.suffix or not b.suffix:
                return False
            if _UNIT_REGISTRY(a.suffix) != _UNIT_REGISTRY(b.suffix):
                return False
            return a.range == b.range
        return a.range == b.range and a.suffix == b.suffix
    return a == b
