import datetime
import math
from dataclasses import replace
from typing import Any
from typing import Iterable
from typing import cast

from amarcord.db.attributi import AttributoConversionFlags
from amarcord.db.attributi import convert_attributo_value
from amarcord.db.attributi import datetime_from_attributo_int
from amarcord.db.attributi import datetime_to_attributo_int
from amarcord.db.attributo_id import AttributoId
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
from amarcord.json_types import JSONDict
from amarcord.json_types import JSONValue

SPECIAL_VALUE_CHOICE_NONE = ""

JsonAttributiMap = dict[str, JSONValue]

UntypedAttributiMap = dict[int, AttributoValue]


def _check_type(id_: int, type_: AttributoType, value: AttributoValue) -> None:
    if value is None:
        return
    if isinstance(type_, AttributoTypeChemical):
        if not isinstance(value, int):
            raise Exception(
                f'attributo "{id_}": should have type int, but got value {value}'
            )
    if isinstance(type_, (AttributoTypeInt, AttributoTypeChemical)):
        if not isinstance(value, int):
            raise Exception(
                f'attributo "{id_}": should have type int, but got value {value} (type {type(value)})'
            )
    elif isinstance(type_, AttributoTypeString):
        if not isinstance(value, str):
            raise Exception(
                f'attributo "{id_}": should have type string, but got value {value}'
            )
    elif isinstance(type_, AttributoTypeBoolean):
        if not isinstance(value, bool):
            raise Exception(
                f'attributo "{id_}": should have type bool, but got value {value}'
            )
    elif isinstance(type_, AttributoTypeDecimal):
        assert isinstance(
            value, (float, int)
        ), f'attributo "{id_}": expected type float but got value {value}'
        if type_.range is not None and not type_.range.value_is_inside(value):
            raise ValueError(
                f'attributo "{id_}": value is out of range; range is {type_.range}, '
                + f"value is {value}"
            )
    elif isinstance(type_, AttributoTypeChoice):
        assert isinstance(
            value, str
        ), f'attributo "{id_}": expected type string but got value {value}'
        assert (
            value == SPECIAL_VALUE_CHOICE_NONE or value in type_.values
        ), f'attributo "{id_}": value "{value}" not one of ' + ", ".join(type_.values)
    elif isinstance(type_, AttributoTypeList):
        assert isinstance(
            value, list
        ), f'attributo "{id_}": expected type list but got value {value}'
        assert (
            type_.min_length is None or len(value) >= type_.min_length
        ), f'attributo "{id_}": list min length is {type_.min_length}, got {len(value)} element(s)'
        assert (
            type_.max_length is None or len(value) <= type_.max_length
        ), f'attributo "{id_}": list max length is {type_.min_length}, got {len(value)} element(s)'

        for v in value:
            if type_.sub_type == ArrayAttributoType.ARRAY_STRING:
                assert isinstance(v, str)
            elif type_.sub_type == ArrayAttributoType.ARRAY_BOOL:
                assert isinstance(v, bool)
            else:
                assert isinstance(v, (int, float))
    else:
        assert isinstance(
            type_, AttributoTypeDateTime
        ), f'expected "{value}" to be of datetime, is {type(value)}'
        assert isinstance(
            value, datetime.datetime
        ), f'attributo "{id_}": expected type datetime but got value {value}'


def check_attributo_types(
    types: dict[AttributoId, DBAttributo],
    d: UntypedAttributiMap,
) -> UntypedAttributiMap:
    for id_str, value in d.items():
        try:
            id_int = AttributoId(int(id_str))
        except:
            raise Exception(f'got non-numeric attributo ID "{id_str}"')
        type_ = types.get(id_int, None)
        if type_ is None:
            raise Exception(f'attributo "{id_int}" not found!')
        _check_type(id_int, type_.attributo_type, value)
    return d


def _convert_single_attributo_value_from_json(
    id_str: str,
    v: JSONValue,
    types: dict[AttributoId, DBAttributo],
) -> tuple[AttributoId, AttributoValue]:
    try:
        id_int = AttributoId(int(id_str))
    except:
        raise Exception(f'got a non-integer for the attributo id: "{id_str}"')
    attributo_type = types.get(id_int, None)
    if attributo_type is None:
        raise ValueError(
            f'cannot convert attributo with ID "{id_int}" from JSON, don\'t have a type! value is "{v}", attributi {types}'
        )
    return id_int, _convert_single_attributo_value_from_json_with_type(
        id_int, v, attributo_type.attributo_type
    )


def _convert_single_attributo_value_from_json_with_type(
    i: int,
    v: JSONValue,
    attributo_type: AttributoType,
) -> AttributoValue:
    if v is None:
        return None
    if isinstance(attributo_type, AttributoTypeChemical):
        assert isinstance(
            v, int
        ), f'expected type int for attributo "{i}", got {type(v)}'
        return v
    if isinstance(attributo_type, AttributoTypeBoolean):
        assert isinstance(
            v, bool
        ), f'expected type bool for attributo "{i}", got {type(v)}'
        return v
    if isinstance(attributo_type, AttributoTypeInt):
        assert isinstance(
            v, int
        ), f'expected type int for attributo "{i}", got {type(v)}'
        return v
    if isinstance(attributo_type, AttributoTypeString):
        assert isinstance(
            v, str
        ), f'expected type string for attributo "{i}", got {type(v)}'
        return v
    if isinstance(attributo_type, AttributoTypeDecimal):
        assert isinstance(
            v, (float, int)
        ), f'expected type float for attributo "{i}", got {type(v)}'
        if (
            attributo_type.range is not None
            and not attributo_type.range.value_is_inside(v)
        ):
            raise ValueError(
                f'value for attributo "{i}" is out of range; range is {attributo_type.range}, '
                + f"value is {v}"
            )
        return float(v)
    if isinstance(attributo_type, AttributoTypeDateTime):
        assert isinstance(
            v, int
        ), f'expected type int for datetime attributo "{i}", got {type(v)}'

        return datetime_from_attributo_int(v)

    if isinstance(attributo_type, AttributoTypeChoice):
        assert isinstance(
            v, str
        ), f'expected type str for choice attributo "{i}", got {type(v)}'
        # It's valid for a choice to be explicitly empty (e.g. not given)
        if v == "":
            return ""
        choices = attributo_type.values
        if v not in choices:
            choices_str = ", ".join(choices)
            raise ValueError(
                f'value for attributo "{i}" has to be one of {choices_str}, is "{v}"'
            )
        return v
    assert isinstance(attributo_type, AttributoTypeList)
    assert isinstance(
        v, list
    ), f'expected type list for list attributo "{i}", got {type(v)}'
    assert not v or isinstance(
        v[0], (float, str, int)
    ), f"got a non-empty list of {type(v[0])}, we only support float, int for now"
    lv = len(v)
    min_len = attributo_type.min_length
    max_len = attributo_type.max_length
    if min_len is not None and lv < min_len:
        raise ValueError(
            f"attributo {i}: the list needs at least {min_len} element(s), got {lv} element(s)"
        )
    if max_len is not None and lv > max_len:
        raise ValueError(
            f"attributo {i}: the list needs at least {max_len} element(s), got {lv} element(s)"
        )
    if attributo_type.sub_type == ArrayAttributoType.ARRAY_STRING:
        str_list: list[str] = []
        for sub_value in v:
            assert isinstance(sub_value, str)
            str_list.append(sub_value)
        return str_list
    if attributo_type.sub_type == ArrayAttributoType.ARRAY_BOOL:
        bool_list: list[bool] = []
        for sub_value in v:
            assert isinstance(sub_value, bool)
            bool_list.append(sub_value)
        return bool_list
    assert attributo_type.sub_type == ArrayAttributoType.ARRAY_NUMBER
    number_list: list[float] = []
    for sub_value in v:
        assert isinstance(sub_value, (int, float))
        number_list.append(sub_value)
    return number_list


def convert_single_attributo_value_to_json(value: AttributoValue) -> JSONValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime.datetime):
        return datetime_to_attributo_int(value)
    assert isinstance(value, list)
    if not value:
        # pyright complains that JSONValue and list[str] are not compatible
        return cast(list[str], [])  # pyright: ignore
    # I think the pyright: ignore here is wrong and we should ditch this test, but better
    # safe than sorry.
    if value[0] is None:  # pyright: ignore[reportUnnecessaryComparison]
        # Why should this be unreachable? [None] != []
        return None  # type: ignore
    assert isinstance(value[0], (str, int, float))
    # pyright complains that JSONValue and list[int] are not compatible
    return value  # pyright: ignore


class AttributiMap:
    def __init__(
        self,
        types_dict: dict[AttributoId, DBAttributo],
        impl: dict[AttributoId, AttributoValue],
    ) -> None:
        self._attributi = impl
        self._types = types_dict

    @staticmethod
    def from_types_and_raw(
        types: Iterable[DBAttributo],
        raw_attributi: UntypedAttributiMap,
    ) -> "AttributiMap":
        result = AttributiMap({a.id: a for a in types}, {})
        result.extend(raw_attributi)
        return result

    @staticmethod
    def from_types_and_json_dict(
        types: Iterable[DBAttributo], json_dict: JSONDict
    ) -> "AttributiMap":
        attributi: dict[AttributoId, AttributoValue] = {}
        types_dict = {a.id: a for a in types}
        for attributo_id_str, attributo_value in json_dict.items():
            id_, v = _convert_single_attributo_value_from_json(
                attributo_id_str,
                attributo_value,
                types_dict,
            )
            attributi[id_] = v

        types_dict = {a.id: a for a in types}
        return AttributiMap(types_dict, attributi)

    @staticmethod
    def from_types_and_value_rows(
        types: Iterable[DBAttributo],
        value_rows: list[tuple[AttributoId, AttributoValue]],
    ) -> "AttributiMap":
        attributi: dict[AttributoId, AttributoValue] = {a[0]: a[1] for a in value_rows}
        types_dict = {a.id: a for a in types}
        return AttributiMap(types_dict, attributi)

    def copy(self) -> "AttributiMap":
        return AttributiMap(
            types_dict=self._types.copy(),
            impl=self._attributi.copy(),
        )

    def select_int_unsafe(self, attributo_id: AttributoId) -> int:
        selected = self.select_unsafe(attributo_id)
        if not isinstance(selected, int):
            raise Exception(
                f"Attributo {attributo_id} is not an integer but {type(selected)}"
            )
        return selected

    def select_float_unsafe(self, attributo_id: AttributoId) -> float:
        selected = self.select_unsafe(attributo_id)
        if not isinstance(selected, (int, float)):
            raise Exception(
                f"Attributo {attributo_id} is not a decimal value but {type(selected)}"
            )
        return selected

    def select_bool_unsafe(self, attributo_id: AttributoId) -> bool:
        selected = self.select_unsafe(attributo_id)
        if not isinstance(selected, bool):
            raise Exception(
                f"Attributo {attributo_id} is not a boolean value but {type(selected)}"
            )
        return selected

    def select_datetime_unsafe(self, attributo_id: AttributoId) -> datetime.datetime:
        selected = self.select_unsafe(attributo_id)
        if not isinstance(selected, datetime.datetime):
            raise Exception(
                f"Attributo {attributo_id} is not a datetime but {type(selected)}"
            )
        return selected

    def select_unsafe(self, attributo_id: AttributoId) -> AttributoValue:
        selected = self.select(attributo_id)
        if selected is None:
            if attributo_id not in self._types:
                raise Exception(
                    f'Tried to retrieve "{attributo_id}", but I don\'t even know the attributo!'
                )
            raise Exception(
                f'Tried to retrieve "{attributo_id}" value, but didn\'t find the value!'
            )
        return selected

    def select_datetime(self, attributo_id: AttributoId) -> datetime.datetime | None:
        v = self.select(attributo_id)
        if v is None:
            return None
        assert isinstance(
            v, datetime.datetime
        ), f'expected datetime for attributo "{attributo_id}", got {type(v)}'
        return v

    def select_chemical_id(self, attributo_id: AttributoId) -> int | None:
        return self.select_int(attributo_id)

    def select_int(self, attributo_id: AttributoId) -> int | None:
        v = self.select(attributo_id)
        assert v is None or isinstance(
            v, int
        ), f"attributo {attributo_id} has type {type(v)} instead of int"
        return v if v is not None else None

    def select_decimal(self, attributo_id: AttributoId) -> float | None:
        v = self.select(attributo_id)
        assert v is None or isinstance(
            v, (float, int)
        ), f"attributo {attributo_id} has type {type(v)} instead of float"
        return float(v) if v is not None else None

    def select_string(self, attributo_id: AttributoId) -> str | None:
        v = self.select(attributo_id)
        assert v is None or isinstance(
            v, str
        ), f"attributo {attributo_id} has type {type(v)} instead of string"
        return v if v is not None else None

    def select(self, attributo_id: AttributoId) -> AttributoValue:
        return self._attributi.get(attributo_id, None)

    def select_with_type(
        self, attributo_id: AttributoId
    ) -> tuple[AttributoType, AttributoValue | None]:
        type_ = self._types.get(attributo_id, None)
        assert type_ is not None, f"couldn't find attributo {attributo_id}"
        value = self._attributi.get(attributo_id, None)
        return type_.attributo_type, value

    def extend(self, new_attributi: UntypedAttributiMap) -> None:
        int_map = check_attributo_types(self._types, new_attributi)
        for k, v in int_map.items():
            if v is not None:
                self._attributi[AttributoId(k)] = v

    def create_sub_map_for_group(self, group: str) -> "AttributiMap":
        attributi_in_group = {k.id for k in self._types.values() if k.group == group}
        return AttributiMap(
            self._types.copy(),
            {k: v for k, v in self._attributi.items() if k in attributi_in_group},
        )

    def extend_with_attributi_map(self, new_attributi: "AttributiMap") -> None:
        # no need to type-check here, it'd be duplicated
        for k, v in new_attributi.items():
            self._attributi[k] = v

    def append_single(self, attributo: AttributoId, value: AttributoValue) -> None:
        if value is not None:
            self.extend({attributo: value})

    def remove_but_keep_type(self, attributo: AttributoId) -> bool:
        previous = self._attributi.pop(attributo, None)
        return previous is not None

    def remove_with_type(self, attributo: AttributoId) -> bool:
        previous = self._attributi.pop(attributo, None)
        # remove from types as well, so we're consistent when typing
        if previous is not None:
            self._types.pop(attributo, None)
        return previous is not None

    def json_items(self) -> list[tuple[AttributoId, JSONValue]]:
        return [
            (attributo_id, convert_single_attributo_value_to_json(value))
            for attributo_id, value in self._attributi.items()
        ]

    def to_json(self) -> JSONDict:
        return {
            str(attributo_id): convert_single_attributo_value_to_json(value)
            for attributo_id, value in self._attributi.items()
        }

    def convert_attributo(
        self,
        conversion_flags: AttributoConversionFlags,
        id_: AttributoId,
        after_type: AttributoType,
    ) -> None:
        before_type = self._types[id_].attributo_type
        before_value = self._attributi.get(id_, None)
        after_value = convert_attributo_value(
            before_type, after_type, conversion_flags, before_value
        )
        # Change types
        self._types[id_] = replace(self._types[id_], attributo_type=after_type)

        self.append_single(id_, after_value)

    def ids(self) -> set[int]:
        return set(self._attributi.keys())

    def items(self) -> Iterable[tuple[AttributoId, AttributoValue]]:
        # noinspection PyTypeChecker,PydanticTypeChecker
        return self._attributi.items()

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, AttributiMap):
            return self._attributi == other._attributi
        return NotImplemented

    def __hash__(self) -> int:
        return hash(self._attributi)

    def __repr__(self) -> str:
        return self._attributi.__repr__()

    def retrieve_type(self, attributo_id: AttributoId) -> DBAttributo | None:
        return self._types.get(attributo_id)


def decimal_attributi_match(
    run_value_type: AttributoTypeDecimal,
    run_value: AttributoValue,
    data_set_value: AttributoValue,
) -> bool:
    if run_value is None and data_set_value is None:
        return True
    # They can't be both None at once, and if either is None, we fail our matching criterion
    if run_value is None or data_set_value is None:
        return False
    assert isinstance(
        run_value, (float, int)
    ), f"decimal run attributo is not int/float: {run_value}"
    assert isinstance(
        data_set_value, (float, int)
    ), f"decimal data set attributo is not int/float: {data_set_value}"
    if run_value_type.tolerance:
        if run_value_type.tolerance_is_absolute:
            if not math.isclose(
                float(run_value),
                float(data_set_value),
                abs_tol=run_value_type.tolerance,
            ):
                return False
        else:
            if not math.isclose(
                float(run_value),
                float(data_set_value),
                rel_tol=run_value_type.tolerance,
            ):
                return False
    else:
        # Use whatever math.isclose deems sensible for a float comparison.
        if not math.isclose(float(run_value), float(data_set_value)):
            return False
    return True


def run_matches_dataset(
    run_attributi: AttributiMap, data_set_attributi: AttributiMap
) -> bool:
    for name, data_set_value in data_set_attributi.items():
        run_value_type, run_value = run_attributi.select_with_type(name)
        if isinstance(data_set_value, bool):
            if run_value is None and data_set_value is False:
                continue
            if run_value is None and data_set_value is True:
                return False
        if isinstance(run_value_type, AttributoTypeDecimal):
            if not decimal_attributi_match(run_value_type, run_value, data_set_value):
                return False
        elif run_value != data_set_value:
            return False
    return True
