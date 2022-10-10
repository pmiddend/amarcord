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
from amarcord.json_types import JSONValue

SPECIAL_VALUE_CHOICE_NONE = ""

SPECIAL_CHEMICAL_ID_NONE = 0

JsonAttributiMap = dict[str, JSONValue]

UntypedAttributiMap = dict[AttributoId, AttributoValue]


def _check_type(
    name: str, chemical_ids: list[int], type_: AttributoType, value: AttributoValue
) -> None:
    if value is None:
        return
    if isinstance(type_, AttributoTypeChemical):
        if not isinstance(value, int):
            raise Exception(
                f'attributo "{name}": should have type int, but got value {value}'
            )
        if value != SPECIAL_CHEMICAL_ID_NONE and value not in chemical_ids:
            raise Exception(f'attributo "{name}": invalid chemical ID {value}')
    if isinstance(type_, (AttributoTypeInt, AttributoTypeChemical)):
        if not isinstance(value, int):
            raise Exception(
                f'attributo "{name}": should have type int, but got value {value}'
            )
    elif isinstance(type_, AttributoTypeString):
        if not isinstance(value, str):
            raise Exception(
                f'attributo "{name}": should have type string, but got value {value}'
            )
    elif isinstance(type_, AttributoTypeBoolean):
        if not isinstance(value, bool):
            raise Exception(
                f'attributo "{name}": should have type bool, but got value {value}'
            )
    elif isinstance(type_, AttributoTypeDecimal):
        assert isinstance(
            value, (float, int)
        ), f'attributo "{name}": expected type float but got value {value}'
        if type_.range is not None and not type_.range.value_is_inside(value):
            raise ValueError(
                f'attributo "{name}": value is out of range; range is {type_.range}, '
                f"value is {value}"
            )
    elif isinstance(type_, AttributoTypeChoice):
        assert isinstance(
            value, str
        ), f'attributo "{name}": expected type string but got value {value}'
        assert (
            value == SPECIAL_VALUE_CHOICE_NONE or value in type_.values
        ), f'attributo "{name}": value "{value}" not one of ' + ", ".join(type_.values)
    elif isinstance(type_, AttributoTypeList):
        assert isinstance(
            value, list
        ), f'attributo "{name}": expected type list but got value {value}'
        assert (
            type_.min_length is None or len(value) >= type_.min_length
        ), f'attributo "{name}": list min length is {type_.min_length}, got {len(value)} element(s)'
        assert (
            type_.max_length is None or len(value) <= type_.max_length
        ), f'attributo "{name}": list max length is {type_.min_length}, got {len(value)} element(s)'

        for i, v in enumerate(value):
            _check_type(name + f"[{i}]", chemical_ids, type_.sub_type, v)  # type: ignore
    elif isinstance(type_, AttributoTypeDateTime):
        assert isinstance(
            value, datetime.datetime
        ), f'attributo "{name}": expected type datetime but got value {value}'


def check_attributo_types(
    types: dict[AttributoId, DBAttributo],
    chemical_ids: list[int],
    d: UntypedAttributiMap,
) -> None:
    for name, value in d.items():
        type_ = types.get(name, None)
        if type_ is None:
            raise Exception(f'attributo "{name}" not found!')
        _check_type(name, chemical_ids, type_.attributo_type, value)


def _convert_single_attributo_value_from_json(
    i: AttributoId,
    v: JSONValue,
    types: dict[AttributoId, DBAttributo],
    chemical_ids: list[int],
) -> AttributoValue:
    attributo_type = types.get(i, None)
    if attributo_type is None:
        raise ValueError(
            f'cannot convert attributo "{i}" from JSON, don\'t have a type! value is "{v}"'
        )
    return _convert_single_attributo_value_from_json_with_type(
        i, v, attributo_type.attributo_type, chemical_ids
    )


def _convert_single_attributo_value_from_json_with_type(
    i: AttributoId,
    v: JSONValue,
    attributo_type: AttributoType,
    chemical_ids: list[int],
) -> AttributoValue:
    if v is None:
        return None
    if isinstance(attributo_type, AttributoTypeChemical):
        assert isinstance(
            v, int
        ), f'expected type int for attributo "{i}", got {type(v)}'
        if v != SPECIAL_CHEMICAL_ID_NONE and v not in chemical_ids:
            raise Exception(f"{v} is not a valid chemical ID")
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
                f"value is {v}"
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
    if isinstance(attributo_type, AttributoTypeList):
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
        return [  # type: ignore
            _convert_single_attributo_value_from_json_with_type(
                i, sub_value, attributo_type.sub_type, chemical_ids
            )
            for sub_value in v
        ]
    raise Exception(f'invalid property type for attributo "{i}": {attributo_type}')


def convert_single_attributo_value_to_json(value: AttributoValue) -> JSONValue:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, datetime.datetime):
        return datetime_to_attributo_int(value)
    if isinstance(value, list):
        if not value:
            return cast(list[str], [])
        if value[0] is None:
            # Why should this be unreachable? [None] != []
            return None  # type: ignore
        if isinstance(value[0], (str, int, float)):
            return value
        raise Exception(
            f"invalid attributo value type: list of type {type(value[0])} (value {value[0]}); can only handle str, int, float right now"
        )
    raise Exception(f"invalid attributo value type: {type(value)}: {value}")


class AttributiMap:
    def __init__(
        self,
        types_dict: dict[AttributoId, DBAttributo],
        chemical_ids: list[int],
        impl: UntypedAttributiMap,
    ) -> None:
        self._attributi = impl
        self._types = types_dict
        self._chemical_ids = chemical_ids

    @staticmethod
    def from_types_and_raw(
        types: Iterable[DBAttributo],
        chemical_ids: list[int],
        raw_attributi: UntypedAttributiMap,
    ) -> "AttributiMap":
        result = AttributiMap({a.name: a for a in types}, chemical_ids, {})
        result.extend(raw_attributi)
        return result

    @staticmethod
    def from_types_and_json(
        types: Iterable[DBAttributo],
        chemical_ids: list[int],
        raw_attributi: JsonAttributiMap,
    ) -> "AttributiMap":
        attributi: UntypedAttributiMap = {}
        types_dict = {a.name: a for a in types}
        if raw_attributi is not None:
            for attributo_name, attributo_value in raw_attributi.items():
                v = _convert_single_attributo_value_from_json(
                    AttributoId(attributo_name),
                    attributo_value,
                    types_dict,
                    chemical_ids,
                )
                attributi[AttributoId(attributo_name)] = v

        return AttributiMap(types_dict, chemical_ids, attributi)

    def copy(self) -> "AttributiMap":
        return AttributiMap(
            types_dict=self._types.copy(),
            chemical_ids=self._chemical_ids.copy(),
            impl=self._attributi.copy(),
        )

    def select_int_unsafe(self, attributo_id: AttributoId) -> int:
        selected = self.select_unsafe(attributo_id)
        if not isinstance(selected, int):
            raise Exception(
                f"Attributo {attributo_id} is not an integer but {type(selected)}"
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
        check_attributo_types(self._types, self._chemical_ids, new_attributi)
        for k, v in new_attributi.items():
            if v is not None:
                self._attributi[k] = v

    def create_sub_map_for_group(self, group: str) -> "AttributiMap":
        attributi_in_group = {k.name for k in self._types.values() if k.group == group}
        return AttributiMap(
            self._types.copy(),
            self._chemical_ids.copy(),
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

    def to_json(self) -> JsonAttributiMap:
        return {
            attributo_id: convert_single_attributo_value_to_json(value)
            for attributo_id, value in self._attributi.items()
        }

    def convert_attributo(
        self,
        conversion_flags: AttributoConversionFlags,
        old_name: AttributoId,
        new_name: AttributoId,
        after_type: AttributoType,
    ) -> None:
        before_type = self._types[old_name].attributo_type
        before_value = self._attributi.get(old_name, None)
        after_value = convert_attributo_value(
            before_type, after_type, conversion_flags, before_value
        )
        # Change types
        self._types[new_name] = replace(
            self._types[old_name], attributo_type=after_type
        )
        if new_name != old_name:
            del self._types[old_name]

        # Change value
        if new_name == old_name:
            self.append_single(old_name, after_value)
        else:
            self.remove_with_type(old_name)
            self.append_single(new_name, after_value)

    def names(self) -> set[str]:
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
