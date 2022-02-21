import datetime
from dataclasses import replace
from typing import Dict, Set
from typing import List
from typing import Optional
from typing import cast

from amarcord.db.attributi import (
    convert_attributo_value,
    AttributoConversionFlags,
    datetime_to_attributo_int,
    datetime_from_attributo_int,
)
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
from amarcord.db.comment import DBComment
from amarcord.db.dbattributo import DBAttributo
from amarcord.json import JSONValue

JsonAttributiMap = Dict[str, JSONValue]

UntypedAttributiMap = Dict[str, AttributoValue]


def _check_type(
    name: str, sample_ids: List[int], type_: AttributoType, value: AttributoValue
) -> None:
    if value is None:
        return
    if isinstance(type_, AttributoTypeSample):
        if not isinstance(value, int):
            raise Exception(
                f'attributo "{name}": should have type int, but got value {value}'
            )
        if value not in sample_ids:
            raise Exception(f'attributo "{name}": invalid sample ID {value}')
    if isinstance(type_, (AttributoTypeInt, AttributoTypeSample)):
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
            value == "" or value in type_.values
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
            _check_type(name + f"[{i}]", sample_ids, type_.sub_type, v)  # type: ignore
    elif isinstance(type, AttributoTypeDateTime):
        assert isinstance(
            value, int
        ), f'attributo "{name}": expected type int but got value {value}'


def _check_types(
    types: Dict[AttributoId, DBAttributo],
    sample_ids: List[int],
    d: Dict[AttributoId, AttributoValue],
) -> None:
    for name, value in d.items():
        type_ = types.get(name, None)
        if type_ is None:
            raise Exception(f'attributo "{name}" not found!')
        _check_type(name, sample_ids, type_.attributo_type, value)


def _convert_single_attributo_value_from_json(
    i: AttributoId,
    v: JSONValue,
    types: Dict[AttributoId, DBAttributo],
    sample_ids: List[int],
) -> AttributoValue:
    attributo_type = types.get(i, None)
    if attributo_type is None:
        raise ValueError(
            f'cannot convert attributo "{i}" from JSON, don\'t have a type! value is "{v}"'
        )

    if v is None:
        return None
    if isinstance(attributo_type.attributo_type, AttributoTypeSample):
        assert isinstance(
            v, int
        ), f'expected type int for attributo "{i}", got {type(v)}'
        if v == 0:
            # special case: 0 is the "no sample ID" sample ID
            return None
        if v not in sample_ids:
            raise Exception(f"{v} is not a valid sample ID")
        return v
    if isinstance(attributo_type.attributo_type, AttributoTypeBoolean):
        assert isinstance(
            v, bool
        ), f'expected type bool for attributo "{i}", got {type(v)}'
        return v
    if isinstance(attributo_type.attributo_type, AttributoTypeInt):
        assert isinstance(
            v, int
        ), f'expected type int for attributo "{i}", got {type(v)}'
        return v
    if isinstance(attributo_type.attributo_type, AttributoTypeString):
        assert isinstance(
            v, str
        ), f'expected type string for attributo "{i}", got {type(v)}'
        return v
    if isinstance(attributo_type.attributo_type, AttributoTypeDecimal):
        assert isinstance(
            v, (float, int)
        ), f'expected type float for attributo "{i}", got {type(v)}'
        if (
            attributo_type.attributo_type.range is not None
            and not attributo_type.attributo_type.range.value_is_inside(v)
        ):
            raise ValueError(
                f'value for attributo "{i}" is out of range; range is {attributo_type.attributo_type.range}, '
                f"value is {v}"
            )
        return float(v)
    if isinstance(attributo_type.attributo_type, AttributoTypeDateTime):
        assert isinstance(
            v, int
        ), f'expected type int for datetime attributo "{i}", got {type(v)}'

        return datetime_from_attributo_int(v)

    if isinstance(attributo_type.attributo_type, AttributoTypeChoice):
        assert isinstance(
            v, str
        ), f'expected type str for choice attributo "{i}", got {type(v)}'
        # It's valid for a choice to be explicitly empty (e.g. not given)
        if v == "":
            return None
        choices = attributo_type.attributo_type.values
        if v not in choices:
            choices_str = ", ".join(choices)
            raise ValueError(
                f'value for attributo "{i}" has to be one of {choices_str}, is "{v}"'
            )
        return v
    if isinstance(attributo_type.attributo_type, AttributoTypeList):
        assert isinstance(
            v, list
        ), f'expected type list for list attributo "{i}", got {type(v)}'
        assert not v or isinstance(
            v[0], (float, str, int)
        ), f"got a non-empty list of {type(v[0])}, we only support float, int for now"
        return v
    raise Exception(
        f'invalid property type for attributo "{i}": {attributo_type.attributo_type}'
    )


class AttributiMap:
    def __init__(
        self,
        types_dict: Dict[str, DBAttributo],
        sample_ids: List[int],
        impl: UntypedAttributiMap,
    ) -> None:
        self._attributi = impl
        self._types = types_dict
        self._sample_ids = sample_ids

    @staticmethod
    def from_types_and_json(
        types: List[DBAttributo], sample_ids: List[int], raw_attributi: JsonAttributiMap
    ) -> "AttributiMap":
        attributi: UntypedAttributiMap = {}
        types_dict = {a.name: a for a in types}
        if raw_attributi is not None:
            for attributo_name, attributo_value in raw_attributi.items():
                v = _convert_single_attributo_value_from_json(
                    attributo_name, attributo_value, types_dict, sample_ids
                )
                if v is not None:
                    attributi[attributo_name] = v

        return AttributiMap(types_dict, sample_ids, attributi)

    def copy(self) -> "AttributiMap":
        return AttributiMap(
            types_dict=self._types.copy(),
            sample_ids=self._sample_ids.copy(),
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

    def select_comments_unsafe(self, attributo_id: AttributoId) -> List[DBComment]:
        selected = self.select_unsafe(attributo_id)
        if (
            not isinstance(selected, list)
            or selected
            and not isinstance(selected[0], DBComment)
        ):
            raise Exception(
                f"Attributo {attributo_id} are not comments but {type(selected)}"
            )
        return selected  # type: ignore

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

    def select_datetime(self, attributo_id: AttributoId) -> Optional[datetime.datetime]:
        v = self.select(attributo_id)
        if v is None:
            return None
        assert isinstance(
            v, datetime.datetime
        ), f'expected datetime for attributo "{attributo_id}", got {type(v)}'
        return v

    def select_sample_id(self, attributo_id: AttributoId) -> Optional[int]:
        return self.select_int(attributo_id)

    def select_int(self, attributo_id: AttributoId) -> Optional[int]:
        v = self.select(attributo_id)
        assert v is None or isinstance(
            v, int
        ), f"attributo {attributo_id} has type {type(v)} instead of int"
        return v if v is not None else None

    def select_string(self, attributo_id: AttributoId) -> Optional[str]:
        v = self.select(attributo_id)
        assert v is None or isinstance(
            v, str
        ), f"attributo {attributo_id} has type {type(v)} instead of string"
        return v if v is not None else None

    def select(self, attributo_id: AttributoId) -> AttributoValue:
        return self._attributi.get(attributo_id, None)

    def extend(self, new_attributi: Dict[AttributoId, AttributoValue]) -> None:
        _check_types(self._types, self._sample_ids, new_attributi)
        for k, v in new_attributi.items():
            if v is not None:
                self._attributi[k] = v

    def append_single(self, attributo: AttributoId, value: AttributoValue) -> None:
        if value is not None:
            self.extend({attributo: value})

    def remove(self, attributo: AttributoId) -> bool:
        previous = self._attributi.pop(attributo, None)
        # remove from types as well, so we're consistent when typing
        if previous is not None:
            self._types.pop(attributo, None)
        return previous is not None

    def to_json(self) -> JsonAttributiMap:
        json_dict: Dict[str, JSONValue] = {}
        for attributo_id, value in self._attributi.items():
            if value is None or isinstance(value, (str, int, float, bool)):
                json_dict[attributo_id] = value
            if isinstance(value, datetime.datetime):
                json_dict[attributo_id] = datetime_to_attributo_int(value)
            if isinstance(value, list):
                if not value:
                    json_dict[attributo_id] = cast(List[str], [])
                else:
                    if isinstance(value[0], (str, int, float)):
                        json_dict[attributo_id] = value
        return json_dict

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
            self.remove(old_name)
            self.append_single(new_name, after_value)

    def names(self) -> Set[str]:
        return set(self._attributi.keys())
