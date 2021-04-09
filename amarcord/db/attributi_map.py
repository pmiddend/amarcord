import datetime
from typing import Dict
from typing import Iterable
from typing import List
from typing import Optional
from typing import cast

from isodate import duration_isoformat
from isodate import parse_duration

from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeComments
from amarcord.db.attributo_type import AttributoTypeDateTime
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.attributo_type import AttributoTypeDuration
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.attributo_type import AttributoTypeList
from amarcord.db.attributo_type import AttributoTypeSample
from amarcord.db.attributo_type import AttributoTypeString
from amarcord.db.attributo_type import AttributoTypeTags
from amarcord.db.attributo_type import AttributoTypeUserName
from amarcord.db.attributo_value import AttributoValue
from amarcord.db.attributo_value_with_source import AttributoValueWithSource
from amarcord.db.comment import DBComment
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.karabo import Karabo
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.db.raw_attributi_map import Source
from amarcord.modules.json import JSONDict
from amarcord.modules.json import JSONValue
from amarcord.query_parser import Row

AttributiMapImpl = Dict[Source, Dict[AttributoId, AttributoValue]]


def _convert_single_attributo_value_from_json(
    i: AttributoId,
    v: JSONValue,
    types: Dict[AttributoId, DBAttributo],
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
        return v
    if isinstance(attributo_type.attributo_type, AttributoTypeInt):
        assert isinstance(
            v, int
        ), f'expected type int for attributo "{i}", got {type(v)}'
        if attributo_type.attributo_type.nonNegative and v < 0:
            raise ValueError(
                f'attributo "{i}" is supposed to be non-negative, but is {v}'
            )
        return v
    if isinstance(attributo_type.attributo_type, AttributoTypeString):
        assert isinstance(
            v, str
        ), f'expected type string for attributo "{i}", got {type(v)}'
        return v
    if isinstance(attributo_type.attributo_type, AttributoTypeDouble):
        assert isinstance(
            v, (float, int)
        ), f'expected type float for attributo "{i}", got {type(v)}'
        if (
            attributo_type.attributo_type.range is not None
            and not attributo_type.attributo_type.range.value_is_inside(v)
        ):
            raise ValueError(
                f'value for attributo "{i}" is out of range; range is {attributo_type.attributo_type.range}, value is {v}'
            )
        return float(v)
    if isinstance(attributo_type.attributo_type, AttributoTypeComments):
        raise Exception(f"cannot deserialize comments from JSON for attributo {i}")
    if isinstance(attributo_type.attributo_type, AttributoTypeDateTime):
        assert isinstance(
            v, str
        ), f'expected type string for datetime attributo "{i}", got {type(v)}'
        return datetime.datetime.fromisoformat(v)
    if isinstance(attributo_type.attributo_type, AttributoTypeDuration):
        assert isinstance(
            v, str
        ), f'expected type string for duration attributo "{i}", got {type(v)}'
        return parse_duration(v)
    if isinstance(attributo_type.attributo_type, AttributoTypeTags):
        assert isinstance(
            v, list
        ), f'expected type list for duration attributo "{i}", got {type(v)}'
        if not v:
            return cast(List[str], [])
        first = v[0]
        assert isinstance(
            first, str
        ), f'expected type list of strings for duration attributo "{i}", got {type(first)}'
        return v
    if isinstance(attributo_type.attributo_type, AttributoTypeChoice):
        assert isinstance(
            v, str
        ), f'expected type str for choice attributo "{i}", got {type(v)}'
        choices = [v[1] for v in attributo_type.attributo_type.values]
        if v not in choices:
            choices_str = ", ".join(choices)
            raise ValueError(
                f'value for attributo "{i}" has to be one of {choices_str}, is "{v}"'
            )
        return v
    if isinstance(attributo_type.attributo_type, AttributoTypeUserName):
        assert isinstance(
            v, str
        ), f'expected type str for user name attributo "{i}", got {type(v)}'
        return v
    if isinstance(attributo_type.attributo_type, AttributoTypeList):
        assert isinstance(
            v, list
        ), f'expected type list for list attributo "{i}", got {type(v)}'
        assert not v or isinstance(
            v[0], (float, str)
        ), f"got a non-empty list of {type(v[0])}, we only support float, int for now"
        return v
    raise Exception(
        f'invalid property type for attributo "{i}": {attributo_type.attributo_type}'
    )


class AttributiMap:
    def __init__(
        self,
        types: Dict[AttributoId, DBAttributo],
        raw_attributi: Optional[RawAttributiMap] = None,
    ) -> None:
        self._attributi: AttributiMapImpl = {}
        if raw_attributi is not None:
            for source, source_attributi in raw_attributi.items():
                self._attributi[source] = {
                    AttributoId(k): _convert_single_attributo_value_from_json(
                        AttributoId(k), v, types
                    )
                    for k, v in source_attributi.items()
                }

    def has_manual_value(self, attributo_id: AttributoId) -> bool:
        manual_attributi = self._attributi.get(MANUAL_SOURCE_NAME, None)
        if manual_attributi is None:
            return False
        return attributo_id in manual_attributi

    def select_int_unsafe(self, attributo_id: AttributoId) -> int:
        selected = self.select_unsafe(attributo_id)
        if not isinstance(selected.value, int):
            raise Exception(
                f"Attributo {attributo_id} is not an integer but {type(selected.value)}"
            )
        return selected.value

    def select_comments_unsafe(self, attributo_id: AttributoId) -> List[DBComment]:
        selected = self.select_unsafe(attributo_id)
        if (
            not isinstance(selected.value, list)
            or selected.value
            and not isinstance(selected.value[0], DBComment)
        ):
            raise Exception(
                f"Attributo {attributo_id} are not comments but {type(selected.value)}"
            )
        return selected.value  # type: ignore

    def select_karabo(self, attributo_id: AttributoId) -> Optional[Karabo]:
        selected = self.select_unsafe(attributo_id)
        # noinspection PyTypeChecker
        return selected.value if selected is not None else None  # type: ignore

    def select_unsafe(self, attributo_id: AttributoId) -> AttributoValueWithSource:
        selected = self.select(attributo_id)
        if selected is None:
            raise Exception(f'Tried to retrieve "{attributo_id}", but didn\'t find it!')
        return selected

    def select_value(self, attributo_id: AttributoId) -> Optional[AttributoValue]:
        v = self.select(attributo_id)
        return v.value if v is not None else None

    def select_datetime(self, attributo_id: AttributoId) -> Optional[datetime.datetime]:
        v = self.select(attributo_id)
        if v is None:
            return None
        assert isinstance(
            v.value, datetime.datetime
        ), f'expected datetime for attributo "{attributo_id}", got {type(v)}'
        return v.value

    def select_int(self, attributo_id: AttributoId) -> Optional[int]:
        v = self.select_value(attributo_id)
        assert v is None or isinstance(
            v, int
        ), f"attributo {attributo_id} has type {type(v)} instead of int"
        return v if v is not None else None

    def select(self, attributo_id: AttributoId) -> Optional[AttributoValueWithSource]:
        manual_attributi = self._attributi.get(MANUAL_SOURCE_NAME, None)

        if manual_attributi is not None:
            manual_attributo = manual_attributi.get(attributo_id, None)
            if manual_attributo:
                return AttributoValueWithSource(manual_attributo, MANUAL_SOURCE_NAME)

        for source, values in self._attributi.items():
            assert isinstance(values, dict)
            attributo = values.get(attributo_id, None)
            if attributo is not None:
                return AttributoValueWithSource(attributo, source)

        return None

    def append_to_source(
        self, source: Source, new_attributi: Dict[AttributoId, AttributoValue]
    ) -> None:
        # TODO: check proper types here
        source_value = self._attributi.get(source, None)

        if source_value is None:
            self._attributi[source] = new_attributi
        else:
            source_value.update(new_attributi)

    def append_single_to_source(
        self, source: Source, attributo: AttributoId, value: AttributoValue
    ) -> None:
        self.append_to_source(source, {attributo: value})

    def remove_manual(self, attributo: AttributoId) -> None:
        manual_attributi = self._attributi.get(MANUAL_SOURCE_NAME, None)
        if manual_attributi is None:
            return
        manual_attributi.pop(attributo, None)

    def set_single_manual(self, attributo: AttributoId, value: AttributoValue) -> None:
        # TODO: check proper types here
        self.append_single_to_source(MANUAL_SOURCE_NAME, attributo, value)

    def to_raw(self) -> RawAttributiMap:
        json_dict: JSONDict = {}
        for source, values in self._attributi.items():
            source_dict: JSONDict = {}
            for attributo_id, value in values.items():
                if value is None or isinstance(value, (str, int, float, bool)):
                    source_dict[attributo_id] = value
                if isinstance(value, datetime.datetime):
                    source_dict[attributo_id] = value.isoformat()
                if isinstance(value, datetime.timedelta):
                    source_dict[attributo_id] = duration_isoformat(value)
                if isinstance(value, list):
                    if not value:
                        source_dict[attributo_id] = cast(List[str], [])
                    else:
                        if isinstance(value[0], (str, int, float)):
                            source_dict[attributo_id] = value
            json_dict[source] = source_dict
        return RawAttributiMap(json_dict)

    def to_query_row(self, attributi_ids: Iterable[AttributoId], prefix: str) -> Row:
        result: Row = {}
        for attributo_id in attributi_ids:
            v = self.select_value(attributo_id)
            if isinstance(v, list) and v and isinstance(v[0], DBComment):
                result[prefix + attributo_id] = "".join(
                    cast(DBComment, c).text + cast(DBComment, c).author for c in v
                )
            else:
                result[prefix + attributo_id] = self.select_value(attributo_id)  # type: ignore
        return result

    def remove_attributo(self, attributo_id: AttributoId) -> bool:
        existed = False
        for v in self._attributi.values():
            if v.pop(attributo_id, None) is not None:
                existed = True
        return existed

    def values_per_source(
        self, attributo_id: AttributoId
    ) -> Dict[Source, AttributoValue]:
        result: Dict[Source, AttributoValue] = {}
        for source, attributi in self._attributi.items():
            a = attributi.get(attributo_id, None)
            if a is not None:
                result[source] = a
        return result
