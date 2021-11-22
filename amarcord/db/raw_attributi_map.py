import datetime
from copy import deepcopy
from dataclasses import dataclass
from typing import Any
from typing import Dict
from typing import ItemsView
from typing import Optional

from amarcord.db.attributo_id import AttributoId
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.modules.json import JSONArray
from amarcord.modules.json import JSONDict
from amarcord.modules.json import JSONValue

Source = str


@dataclass(frozen=True)
class RawAttributoValueWithSource:
    value: JSONValue
    source: Source


class RawAttributiMap:
    def __init__(self, db_column: JSONDict) -> None:
        self._sources: Dict[Source, JSONDict] = {}
        for k, v in db_column.items():
            assert isinstance(v, dict)
            self._sources[k] = v

    def __eq__(self, other: Any) -> bool:
        if isinstance(other, self.__class__):
            return self._sources == other._sources
        return False

    def __ne__(self, other: Any) -> bool:
        return not self.__eq__(other)

    def copy(self) -> "RawAttributiMap":
        return RawAttributiMap(deepcopy(self._sources))  # type: ignore

    def items(self) -> ItemsView[Source, JSONDict]:
        return self._sources.items()

    def select_int_unsafe(self, attributo_id: AttributoId) -> int:
        selected = self.select_unsafe(attributo_id)
        if not isinstance(selected.value, int):
            raise Exception(
                f"Attributo {attributo_id} is not an integer but {type(selected.value)}"
            )
        return selected.value

    def select_unsafe(self, attributo_id: AttributoId) -> RawAttributoValueWithSource:
        selected = self.select(attributo_id)
        if selected is None:
            raise Exception(
                f'Tried to retrieve "{attributo_id}", but didn\'t find it! JSON value is: {self.to_json()}'
            )
        return selected

    def select_value(self, attributo_id: AttributoId) -> Optional[JSONValue]:
        v = self.select(attributo_id)
        return v.value if v is not None else None

    def select_int(self, attributo_id: AttributoId) -> Optional[int]:
        v = self.select_value(attributo_id)
        assert v is None or isinstance(
            v, int
        ), f"attributo {attributo_id} has type {type(v)} instead of int"
        return v if v is not None else None

    def select(
        self, attributo_id: AttributoId
    ) -> Optional[RawAttributoValueWithSource]:
        manual_attributi = self._sources.get(MANUAL_SOURCE_NAME, None)

        assert manual_attributi is None or isinstance(manual_attributi, dict)

        if manual_attributi is not None:
            manual_attributo = manual_attributi.get(attributo_id, None)
            if manual_attributo:
                return RawAttributoValueWithSource(manual_attributo, MANUAL_SOURCE_NAME)

        for source, values in self._sources.items():
            assert isinstance(values, dict)
            attributo = values.get(attributo_id, None)
            if attributo is not None:
                return RawAttributoValueWithSource(attributo, source)

        return None

    def append_to_source(
        self, source: Source, new_attributi: Dict[AttributoId, JSONValue]
    ) -> None:
        source_value = self._sources.get(source, None)

        assert source_value is None or isinstance(source_value, dict)

        if source_value is None:
            self._sources[source] = new_attributi  # type: ignore
        else:
            source_value.update(new_attributi)  # type: ignore

    def append_single_datetime_to_source(
        self, source: Source, attributo: AttributoId, value: datetime.datetime
    ) -> None:
        self.append_single_to_source(source, attributo, value.isoformat())

    def append_single_to_source(
        self, source: Source, attributo: AttributoId, value: JSONValue
    ) -> None:
        assert value is not None
        if not isinstance(value, (int, str, float, bool, dict, list)):
            raise ValueError(
                f"attributo {attributo} can only hold JSON-compatible values, got {type(value)}"
            )
        self.append_to_source(source, {attributo: value})

    def set_single_manual(self, attributo: AttributoId, value: JSONValue) -> None:
        self.append_single_to_source(MANUAL_SOURCE_NAME, attributo, value)

    def to_json(self) -> JSONDict:
        return self._sources  # type: ignore

    def to_json_array(self, table: str) -> JSONArray:
        return [
            {
                "table": table,
                "source": source,
                "name": attributo_id,
                "value": attributo_value,
            }
            for source, values in self._sources.items()
            for attributo_id, attributo_value in values.items()
        ]

    def remove_attributo(
        self, attributo_id: AttributoId, source: Optional[str]
    ) -> bool:
        if source is not None:
            if source not in self._sources:
                return False
            return self._sources[source].pop(attributo_id, None) is not None
        existed = False
        for v in self._sources.values():
            assert v is None or isinstance(v, dict)
            if v is not None and v.pop(attributo_id, None) is not None:
                existed = True
        return existed

    def remove_manual_attributo(self, attributo_id: AttributoId) -> bool:
        return self.remove_attributo(attributo_id, MANUAL_SOURCE_NAME)
