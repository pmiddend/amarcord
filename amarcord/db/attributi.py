import datetime
import logging
from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Mapping, Optional, Tuple, Union, cast

from PyQt5 import QtCore, QtWidgets

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.karabo import Karabo

from amarcord.json_schema import (
    JSONSchemaArray,
    JSONSchemaInteger,
    JSONSchemaNumber,
    JSONSchemaString,
    JSONSchemaStringFormat,
    parse_schema_type,
)
from amarcord.modules.json import JSONDict
from amarcord.qt.numeric_range_format_widget import NumericRange
from amarcord.qt.table_delegates import (
    ComboItemDelegate,
    DateTimeItemDelegate,
    DoubleItemDelegate,
    IntItemDelegate,
    TagsItemDelegate,
)
from amarcord.query_parser import Row

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class PropertyInt:
    nonNegative: bool = False
    range: Optional[Tuple[int, int]] = None


@dataclass(frozen=True)
class PropertyString:
    pass


@dataclass(frozen=True)
class PropertyComments:
    pass


@dataclass(frozen=True)
class PropertyDouble:
    range: Optional[NumericRange] = None
    suffix: Optional[str] = None


@dataclass(frozen=True)
class PropertyTags:
    pass


@dataclass(frozen=True)
class PropertySample:
    pass


@dataclass(frozen=True)
class PropertyDateTime:
    pass


@dataclass(frozen=True)
class PropertyChoice:
    values: List[Tuple[str, Any]]


RichAttributoType = Union[
    PropertyInt,
    PropertyChoice,
    PropertyDouble,
    PropertyTags,
    PropertySample,
    PropertyString,
    PropertyComments,
    PropertyDateTime,
]


def delegate_for_property_type(
    proptype: RichAttributoType,
    sample_ids: List[int],
    parent: Optional[QtCore.QObject] = None,
) -> QtWidgets.QAbstractItemDelegate:
    if isinstance(proptype, PropertyInt):
        return IntItemDelegate(proptype.nonNegative, proptype.range, parent)
    if isinstance(proptype, PropertyDouble):
        return DoubleItemDelegate(proptype.range, proptype.suffix, parent)
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


def schema_to_property_type(
    json_schema: JSONDict, suffix: Optional[str]
) -> RichAttributoType:
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
                parsed_schema.exclusiveMinimum is not None,
                parsed_schema.maximum
                if parsed_schema.maximum is not None
                else parsed_schema.exclusiveMaximum,
                parsed_schema.exclusiveMaximum is not None,
            ),
            suffix=suffix,
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
        return PropertyString()
    raise Exception(f'invalid schema type "{type(parsed_schema)}"')


def property_type_to_schema(rp: RichAttributoType) -> JSONDict:
    if isinstance(rp, PropertyInt):
        result_int: Dict[str, Any] = {"type": "number"}
        if rp.range is not None:
            result_int["minimum"] = rp.range[0]
            result_int["maximum"] = rp.range[1]
        return result_int
    if isinstance(rp, PropertyDouble):
        result_double: Dict[str, Any] = {"type": "number"}
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
        return result_double
    if isinstance(rp, PropertyString):
        return {"type": "string"}
    if isinstance(rp, PropertySample):
        return {"type": "integer"}
    if isinstance(rp, PropertyChoice):
        return {"type": "string", "enum": [v[1] for v in rp.values]}
    if isinstance(rp, PropertyDateTime):
        return {"type": "string", "format": "date-time"}
    if isinstance(rp, PropertyTags):
        return {"type": "array", "items": {"type": "string"}}
    raise Exception(f"invalid property type {type(rp)}")


@dataclass(frozen=True)
class DBAttributo:
    name: AttributoId
    description: str
    suffix: Optional[str]
    associated_table: AssociatedTable
    rich_property_type: RichAttributoType


def pretty_print_attributo(
    attributo_metadata: Optional[DBAttributo], value: Any
) -> str:
    if attributo_metadata is not None and isinstance(
        attributo_metadata.rich_property_type, PropertyComments
    ):
        assert isinstance(value, list), f"Comment column isn't a list but {type(value)}"
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


@dataclass(frozen=True)
class DBRunComment:
    id: Optional[int]
    run_id: int
    author: str
    text: str
    created: datetime.datetime


AttributoValue = Union[List[DBRunComment], str, int, float, List[str]]
Source = str


@dataclass(frozen=True)
class AttributoValueWithSource:
    value: AttributoValue
    source: Source


AttributiMapImpl = Dict[Source, Dict[AttributoId, AttributoValue]]


class AttributiMap:
    def __init__(self, db_column: Mapping[str, Any]) -> None:
        self._attributi: AttributiMapImpl = {}
        for k, v in db_column.items():
            assert isinstance(v, dict)
            self._attributi[AttributoId(k)] = v

    def select_int_unsafe(self, attributo_id: AttributoId) -> int:
        selected = self.select_unsafe(attributo_id)
        if not isinstance(selected.value, int):
            raise Exception(
                f"Attributo {attributo_id} is not an integer but {type(selected.value)}"
            )
        return selected.value

    def select_comments_unsafe(self, attributo_id: AttributoId) -> List[DBRunComment]:
        selected = self.select_unsafe(attributo_id)
        if (
            not isinstance(selected.value, list)
            or selected.value
            and not isinstance(selected.value[0], DBRunComment)
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
            raise Exception(
                f'Tried to retrieve attributo "{attributo_id}", but didn\'t find it! Complete JSON value is: {self.to_json()}'
            )
        return selected

    def select_value(self, attributo_id: AttributoId) -> Optional[AttributoValue]:
        v = self.select(attributo_id)
        return v.value if v is not None else None

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
        source_value = self._attributi.get(source, None)

        if source_value is None:
            self._attributi[source] = new_attributi
        else:
            source_value.update(new_attributi)

    def append_single_to_source(
        self, source: Source, attributo: AttributoId, value: AttributoValue
    ) -> None:
        self.append_to_source(source, {attributo: value})

    def set_single_manual(self, attributo: AttributoId, value: AttributoValue) -> None:
        self.append_single_to_source(MANUAL_SOURCE_NAME, attributo, value)

    def to_json(self) -> JSONDict:
        return self._attributi

    def to_query_row(self, attributi_metadata: Iterable[AttributoId]) -> Row:
        result: Row = {}
        for metadata in attributi_metadata:
            v = self.select_value(metadata)
            if isinstance(v, list) and v and isinstance(v[0], DBRunComment):
                result[metadata] = "".join(
                    cast(DBRunComment, c).text + cast(DBRunComment, c).author for c in v
                )
            else:
                result[metadata] = self.select_value(metadata)  # type: ignore
        return result
