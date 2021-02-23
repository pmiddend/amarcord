from dataclasses import dataclass
from typing import List, Optional, Tuple, TypeVar, Union

from PyQt5 import QtCore, QtWidgets

from amarcord.qt.table_delegates import (
    ComboItemDelegate,
    DateTimeItemDelegate,
    DoubleItemDelegate,
    IntItemDelegate,
    TagsItemDelegate,
)

T = TypeVar("T")


@dataclass(frozen=True)
class PropertyInt:
    nonNegative: bool = False
    range: Optional[Tuple[int, int]] = None


@dataclass(frozen=True)
class PropertyDouble:
    range: Optional[Tuple[float, float]] = None
    nonNegative: bool = False
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
    values: List[Tuple[str, T]]


PropertyType = Union[
    PropertyInt,
    PropertyChoice,
    PropertyDouble,
    PropertyTags,
    PropertySample,
    PropertyDateTime,
]


def delegate_for_property_type(
    proptype: PropertyType,
    sample_ids: List[int],
    available_tags: List[str],
    parent: Optional[QtCore.QObject] = None,
) -> QtWidgets.QAbstractItemDelegate:
    if isinstance(proptype, PropertyInt):
        return IntItemDelegate(proptype.nonNegative, proptype.range, parent)
    if isinstance(proptype, PropertyDouble):
        return DoubleItemDelegate(
            proptype.nonNegative, proptype.range, proptype.suffix, parent
        )
    if isinstance(proptype, PropertyChoice):
        return ComboItemDelegate(values=proptype.values, parent=parent)
    if isinstance(proptype, PropertySample):
        return ComboItemDelegate(
            values=[(str(v), v) for v in sample_ids], parent=parent
        )
    if isinstance(proptype, PropertyTags):
        return TagsItemDelegate(available_tags=available_tags, parent=parent)
    if isinstance(proptype, PropertyDateTime):
        return DateTimeItemDelegate(parent=parent)
    raise Exception(f"invalid property type {proptype}")
