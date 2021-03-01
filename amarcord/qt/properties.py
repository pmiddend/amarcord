from dataclasses import dataclass
from typing import Any, List, Optional, Tuple, Union
import logging

from PyQt5 import QtCore, QtWidgets

logger = logging.getLogger(__name__)

from amarcord.qt.table_delegates import (
    ComboItemDelegate,
    DateTimeItemDelegate,
    DoubleItemDelegate,
    IntItemDelegate,
    TagsItemDelegate,
)


@dataclass(frozen=True)
class PropertyInt:
    nonNegative: bool = False
    range: Optional[Tuple[int, int]] = None


@dataclass(frozen=True)
class PropertyString:
    pass


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
    values: List[Tuple[str, Any]]


RichPropertyType = Union[
    PropertyInt,
    PropertyChoice,
    PropertyDouble,
    PropertyTags,
    PropertySample,
    PropertyString,
    PropertyDateTime,
]


def delegate_for_property_type(
    proptype: RichPropertyType,
    sample_ids: List[int],
    parent: Optional[QtCore.QObject] = None,
) -> QtWidgets.QAbstractItemDelegate:
    if isinstance(proptype, PropertyInt):
        return IntItemDelegate(proptype.nonNegative, proptype.range, parent)
    if isinstance(proptype, PropertyDouble):
        return DoubleItemDelegate(
            proptype.nonNegative, proptype.range, proptype.suffix, parent
        )
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
