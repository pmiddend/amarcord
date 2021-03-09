import logging
from dataclasses import dataclass, replace
from enum import Enum
from typing import Iterable, Optional, Set, Union

from PyQt5.QtCore import QRegExp
from PyQt5.QtGui import QRegExpValidator
from PyQt5.QtWidgets import (
    QDialogButtonBox,
    QFormLayout,
    QLineEdit,
    QVBoxLayout,
    QWidget,
)

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributo_id import AttributoId
from amarcord.qt.combo_box import ComboBox
from amarcord.qt.numeric_range_format_widget import (
    NumericRange,
    NumericRangeFormatWidget,
)
from amarcord.db.attributi import (
    DBAttributo,
    PropertyDouble,
    PropertyString,
    PropertyTags,
    RichAttributoType,
)
from amarcord.qt.qtreact import Context, DialogResult

logger = logging.getLogger(__name__)


class _AttributoType(Enum):
    NUMBER = "number"
    STRING = "string"
    STRING_LIST = "list of strings"


def _attributo_type_to_rich(p: _AttributoType) -> RichAttributoType:
    return PropertyDouble() if p == _AttributoType.NUMBER else PropertyString()


@dataclass(frozen=True)
class ProgramState:
    name: str
    description: str
    suffix: str
    type_: _AttributoType
    numeric_range: Optional[NumericRange]
    existing_properties: Set[AttributoId]


@dataclass(frozen=True)
class TypeChange:
    new_type: _AttributoType


@dataclass(frozen=True)
class DescriptionChange:
    new_description: str


@dataclass(frozen=True)
class SuffixChange:
    new_suffix: str


@dataclass(frozen=True)
class NumericRangeChange:
    new_range: Optional[NumericRange]


@dataclass(frozen=True)
class NameChange:
    new_name: str


ProgramEvent = Union[
    TypeChange, DescriptionChange, SuffixChange, NameChange, NumericRangeChange
]


def _state_to_gui(
    context: Context[ProgramState, ProgramEvent],
    parent: Optional[QWidget],
    s: ProgramState,
) -> QWidget:
    root = QWidget(parent)
    dialog_layout = QVBoxLayout(root)

    form = QFormLayout()
    dialog_layout.addLayout(form)
    name_input = QLineEdit()
    name_input.setValidator(QRegExpValidator(QRegExp(r"[a-zA-Z_][a-zA-Z_0-9]*")))
    name_input.setText(s.name)
    name_input.textChanged.connect(context.emitter(NameChange))
    form.addRow("Name", name_input)

    description_input = QLineEdit()
    description_input.setText(s.description)
    description_input.textChanged.connect(context.emitter(DescriptionChange))

    form.addRow("Description", description_input)

    suffix_input = QLineEdit(s.suffix)
    suffix_input.setPlaceholderText("example: mm, µl")
    suffix_input.textChanged.connect(context.emitter(SuffixChange))

    form.addRow("Suffix", suffix_input)

    type_combo = ComboBox(
        items=[(f.value, f) for f in _AttributoType],
        selected=s.type_,
    )
    form.addRow("Type", type_combo)

    if s.type_ == _AttributoType.NUMBER:
        numeric_range_input = NumericRangeFormatWidget(numeric_range=s.numeric_range)
        numeric_range_input.range_changed.connect(context.emitter(NumericRangeChange))
        form.addRow("Value range", numeric_range_input)

    type_combo.item_selected.connect(context.emitter(TypeChange))

    button_box = QDialogButtonBox(  # type: ignore
        QDialogButtonBox.Ok | QDialogButtonBox.Cancel
    )
    button_box.accepted.connect(context.dialog_accept)
    button_box.rejected.connect(context.dialog_reject)

    button_box.button(QDialogButtonBox.Ok).setEnabled(
        bool(s.name) and AttributoId(s.name) not in s.existing_properties
    )

    dialog_layout.addWidget(button_box)

    return root


def _state_reducer(state: ProgramState, event: ProgramEvent) -> ProgramState:
    if isinstance(event, NameChange):
        return replace(state, name=event.new_name)
    if isinstance(event, TypeChange):
        return replace(state, type_=event.new_type)
    if isinstance(event, DescriptionChange):
        return replace(state, description=event.new_description)
    if isinstance(event, SuffixChange):
        return replace(state, suffix=event.new_suffix)
    if isinstance(event, NumericRangeChange):
        return replace(state, numeric_range=event.new_range)
    logger.info("event of invalid type %s", type(event))
    return state


def new_attributo_dialog(
    existing_properties: Iterable[AttributoId],
    parent: Optional[QWidget] = None,
) -> Optional[DBAttributo]:
    dialog_context = Context(
        parent,
        ProgramState(
            name="",
            description="",
            suffix="",
            type_=_AttributoType.NUMBER,
            numeric_range=None,
            existing_properties=set(existing_properties),
        ),
        _state_reducer,
        _state_to_gui,
    )

    final_state, result = dialog_context.exec_dialog()

    if result == DialogResult.REJECTED:
        return None

    rich_prop: RichAttributoType
    if final_state.type_ == _AttributoType.NUMBER:
        rich_prop = PropertyDouble(final_state.numeric_range, final_state.suffix)
    elif final_state.type_ == _AttributoType.STRING:
        rich_prop = PropertyString()
    elif final_state.type_ == _AttributoType.STRING_LIST:
        rich_prop = PropertyTags()
    else:
        raise Exception(f"Invalid property type {final_state.type_}")

    return DBAttributo(
        name=AttributoId(final_state.name),
        description=final_state.description,
        suffix=final_state.suffix,
        rich_property_type=rich_prop,
        associated_table=AssociatedTable.RUN,
    )
