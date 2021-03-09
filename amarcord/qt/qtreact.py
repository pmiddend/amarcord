import logging
from enum import Enum, IntEnum, auto
from typing import Callable, Generic, Optional, Tuple, TypeVar

from PyQt5.QtWidgets import (
    QDialog,
    QDialogButtonBox,
    QHBoxLayout,
    QLabel,
    QLineEdit,
    QPushButton,
    QSpinBox,
    QTableWidget,
    QWidget,
)

from amarcord.qt.combo_box import ComboBox
from amarcord.qt.numeric_range_format_widget import NumericRangeFormatWidget

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

T = TypeVar("T")
S = TypeVar("S")
E = TypeVar("E")
V = TypeVar("V")


class CompareStatus(Enum):
    REPLACED = auto()
    NO_ACTION = auto()
    UPDATED = auto()


class DialogResult(IntEnum):
    ACCEPTED = 1
    REJECTED = 0


def compare_widgets(last_render: QWidget, new_render: QWidget) -> CompareStatus:
    # print("Comparing")
    # pylint: disable=unidiomatic-typecheck
    if type(last_render) != type(new_render):
        # print(f"type change, before {type(last_render)}, after {type(new_render)}")
        return CompareStatus.REPLACED
    if isinstance(last_render, QLineEdit):
        # print("Line edit")
        if last_render.text() != new_render.text():
            # print(f"Text changed to {new_render.text()}")
            last_render.blockSignals(True)
            last_render.setText(new_render.text())
            last_render.blockSignals(False)
            return CompareStatus.UPDATED
        # print("No change")
        return CompareStatus.NO_ACTION
    if isinstance(last_render, QSpinBox):
        # print("Spin box")
        if last_render.value() != new_render.value():
            # print(f"Int changed to {new_render.value()}")
            last_render.blockSignals(True)
            last_render.setValue(new_render.value())
            last_render.blockSignals(False)
            return CompareStatus.UPDATED
        # print("No action")
        return CompareStatus.NO_ACTION
    if isinstance(last_render, NumericRangeFormatWidget):
        if last_render.numeric_range != new_render.numeric_range:
            last_render.numeric_range = new_render.numeric_range
            return CompareStatus.UPDATED
        return CompareStatus.NO_ACTION
    if isinstance(last_render, ComboBox):
        if last_render.current_value() != new_render.current_value():
            logger.info(
                "Combobox has been updated, new value %s", new_render.current_value()
            )
            last_render.blockSignals(True)
            last_render.set_current_value(new_render.current_value())
            last_render.blockSignals(False)
            return CompareStatus.UPDATED
        return CompareStatus.NO_ACTION
    if isinstance(last_render, QTableWidget):
        return CompareStatus.REPLACED
        # if last_render.rowCount() != new_render.rowCount():
        #     return CompareStatus.REPLACED
        # return CompareStatus.NO_ACTION
    if isinstance(last_render, QLabel):
        if last_render.text() != new_render.text():
            last_render.setText(new_render.text())
        return CompareStatus.UPDATED
    if isinstance(last_render, QDialogButtonBox):
        updated = False
        for button in [QDialogButtonBox.Ok]:
            last_button = last_render.button(button)
            new_button = new_render.button(button)
            if last_button.isEnabled() != new_button.isEnabled():
                last_button.setEnabled(new_button.isEnabled())
                updated = True
        return CompareStatus.UPDATED if updated else CompareStatus.NO_ACTION
    if isinstance(last_render, QPushButton):
        if last_render.isEnabled() != new_render.isEnabled():
            last_render.setEnabled(new_render.isEnabled())
        return CompareStatus.NO_ACTION
    assert type(last_render) == QWidget, f"unknown widget type {type(last_render)}"
    if len(last_render.children()) != len(new_render.children()):
        logger.info("length of children differs, replacing")
        return CompareStatus.REPLACED
    assert len(last_render.children()) == len(new_render.children())
    for old_child, new_child in zip(last_render.children(), new_render.children()):
        if not isinstance(old_child, QWidget) or not isinstance(new_child, QWidget):
            continue
        comparison = compare_widgets(old_child, new_child)
        if comparison == CompareStatus.UPDATED:
            continue
        if comparison == CompareStatus.REPLACED:
            last_render.layout().replaceWidget(old_child, new_child)
    return CompareStatus.UPDATED


class Context(Generic[S, E]):
    def __init__(
        self,
        parent: Optional[QWidget],
        initial_state: S,
        reducer: Callable[[S, E], S],
        renderer: Callable[["Context[S, E]", Optional[QWidget], S], QWidget],
    ) -> None:
        self._state = initial_state
        self._reducer = reducer
        self._renderer = renderer
        self._last_render: Optional[QWidget] = None
        self._dialog: QDialog = QDialog(parent)
        self._dialog_layout = QHBoxLayout()
        self._dialog.setLayout(self._dialog_layout)

    def exec_dialog(self) -> Tuple[S, DialogResult]:
        self._dialog_layout.addWidget(self._renderer(self, self._dialog, self._state))
        result = self._dialog.exec()
        return self._state, DialogResult(result)

    def emitter(self, f: Callable[[V], E]) -> Callable[[V], None]:
        return lambda v: self.emit_event(f(v))

    def dialog_accept(self) -> None:
        self._dialog.accept()

    def dialog_reject(self) -> None:
        self._dialog.reject()

    def null_emitter(self, f: Callable[[], E]) -> Callable[[], None]:
        return lambda: self.emit_event(f())

    def emit_event(self, s: E) -> None:
        self._state = self._reducer(self._state, s)
        new_render = self._renderer(self, None, self._state)

        last_render = self._dialog_layout.itemAt(0).widget()
        comparison = compare_widgets(last_render, new_render)
        if comparison == CompareStatus.REPLACED:
            replaced = self._dialog_layout.replaceWidget(last_render, new_render)
            if replaced:
                logger.info("Replacement successful")
                last_render.deleteLater()
            self._dialog_layout.update()
            # self._last_render = new_render
            logger.info("Replaced the root widget")
            # raise ValueError(
            #     "you replaced the root element, which we do not support yet"
            # )
