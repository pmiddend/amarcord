from typing import Optional

from PyQt5 import QtWidgets
from PyQt5 import QtGui
from PyQt5 import QtCore
from PyQt5.QtCore import QTimer, pyqtSignal
from PyQt5.QtWidgets import QCompleter, QLineEdit, QWidget

from amarcord.util import word_under_cursor


class InfixCompletingLineEdit(QLineEdit):
    text_changed_debounced = pyqtSignal(str)

    def __init__(
        self, content: Optional[str] = None, parent: Optional[QWidget] = None
    ) -> None:
        super().__init__(parent)
        self._completer: Optional[QCompleter] = None
        if content is not None:
            self.setText(content)
        self.textChanged.connect(self._slot_text_changed)
        self._debounce = QTimer()
        self._debounce.setInterval(500)
        self._debounce.setSingleShot(True)
        self._debounce.timeout.connect(self._debounced)

    def _slot_text_changed(self) -> None:
        self._debounce.start()

    def _debounced(self) -> None:
        self.text_changed_debounced.emit(self.text())

    def setCompleter(self, completer: Optional[QCompleter]) -> None:
        if self._completer is not None:
            # mypy complains about too many arguments to "disconnect"
            self._completer.disconnect()  # type: ignore

        self._completer = completer

        if self._completer is None:
            return

        self._completer.setWidget(self)
        self._completer.setCompletionMode(QtWidgets.QCompleter.PopupCompletion)
        self._completer.activated.connect(self.insertCompletion)

    # mypy complains about Optional[QCompleter] not being in the
    # supertype, but I cannot invent a completer either
    def completer(self) -> Optional[QtWidgets.QCompleter]:  # type: ignore
        return self._completer

    def insertCompletion(self, completion: str) -> None:
        if self._completer is None or self._completer.widget() != self:
            return

        extra = len(completion) - len(self._completer.completionPrefix())

        if extra == 0:
            return

        t = self.text()

        previousCursorPosition = self.cursorPosition()
        self.setText(
            t[0:previousCursorPosition]
            + completion[-extra:]
            + t[previousCursorPosition:]
        )
        self.setCursorPosition(previousCursorPosition + extra)

    def textUnderCursor(self) -> str:
        return word_under_cursor(self.text(), self.cursorPosition())

        # tc = self.textCursor()
        # tc.select(QtGui.QTextCursor.WordUnderCursor)
        # return tc.selectedText()

    def focusInEvent(self, e: QtGui.QFocusEvent) -> None:
        if self._completer is not None:
            self._completer.setWidget(self)
        super().focusInEvent(e)

    def keyPressEvent(self, e: QtGui.QKeyEvent) -> None:
        if (
            self._completer is not None
            and self._completer.popup() is not None
            and self._completer.popup().isVisible()
        ):
            if e.key() in [
                QtCore.Qt.Key_Enter,
                QtCore.Qt.Key_Return,
                QtCore.Qt.Key_Escape,
                QtCore.Qt.Key_Tab,
                QtCore.Qt.Key_Backtab,
            ]:
                e.ignore()
                return

        # mypy complains about: Unsupported operand types for & ("KeyboardModifiers" and "KeyboardModifier")
        isShortcut = (
            e.modifiers() & QtCore.Qt.ControlModifier and e.key() == QtCore.Qt.Key_E  # type: ignore
        )
        if self._completer is None or not isShortcut:
            super().keyPressEvent(e)

        ctrlOrShift = (
            e.modifiers() & QtCore.Qt.ControlModifier  # type: ignore
            or e.modifiers() & QtCore.Qt.ShiftModifier  # type: ignore
        )
        if self._completer is None or ctrlOrShift and not e.text():
            return

        eow = "~!@#$%^&*()_+{}|:\"<>?,./;'[]\\-="
        hasModifier = e.modifiers() != QtCore.Qt.NoModifier and not ctrlOrShift  # type: ignore
        completionPrefix = self.textUnderCursor()

        if not isShortcut and (
            hasModifier
            or not e.text()
            or len(completionPrefix) < 3
            or e.text()[-1] in eow
        ):
            if self._completer.popup() is not None:
                self._completer.popup().hide()
            return

        if completionPrefix != self._completer.completionPrefix():
            self._completer.setCompletionPrefix(completionPrefix)
            if self._completer.popup() is not None:
                self._completer.popup().setCurrentIndex(
                    self._completer.completionModel().index(0, 0)
                )

        cr = self.cursorRect()
        cr.setWidth(
            self._completer.popup().sizeHintForColumn(0)
            + self._completer.popup().verticalScrollBar().sizeHint().width()
        )
        self._completer.complete(cr)
