from typing import Optional

import re

from PyQt5 import QtWidgets
from PyQt5 import QtGui
from PyQt5 import QtCore

from amarcord.util import word_under_cursor


class InfixCompletingLineEdit(QtWidgets.QLineEdit):
    def __init__(self, parent: Optional[QtWidgets.QWidget]) -> None:
        super().__init__(parent)
        self._completer: Optional[QtWidgets.QCompleter] = None

    def setCompleter(self, completer: Optional[QtWidgets.QCompleter]) -> None:
        if self._completer is not None:
            self._completer.disconnect(self)

        self._completer = completer

        if self._completer is None:
            return

        self._completer.setWidget(self)
        self._completer.setCompletionMode(QtWidgets.QCompleter.PopupCompletion)
        self._completer.activated.connect(self.insertCompletion)

    def completer(self) -> Optional[QtWidgets.QCompleter]:
        return self._completer

    def insertCompletion(self, completion: str) -> None:
        if self._completer is None or self._completer.widget() != self:
            return

        # tc = self.textCursor()

        extra = len(completion) - len(self._completer.completionPrefix())
        t = self.text()

        previousCursorPosition = self.cursorPosition()
        self.setText(
            t[0:previousCursorPosition]
            + completion[-extra:]
            + t[previousCursorPosition:]
        )
        self.setCursorPosition(previousCursorPosition + extra)

    def textUnderCursor(self) -> str:
        print(f"text: {self.text()}, cursor pos: {self.cursorPosition()}")
        return _word_under_cursor(self.text(), self.cursorPosition())

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

        isShortcut = (
            e.modifiers() & QtCore.Qt.ControlModifier and e.key() == QtCore.Qt.Key_E
        )
        if self._completer is None or not isShortcut:
            super().keyPressEvent(e)

        ctrlOrShift = (
            e.modifiers() & QtCore.Qt.ControlModifier
            or e.modifiers() & QtCore.Qt.ShiftModifier
        )
        if self._completer is None or ctrlOrShift and not e.text():
            return

        eow = "~!@#$%^&*()_+{}|:\"<>?,./;'[]\\-="
        hasModifier = e.modifiers() != QtCore.Qt.NoModifier and not ctrlOrShift
        completionPrefix = self.textUnderCursor()
        print(completionPrefix)

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
