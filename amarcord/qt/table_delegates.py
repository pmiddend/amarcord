import logging
import datetime
from typing import List, Optional, Tuple, TypeVar, cast

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtWidgets import QDateTimeEdit

from amarcord.qt.datetime import (
    from_qt_datetime,
    parse_natural_delta,
    print_natural_delta,
    qt_from_isoformat,
    qt_to_isoformat,
    to_qt_datetime,
)
from amarcord.qt.numeric_input_widget import NumericInputWidget
from amarcord.qt.numeric_range_format_widget import NumericRange
from amarcord.qt.tags import Tags
from amarcord.qt.validated_line_edit import ValidatedLineEdit

T = TypeVar("T")

logger = logging.getLogger(__name__)


class ComboItemDelegate(QtWidgets.QStyledItemDelegate):
    def __init__(
        self, values: List[Tuple[str, T]], parent: Optional[QtCore.QObject]
    ) -> None:
        super().__init__(parent)
        self._values = values

    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        _option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        editor = QtWidgets.QComboBox(parent)
        editor.addItems([v[0] for v in self._values])
        editor.setFrame(False)
        return editor

    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        assert isinstance(editor, QtWidgets.QComboBox)
        model_data = index.model().data(index, QtCore.Qt.EditRole)
        cast(QtWidgets.QComboBox, editor).setCurrentIndex(
            [v[1] for v in self._values].index(model_data)
        )

    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        assert isinstance(editor, QtWidgets.QComboBox)
        model.setData(
            index,
            self._values[cast(QtWidgets.QComboBox, editor).currentIndex()][1],
            QtCore.Qt.EditRole,
        )

    # pylint: disable=no-self-use
    def updateEditorGeometry(
        self,
        editor: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> None:
        editor.setGeometry(option.rect)


class TagsItemDelegate(QtWidgets.QStyledItemDelegate):
    def __init__(
        self, available_tags: List[str], parent: Optional[QtCore.QObject]
    ) -> None:
        super().__init__(parent)
        self._available_tags = available_tags

    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        _option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        editor = Tags(parent)
        editor.completion(self._available_tags)
        return editor

    # pylint: disable=no-self-use
    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        assert isinstance(editor, Tags)
        data = index.model().data(index, QtCore.Qt.EditRole)
        if data is None:
            return
        assert isinstance(data, list)
        cast(Tags, editor).tags(data)

    # pylint: disable=no-self-use
    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        assert isinstance(editor, Tags)
        model.setData(index, cast(Tags, editor).tags_str(), QtCore.Qt.EditRole)

    # pylint: disable=no-self-use
    def updateEditorGeometry(
        self,
        editor: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> None:
        editor.setGeometry(option.rect)


class DateTimeItemDelegate(QtWidgets.QStyledItemDelegate):
    # pylint: disable=no-self-use
    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        _option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        editor = QtWidgets.QDateTimeEdit(parent)
        editor.setFrame(False)
        return editor

    # pylint: disable=no-self-use
    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        assert isinstance(editor, QtWidgets.QDateTimeEdit)
        data = index.model().data(index, QtCore.Qt.EditRole)
        if data is None:
            return
        assert isinstance(
            data, datetime.datetime
        ), f"expected datetime, got {type(data)}"
        cast(QtWidgets.QDateTimeEdit, editor).setDateTime(to_qt_datetime(data))

    # pylint: disable=no-self-use
    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        assert isinstance(editor, QtWidgets.QDateTimeEdit)
        model.setData(
            index,
            from_qt_datetime(cast(QDateTimeEdit, editor).dateTime()),
            QtCore.Qt.EditRole,
        )

    # pylint: disable=no-self-use
    def updateEditorGeometry(
        self,
        editor: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> None:
        editor.setGeometry(option.rect)


class IntItemDelegate(QtWidgets.QStyledItemDelegate):
    def __init__(
        self,
        non_negative: bool,
        value_range: Optional[Tuple[int, int]],
        parent: Optional[QtCore.QObject],
    ) -> None:
        super().__init__(parent)
        self._nonNegative = non_negative
        self._range = (
            value_range
            if value_range is not None
            else [0, 2 ** 30]
            if non_negative
            else None
        )

    # pylint: disable=no-self-use
    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        _option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        editor = QtWidgets.QSpinBox(parent)
        editor.setFrame(False)
        if self._range is not None:
            editor.setRange(self._range[0], self._range[1])
        return editor

    # pylint: disable=no-self-use
    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        assert isinstance(editor, QtWidgets.QSpinBox)
        data = index.model().data(index, QtCore.Qt.EditRole)
        if data is None:
            return
        assert isinstance(data, int)
        cast(QtWidgets.QSpinBox, editor).setValue(data)

    # pylint: disable=no-self-use
    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        assert isinstance(editor, QtWidgets.QSpinBox)
        model.setData(
            index, cast(QtWidgets.QSpinBox, editor).value(), QtCore.Qt.EditRole
        )

    # pylint: disable=no-self-use
    def updateEditorGeometry(
        self,
        editor: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> None:
        editor.setGeometry(option.rect)


class DoubleItemDelegate(QtWidgets.QStyledItemDelegate):
    def __init__(
        self,
        numeric_range: Optional[NumericRange],
        suffix: Optional[str],
        parent: Optional[QtCore.QObject],
    ) -> None:
        super().__init__(parent)
        self._range = numeric_range
        self._suffix = suffix

    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        _option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        return NumericInputWidget(None, self._range, placeholder=None, parent=parent)

    # pylint: disable=no-self-use
    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        assert isinstance(editor, NumericInputWidget)
        data = index.model().data(index, QtCore.Qt.EditRole)
        if data is None:
            return
        if not isinstance(data, (int, float)):
            raise ValueError(f"expected float, got {type(data)}")
        cast(NumericInputWidget, editor).set_value(float(data))

    # pylint: disable=no-self-use
    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        assert isinstance(editor, NumericInputWidget)
        value = editor.value()
        if value is not None:
            model.setData(index, value, QtCore.Qt.EditRole)

    # pylint: disable=no-self-use
    def updateEditorGeometry(
        self,
        editor: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> None:
        editor.setGeometry(option.rect)


class DurationItemDelegate(QtWidgets.QStyledItemDelegate):
    def __init__(
        self,
        parent: Optional[QtCore.QObject],
    ) -> None:
        super().__init__(parent)

    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        _option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        return ValidatedLineEdit(
            None,
            print_natural_delta,  # type: ignore
            parse_natural_delta,  # type: ignore
            "example: 1 day, 5 hours, 30 minutes",
            parent,
        )

    # pylint: disable=no-self-use
    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        assert isinstance(editor, ValidatedLineEdit)
        data = index.model().data(index, QtCore.Qt.EditRole)
        if data is None:
            return
        if not isinstance(data, datetime.timedelta):
            raise ValueError(f"expected timedelta, got {type(data)}")
        # noinspection PyTypeChecker
        cast(ValidatedLineEdit, editor).set_value(data)  # type: ignore

    # pylint: disable=no-self-use
    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        assert isinstance(editor, ValidatedLineEdit)
        value = editor.value()
        if value is not None:
            model.setData(index, value, QtCore.Qt.EditRole)

    # pylint: disable=no-self-use
    def updateEditorGeometry(
        self,
        editor: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> None:
        editor.setGeometry(option.rect)
