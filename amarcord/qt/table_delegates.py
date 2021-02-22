import datetime
from typing import List, Optional, Tuple, TypeVar, cast

from PyQt5 import QtCore, QtWidgets

from amarcord.qt.datetime import from_qt_datetime, to_qt_datetime
from amarcord.qt.tags import Tags

T = TypeVar("T")


class ComboItemDelegate(QtWidgets.QStyledItemDelegate):
    def __init__(
        self, values: List[Tuple[str, T]], parent: Optional[QtCore.QObject]
    ) -> None:
        super().__init__(parent)
        self._values = values

    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
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

    def updateEditorGeometry(
        self,
        editor: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
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
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        editor = Tags(parent)
        editor.completion(self._available_tags)
        return editor

    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        assert isinstance(editor, Tags)
        data = index.model().data(index, QtCore.Qt.EditRole)
        assert isinstance(data, list)
        cast(Tags, editor).tags(data)

    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        assert isinstance(editor, Tags)
        model.setData(index, cast(Tags, editor).tagsStr(), QtCore.Qt.EditRole)

    def updateEditorGeometry(
        self,
        editor: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> None:
        editor.setGeometry(option.rect)


class DateTimeItemDelegate(QtWidgets.QStyledItemDelegate):
    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        editor = QtWidgets.QDateTimeEdit(parent)
        editor.setFrame(False)
        return editor

    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        assert isinstance(editor, QtWidgets.QDateTimeEdit)
        data = index.model().data(index, QtCore.Qt.EditRole)
        assert isinstance(data, datetime.datetime)
        cast(QtWidgets.QDateTimeEdit, editor).setDateTime(to_qt_datetime(data))

    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        assert isinstance(editor, QtWidgets.QDateTimeEdit)
        model.setData(
            index,
            from_qt_datetime(cast(QtWidgets.QDateTimeEdit, editor).dateTime()),
            QtCore.Qt.EditRole,
        )

    def updateEditorGeometry(
        self,
        editor: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> None:
        editor.setGeometry(option.rect)


class IntItemDelegate(QtWidgets.QStyledItemDelegate):
    def __init__(
        self,
        nonNegative: bool,
        range: Optional[Tuple[int, int]],
        parent: Optional[QtCore.QObject],
    ) -> None:
        super().__init__(parent)
        self._nonNegative = nonNegative
        self._range = (
            range if range is not None else [0, 1 ** 31] if nonNegative else None
        )

    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        editor = QtWidgets.QSpinBox(parent)
        editor.setFrame(False)
        if self._range is not None:
            editor.setRange(self._range[0], self._range[1])
        return editor

    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        assert isinstance(editor, QtWidgets.QSpinBox)
        data = index.model().data(index, QtCore.Qt.EditRole)
        assert isinstance(data, int)
        cast(QtWidgets.QSpinBox, editor).setValue(data)

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

    def updateEditorGeometry(
        self,
        editor: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> None:
        editor.setGeometry(option.rect)


class DoubleItemDelegate(QtWidgets.QStyledItemDelegate):
    def __init__(
        self,
        nonNegative: bool,
        range: Optional[Tuple[float, float]],
        parent: Optional[QtCore.QObject],
    ) -> None:
        super().__init__(parent)
        self._range = (
            range if range is not None else [0, 10000] if nonNegative else None
        )

    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        editor = QtWidgets.QDoubleSpinBox(parent)
        editor.setFrame(False)
        if self._range is not None:
            editor.setRange(self._range[0], self._range[1])
        return editor

    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        assert isinstance(editor, QtWidgets.QDoubleSpinBox)
        data = index.model().data(index, QtCore.Qt.EditRole)
        if not isinstance(data, float):
            raise ValueError(f"expected float, got {type(data)}")
        cast(QtWidgets.QDoubleSpinBox, editor).setValue(data)

    def setModelData(
        self,
        editor: QtWidgets.QWidget,
        model: QtCore.QAbstractItemModel,
        index: QtCore.QModelIndex,
    ) -> None:
        assert isinstance(editor, QtWidgets.QDoubleSpinBox)
        model.setData(
            index, cast(QtWidgets.QDoubleSpinBox, editor).value(), QtCore.Qt.EditRole
        )

    def updateEditorGeometry(
        self,
        editor: QtWidgets.QWidget,
        option: QtWidgets.QStyleOptionViewItem,
        index: QtCore.QModelIndex,
    ) -> None:
        editor.setGeometry(option.rect)
