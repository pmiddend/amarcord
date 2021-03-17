import datetime
import logging
from typing import List, Optional, Tuple, TypeVar, cast

from PyQt5 import QtCore, QtWidgets
from PyQt5.QtWidgets import QDateTimeEdit

from amarcord.db.attributo_type import (
    AttributoTypeChoice,
    AttributoTypeDateTime,
    AttributoTypeDouble,
    AttributoTypeDuration,
    AttributoTypeInt,
    AttributoTypeList,
    AttributoTypeSample,
    AttributoTypeString,
    AttributoTypeTags,
    AttributoTypeUserName,
    AttributoType,
)
from amarcord.qt.datetime import (
    from_qt_datetime,
    parse_natural_delta,
    print_natural_delta,
    to_qt_datetime,
)
from amarcord.qt.numeric_input_widget import NumericInputWidget
from amarcord.qt.numeric_range_format_widget import NumericRange
from amarcord.qt.tags import Tags
from amarcord.qt.validated_line_edit import ValidatedLineEdit
from amarcord.qt.validators import parse_float_list, parse_string_list

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


class ListItemDelegate(QtWidgets.QStyledItemDelegate):
    def __init__(
        self,
        min_length: Optional[int],
        max_length: Optional[int],
        subtype: AttributoType,
        parent: Optional[QtCore.QObject],
    ) -> None:
        super().__init__(parent)
        self._min_length = min_length
        self._max_length = max_length
        self._subtype = subtype

    def createEditor(
        self,
        parent: QtWidgets.QWidget,
        _option: QtWidgets.QStyleOptionViewItem,
        _index: QtCore.QModelIndex,
    ) -> QtWidgets.QWidget:
        if isinstance(self._subtype, AttributoTypeString):
            return ValidatedLineEdit(
                None,
                lambda str_list: ", ".join(str(s) for s in str_list),  # type: ignore
                lambda str_list_str: parse_string_list(str_list_str, None),  # type: ignore
                "list of strings, separated by commas",
                parent,
            )
        if isinstance(self._subtype, AttributoTypeDouble):
            infix = (
                "list of numbers"
                if self._min_length is None and self._max_length is None
                else f"list of at least {self._min_length} number(s)"
                if self._min_length is not None and self._max_length is None
                else f"{self._min_length} numbers"
                if self._min_length == self._max_length
                else f"at least {self._min_length}, at most {self._max_length} number(s)"
            )
            suffix = ", separated by commas"
            return ValidatedLineEdit(
                None,
                lambda float_list: ", ".join(str(s) for s in float_list),  # type: ignore
                lambda float_list_str: parse_float_list(float_list_str, min_elements=3, max_elements=3),  # type: ignore
                infix + suffix,
                parent,
            )
        raise Exception(f"no delegate for list of {type(self._subtype)} yet")

    # pylint: disable=no-self-use
    def setEditorData(
        self, editor: QtWidgets.QWidget, index: QtCore.QModelIndex
    ) -> None:
        assert isinstance(editor, ValidatedLineEdit)
        data = index.model().data(index, QtCore.Qt.EditRole)
        if data is None:
            return
        if not isinstance(data, list):
            raise ValueError(f"expected list, got {type(data)}")
        if data and not isinstance(data[0], (str, float, int)):
            raise ValueError(f"expected string or number, got {type(data[0])}")
        # mypy complains about unpacking QVariant. Don't care here.
        # noinspection PyTypeChecker
        editor.set_value(data)  # type: ignore

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
    # pylint: disable=no-self-use
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


def delegate_for_attributo_type(
    proptype: AttributoType,
    sample_ids: List[int],
    parent: Optional[QtCore.QObject] = None,
) -> QtWidgets.QAbstractItemDelegate:
    if isinstance(proptype, AttributoTypeInt):
        return IntItemDelegate(proptype.nonNegative, proptype.range, parent)
    if isinstance(proptype, AttributoTypeDouble):
        return DoubleItemDelegate(proptype.range, proptype.suffix, parent)
    if isinstance(proptype, AttributoTypeList):
        return ListItemDelegate(
            proptype.min_length, proptype.max_length, proptype.sub_type, parent
        )
    if isinstance(proptype, AttributoTypeDuration):
        return DurationItemDelegate(parent)
    if isinstance(proptype, AttributoTypeString):
        return QtWidgets.QStyledItemDelegate(parent=parent)
    if isinstance(proptype, AttributoTypeUserName):
        return QtWidgets.QStyledItemDelegate(parent=parent)
    if isinstance(proptype, AttributoTypeChoice):
        return ComboItemDelegate(values=proptype.values, parent=parent)
    if isinstance(proptype, AttributoTypeSample):
        return ComboItemDelegate(
            values=[(str(v), v) for v in sample_ids], parent=parent
        )
    if isinstance(proptype, AttributoTypeTags):
        return TagsItemDelegate(available_tags=[], parent=parent)
    if isinstance(proptype, AttributoTypeDateTime):
        return DateTimeItemDelegate(parent=parent)
    raise Exception(f"invalid property type {proptype}")
