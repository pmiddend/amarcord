from dataclasses import dataclass
from typing import Optional
from typing import Dict
from PyQt5.QtWidgets import QHeaderView, QWidget, QTableView
from PyQt5.QtGui import QShowEvent
from PyQt5.QtCore import Qt, QSize


@dataclass
class Margins:
    left: int = 2
    right: int = 2
    top: int = 2
    bottom: int = 2


@dataclass
class _Item:
    item: Optional[QWidget]
    margins: Margins


# Source: https://stackoverflow.com/questions/27000484/add-custom-widgets-as-qtablewidget-horizontalheader
class FilterHeader(QHeaderView):
    def __init__(self, parent: QWidget) -> None:
        super().__init__(Qt.Horizontal, parent)
        self.sectionResized.connect(self._handle_section_resized)
        self.sectionMoved.connect(self._handle_section_moved)
        self._items: Dict[int, _Item] = {}

    def _handle_section_resized(self, i: int) -> None:
        for j in range(self.visualIndex(i), self.count()):
            logical = self.logicalIndex(j)
            iitem = self._items.get(i, None)
            if iitem is None:
                continue
            print(f"section {i} resized")
            self._items[logical].item.setGeometry(  # type: ignore
                self.sectionViewportPosition(logical) + iitem.margins.left,
                iitem.margins.top,
                self.sectionSize(logical)
                - iitem.margins.left
                - iitem.margins.right
                - 1,
                self.height() - iitem.margins.top - iitem.margins.bottom - 1,
            )

    def _handle_section_moved(
        self, logical: int, oldVisualIndex: int, newVisualIndex: int
    ) -> None:
        pass

    def fix_combo_positions(self) -> None:
        for i in range(self.count()):
            print(f"section {i} fixed combo positions")
            self._items[i].item.setGeometry(  # type: ignore
                self.sectionViewportPosition(i) + self._items[i].margins.left,
                self._items[i].margins.top,
                self.sectionSize(i)
                - self._items[i].margins.left
                - self._items[i].margins.right
                - 1,
                self.height()
                - self._items[i].margins.top
                - self._items[i].margins.bottom
                - 1,
            )

    def set_item_widget(self, index: int, widget: QWidget) -> None:
        widget.setParent(self)
        self._items[index] = _Item(None, Margins())
        self._items[index].item = widget

    def set_item_margins(self, index: int, margins: Margins) -> None:
        self._items[index].margins = margins

    def sectionSizeFromContents(self, logical: int) -> QSize:
        visual = self.visualIndex(logical)
        if visual in self._items and self._items[visual].item is not None:
            i = self._items[visual].item
            # print("special: " + str(i.sizeHint()) + ", has layout: " + str(i.layout()))
            print(f"size hint for {visual} is: " + str(i.sizeHint()))  # type: ignore
            return i.sizeHint()  # type: ignore
        return super().sectionSizeFromContents(logical)

    def sectionSize(self, logicalIndex: int) -> int:
        print(
            f"sectionSize called for {logicalIndex}: "
            + str(self.sectionSizeFromContents(logicalIndex).width())
        )
        return self.sectionSizeFromContents(logicalIndex).width()
        # print(
        #     f"sectionSize called for {logicalIndex}: "
        #     + str(super().sectionSize(logicalIndex))
        # )
        # return super().sectionSize(logicalIndex)

    def sectionSizeHint(self, logicalIndex: int) -> int:
        print(f"sectionSizeHint called for {logicalIndex}")
        return super().sectionSizeHint(logicalIndex)

    # def sizeHint(self):
    #     size = super().sizeHint()
    #     if self._items:
    #         size.setHeight(
    #             max(
    #                 f.item.sizeHint().height()
    #                 for f in self._items.values()
    #                 if f.item is not None
    #             )
    #         )
    #     return size

    def showEvent(self, e: QShowEvent) -> None:
        for i in range(self.count()):
            if i not in self._items:
                continue
            # if i not in self._items:
            #     self._items[i] = _Item(None, Margins())

            item = self._items[i]

            # if item.item is None:
            #     item.item = QWidget(self)
            # else:
            #     pass
            #     # item.item.setParent(self)

            left = self.sectionViewportPosition(i) + item.margins.left
            top = item.margins.top
            width = self.sectionSize(i) - item.margins.left - item.margins.right - 1
            height = self.height() - item.margins.top - item.margins.bottom - 1
            item.item.setGeometry(  # type: ignore
                left,
                top,
                width,
                height,
            )

        super().showEvent(e)


class FilterHeaderTableView(QTableView):
    def __init__(self, parent: Optional[QWidget]) -> None:
        super().__init__(parent)
        self.header = FilterHeader(self)
        self.setHorizontalHeader(self.header)

    def scrollContentsBy(self, dx: int, dy: int) -> None:
        super().scrollContentsBy(dx, dy)

        if dx != 0:
            self.header.fix_combo_positions()

    def setHorizontalHeaderWidget(self, column: int, widget: QWidget) -> None:
        self.header.set_item_widget(column, widget)
