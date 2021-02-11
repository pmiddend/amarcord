from typing import Optional
from typing import List
from typing import Tuple
from dataclasses import dataclass

from PyQt5.QtWidgets import QWidget
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QStyleOptionFrame, QApplication
from PyQt5.QtCore import QSize
from PyQt5.QtCore import pyqtSignal
from PyQt5.QtCore import QTimerEvent
from PyQt5.QtCore import QPoint
from PyQt5.QtCore import QPointF, QLineF, QSizeF
from PyQt5.QtGui import QPaintEvent, QGuiApplication, QFontMetrics, QPalette
from PyQt5.QtGui import QTextLayout, QPainter, QPainterPath, QColor, QKeySequence
from PyQt5.QtWidgets import QCompleter
from PyQt5.QtGui import QMouseEvent
from PyQt5.QtGui import QFocusEvent
from PyQt5.QtGui import QResizeEvent
from PyQt5.QtGui import QKeyEvent
from PyQt5.QtWidgets import QSizePolicy
from PyQt5.QtCore import Qt
from PyQt5.QtCore import QRect, QRectF


def _remove(s: str, position: int, n: int) -> str:
    return s[:position] + s[position + n :]


@dataclass
class Tag:
    text: str
    rect: QRect


top_text_margin = 1
bottom_text_margin = 1
left_text_margin = 1
right_text_margin = 1

vertical_margin = 3
bottommargin = 1
topmargin = 1

horizontal_margin = 3
leftmargin = 1
rightmargin = 1

tag_spacing = 3
tag_inner_left_padding = 3
tag_inner_right_padding = 4
tag_cross_width = 4
tag_cross_spacing = 2


class Tags(QWidget):
    tagsEdited = pyqtSignal()

    def __init__(self, parent: Optional[QWidget] = None) -> None:
        super().__init__(parent)

        self._tags: List[Tag] = [Tag(text="", rect=QRect())]
        self._editing_index = 0
        self._select_start = 0
        self._select_size = 0
        self._cursor = 0
        self._hscroll = 0
        self._text_layout = QTextLayout()
        self._blink_timer = 0
        self._blink_status = True
        # self._ctrl = QInputControl.LineEdit

        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.setFocusPolicy(Qt.StrongFocus)
        self.setCursor(Qt.IBeamCursor)
        self.setAttribute(Qt.WA_InputMethodEnabled, True)
        self.setMouseTracking(True)
        self._completer = QCompleter()
        self.setupCompleter()
        self.setCursorVisible(self.hasFocus())
        self.updateDisplayText()

    def setCursorVisible(self, visible: bool) -> None:
        if self._blink_timer:
            self.killTimer(self._blink_timer)
            self._blink_timer = 0
            self._blink_status = True

        if visible:
            flashTime = QGuiApplication.styleHints().cursorFlashTime()
            if flashTime >= 2:
                self._blink_timer = self.startTimer(flashTime // 2)
        else:
            self._blink_status = False

    def setupCompleter(self) -> None:
        self._completer.setWidget(self)
        self._completer.activated.connect(self._completerActivated)

    def currentText(self) -> str:
        return self._tags[self._editing_index].text

    def _completerActivated(self, t: str) -> None:
        self._tags[self._editing_index].text = t
        self.moveCursor(len(self.currentText()), False)
        self.updateDisplayText()
        self.calcRects()
        self.update()

    def updateDisplayText(self) -> None:
        self._text_layout.clearLayout()
        self._text_layout.setText(self.currentText())
        self._text_layout.beginLayout()
        self._text_layout.createLine()
        self._text_layout.endLayout()

    def initStyleOption(self, option: QStyleOptionFrame) -> None:
        option.initFrom(self)
        option.rect = self.contentsRect()
        option.lineWidth = self.style().pixelMetric(
            QStyle.PM_DefaultFrameWidth, option, self
        )
        option.midLineWidth = 0
        option.state |= QStyle.State_Sunken  # type: ignore
        option.features = QStyleOptionFrame.None_

    def cRect(self) -> QRect:
        panel = QStyleOptionFrame()
        self.initStyleOption(panel)
        r = self.style().subElementRect(QStyle.SE_LineEditContents, panel, self)
        r.adjust(
            left_text_margin, top_text_margin, -right_text_margin, -bottom_text_margin
        )
        return r

    def calcRects(self) -> None:
        r = self.cRect()
        lt = r.topLeft()

        if self.cursorVisible():
            self.calcRects2(lt, r.height(), self._tags[0 : self._editing_index])
            self.calcEditorRect(lt, r.height())
            self.calcRects2(lt, r.height(), self._tags[self._editing_index + 1 :])
        else:
            self.calcRects2(lt, r.height(), self._nonEmptyTags())

    def _nonEmptyTags(self) -> List[Tag]:
        return [f for f in self._tags if f.text != ""]

    def calcEditorRect(self, lt: QPoint, height: int) -> None:
        w = (
            self.fontMetrics().horizontalAdvance(self._text_layout.text())
            + tag_inner_left_padding
            + tag_inner_right_padding
        )
        self._tags[self._editing_index].rect = QRect(lt, QSize(w, height))
        lt += QPoint(w + tag_spacing, 0)

    def calcRects2(self, lt: QPoint, height: int, range_: List[Tag]) -> None:
        for t in range_:
            i_width = self.fontMetrics().horizontalAdvance(t.text)
            i_r = QRect(lt, QSize(i_width, height))
            i_r.translate(tag_inner_left_padding, 0)
            i_r.adjust(
                -tag_inner_left_padding,
                0,
                tag_inner_right_padding + tag_cross_spacing + tag_cross_width,
                0,
            )
            t.rect = i_r
            lt.setX(i_r.right() + tag_spacing)

    def moveCursor(self, pos: int, mark: bool) -> None:
        if mark:
            e = self._select_start + self._select_size
            anchor = (
                e
                if self._select_size > 0 and self._cursor == self._select_start
                else self._select_start
                if self._select_size > 0 and self._cursor == e
                else self._cursor
            )
            self._select_start = min(anchor, pos)
            self._select_size = max(anchor, pos) - self._select_start
        else:
            self.deselectAll()

        self._cursor = pos

    def selectAll(self) -> None:
        self._select_start = 0
        self._select_size = len(self.currentText())

    def deselectAll(self) -> None:
        self._select_start = 0
        self._select_size = 0

    def sizeHint(self) -> QSize:
        self.ensurePolished()
        fm = QFontMetrics(self.font())
        h = (
            fm.height()
            + 2 * vertical_margin
            + top_text_margin
            + bottom_text_margin
            + topmargin
            + bottommargin
        )
        w = (
            fm.boundingRect("x").width() * 17
            + 2 * horizontal_margin
            + leftmargin
            + rightmargin
        )
        opt = QStyleOptionFrame()
        self.initStyleOption(opt)
        return self.style().sizeFromContents(
            QStyle.CT_LineEdit,
            opt,
            QSize(w, h).expandedTo(QApplication.globalStrut()),
            self,
        )

    def minimumSizeHint(self) -> QSize:
        self.ensurePolished()

        fm = self.fontMetrics()

        h = (
            fm.height()
            + max(2 * vertical_margin, fm.leading())
            + top_text_margin
            + bottom_text_margin
            + topmargin
            + bottommargin
        )
        w = fm.maxWidth() + leftmargin + rightmargin

        opt = QStyleOptionFrame()
        self.initStyleOption(opt)
        return self.style().sizeFromContents(
            QStyle.CT_LineEdit,
            opt,
            QSize(w, h).expandedTo(QApplication.globalStrut()),
            self,
        )

    def completion(self, completions: List[str]) -> None:
        self._completer = QCompleter(completions)
        self.setupCompleter()

    def editNextTag(self) -> None:
        if self._editing_index < len(self._tags) - 1:
            self.setEditingIndex(self._editing_index + 1)
            self.moveCursor(0, False)

    def editNewTag(self) -> None:
        self._tags.append(Tag(text="", rect=QRect()))
        self.setEditingIndex(len(self._tags) - 1)
        self.moveCursor(0, False)

    def setEditingIndex(self, i: int) -> None:
        if not self.currentText():
            self._tags.pop(self._editing_index)
            if self._editing_index <= i:
                i -= 1
        self._editing_index = i

    def tagsStr(self) -> List[str]:
        return [v.text for v in self._tags if v.text != ""]

    def tags(self, tags: List[str]) -> None:
        self._tags = [Tag(text="", rect=QRect())]
        self._tags.extend(Tag(t, QRect()) for t in tags)
        self._editing_index = 0
        self.moveCursor(0, False)
        self.editNewTag()
        self.updateDisplayText()
        self.calcRects()

        self.update()

    def currentRect(self) -> QRect:
        return self._tags[self._editing_index].rect

    def paintEvent(self, event: QPaintEvent) -> None:
        p = QPainter(self)

        panel = QStyleOptionFrame()
        self.initStyleOption(panel)

        self.style().drawPrimitive(QStyle.PE_PanelLineEdit, panel, p, self)

        rect = self.cRect()

        p.setClipRect(rect)

        if self.cursorVisible():
            r = self.currentRect()
            txt_p = QPointF(r.topLeft()) + QPointF(
                tag_inner_left_padding, ((r.height() - self.fontMetrics().height()) / 2)
            )

            self.calcHScroll(r)

            self.drawTags(p, self._tags[0 : self._editing_index])

            formatting = self.formatting()
            self._text_layout.draw(p, txt_p - QPointF(self._hscroll, 0), formatting)

            if self._blink_status:
                self._text_layout.drawCursor(
                    p, txt_p - QPointF(self._hscroll, 0), self._cursor
                )

            self.drawTags(p, self._tags[self._editing_index + 1 :])
        else:
            self.drawTags(p, self._nonEmptyTags())

    def formatting(self) -> List[QTextLayout.FormatRange]:
        if self._select_size == 0:
            return []

        selection = QTextLayout.FormatRange()
        selection.start = self._select_start
        selection.length = self._select_size
        selection.format.setBackground(self.palette().brush(QPalette.Highlight))
        selection.format.setForeground(self.palette().brush(QPalette.HighlightedText))
        return [selection]

    def drawTags(self, p: QPainter, ts: List[Tag]) -> None:
        for it in ts:
            i_r = QRectF(it.rect.translated(-self._hscroll, 0))
            text_pos = i_r.topLeft() + QPointF(
                tag_inner_left_padding,
                self.fontMetrics().ascent()
                + ((i_r.height() - self.fontMetrics().height()) / 2),
            )

            blue = QColor(0, 96, 100, 150)
            path = QPainterPath()
            path.addRoundedRect(i_r, 4, 4)
            p.fillPath(path, blue)

            p.drawText(text_pos, it.text)

            i_cross_r = self.crossRect(i_r)

            pen = p.pen()

            pen.setWidth(2)

            p.save()
            p.setPen(pen)
            p.setRenderHint(QPainter.Antialiasing)
            p.drawLine(QLineF(i_cross_r.topLeft(), i_cross_r.bottomRight()))
            p.drawLine(QLineF(i_cross_r.bottomLeft(), i_cross_r.topRight()))
            p.restore()

    def crossRect(self, r: QRectF) -> QRectF:
        cross = QRectF(QPointF(0, 0), QSizeF(tag_cross_width, tag_cross_width))
        cross.moveCenter(QPointF(r.right() - tag_cross_width, r.center().y()))
        return cross

    def naturalWidth(self) -> float:
        return self._tags[-1].rect.right() - self._tags[0].rect.left()

    def cursorToX(self) -> Tuple[float, int]:
        return self._text_layout.lineAt(0).cursorToX(self._cursor)

    def calcHScroll(self, r: QRect) -> None:
        rect = self.cRect()
        width_used = round(self.naturalWidth()) + 1
        cix = r.x() + round(self.cursorToX()[0])
        if width_used <= rect.width():
            self._hscroll = 0
        elif cix - self._hscroll >= rect.width():
            self._hscroll = cix - rect.width() + 1
        elif cix - self._hscroll < 0 and self._hscroll < width_used:
            self._hscroll = cix
        elif width_used - self._hscroll < rect.width():
            self._hscroll = width_used - rect.width() + 1
        else:
            self._hscroll = max(0, self._hscroll)

    def timerEvent(self, event: QTimerEvent) -> None:
        if event.timerId() == self._blink_timer:
            self._blink_status = not self._blink_status
            self.update()

    def mousePressEvent(self, event: QMouseEvent) -> None:
        found = False
        for i in range(len(self._tags)):
            if self.inCrossArea(i, event.pos()):
                self._tags.pop(i)
                if i <= self._editing_index:
                    self._editing_index -= 1
                found = True
                break

            if (
                not self._tags[i]
                .rect.translated(self._hscroll, 0)
                .contains(event.pos())
            ):
                continue

            if self._editing_index == i:
                self.moveCursor(
                    self._text_layout.lineAt(0).xToCursor(
                        (
                            event.pos()
                            - self.currentRect().translated(-self._hscroll, 0).topLeft()
                        ).x()
                    ),
                    False,
                )
            else:
                self.editTag(i)

            found = True
            break

        if not found:
            self.editNewTag()
            event.accept()

        if event.isAccepted():
            self.updateDisplayText()
            self.calcRects()
            self.updateCursorBlinking()
            self.update()

    def updateCursorBlinking(self) -> None:
        self.setCursorVisible(self.cursorVisible())

    def cursorVisible(self) -> bool:
        return self._blink_timer != 0

    def resizeEvent(self, event: QResizeEvent) -> None:
        self.calcRects()

    def focusInEvent(self, event: QFocusEvent) -> None:
        self.setCursorVisible(True)
        self.updateDisplayText()
        self.calcRects()
        self.update()

    def focusOutEvent(self, event: QFocusEvent) -> None:
        self.setCursorVisible(False)
        self.updateDisplayText()
        self.calcRects()
        self.update()

    def keyPressEvent(self, event: QKeyEvent) -> None:
        event.setAccepted(False)
        unknown = False

        if event == QKeySequence.SelectAll:
            self.selectAll()
            event.accept()
        elif event == QKeySequence.SelectPreviousChar:
            self.moveCursor(
                self._text_layout.previousCursorPosition(self._cursor), True
            )
            event.accept()
        elif event == QKeySequence.SelectNextChar:
            self.moveCursor(self._text_layout.nextCursorPosition(self._cursor), True)
            event.accept()
        else:
            if event.key() == Qt.Key_Left:
                if self._cursor == 0:
                    self.editPreviousTag()
                else:
                    self.moveCursor(
                        self._text_layout.previousCursorPosition(self._cursor), False
                    )
                event.accept()
            elif event.key() == Qt.Key_Right:
                if self._cursor == len(self.currentText()):
                    self.editNextTag()
                else:
                    self.moveCursor(
                        self._text_layout.nextCursorPosition(self._cursor), False
                    )
                event.accept()
            elif event.key() == Qt.Key_Home:
                if self._cursor == 0:
                    self.editTag(0)
                else:
                    self.moveCursor(0, False)
                event.accept()
            elif event.key() == Qt.Key_End:
                if self._cursor == len(self.currentText()):
                    self.editTag(len(self._tags) - 1)
                else:
                    self.moveCursor(len(self.currentText()), False)
                event.accept()
            elif event.key() == Qt.Key_Backspace:
                if self.currentText():
                    self.removeBackwardOne()
                elif self._editing_index > 0:
                    self.editPreviousTag()
                event.accept()
            elif event.key() == Qt.Key_Space:
                if self.currentText():
                    self._tags.insert(
                        self._editing_index + 1, Tag(text="", rect=QRect())
                    )
                    self.editNextTag()
                event.accept()
            else:
                unknown = True

        # event.text() might contain newlines due to somebody pressing "return"
        eventText = event.text().strip()
        if unknown and eventText:
            if self.hasSelection():
                self.removeSelection()
            cur = self._tags[self._editing_index].text
            self._tags[self._editing_index].text = (
                cur[: self._cursor] + eventText + cur[self._cursor :]
            )
            self._cursor += len(event.text())
            event.accept()
            unknown = False

        if event.isAccepted():
            self.updateDisplayText()
            self.calcRects()
            self.updateCursorBlinking()

            self._completer.setCompletionPrefix(self.currentText())
            self._completer.complete()

            self.update()

            self.tagsEdited.emit()

    def hasSelection(self) -> bool:
        return self._select_size > 0

    def editPreviousTag(self) -> None:
        if self._editing_index > 0:
            self.setEditingIndex(self._editing_index - 1)
            self.moveCursor(len(self.currentText()), False)

    def removeSelection(self) -> None:
        self._cursor = self._select_start
        self._tags[self._editing_index].text = _remove(
            self._tags[self._editing_index].text, self._cursor, self._select_size
        )
        self.deselectAll()

    def removeBackwardOne(self) -> None:
        if self.hasSelection():
            self.removeSelection()
        else:
            self._cursor -= 1
            self._tags[self._editing_index].text = _remove(
                self._tags[self._editing_index].text, self._cursor, 1
            )

    def mouseMoveEvent(self, event: QMouseEvent) -> None:
        for i in range(len(self._tags)):
            if self.inCrossArea(i, event.pos()):
                self.setCursor(Qt.ArrowCursor)
                return
        self.setCursor(Qt.IBeamCursor)

    def inCrossArea(self, tag_index: int, point: QPoint) -> bool:
        return self.crossRect(QRectF(self._tags[tag_index].rect)).adjusted(
            -2, 0, 0, 0
        ).translated(-self._hscroll, 0).contains(point) and (
            not self.cursorVisible() or tag_index != self._editing_index
        )

    def editTag(self, i: int) -> None:
        self.setEditingIndex(i)
        self.moveCursor(len(self.currentText()), False)
