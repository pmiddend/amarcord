from typing import Final, Optional
from typing import List
from typing import Tuple
from dataclasses import dataclass

from PyQt5.QtWidgets import QWidget
from PyQt5.QtWidgets import QStyle
from PyQt5.QtWidgets import QStyleOptionFrame, QApplication
from PyQt5.QtCore import QSize, QTimer
from PyQt5.QtCore import pyqtSignal
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


top_text_margin: Final = 1
bottom_text_margin: Final = 1
left_text_margin: Final = 1
right_text_margin: Final = 1

vertical_margin: Final = 3
bottom_margin: Final = 1
top_margin: Final = 1

horizontal_margin: Final = 3
left_margin: Final = 1
right_margin: Final = 1

tag_spacing: Final = 3
tag_inner_left_padding: Final = 3
tag_inner_right_padding: Final = 4
tag_cross_width: Final = 4
tag_cross_spacing: Final = 2


def _cross_rect(r: QRectF) -> QRectF:
    cross = QRectF(QPointF(0, 0), QSizeF(tag_cross_width, tag_cross_width))
    cross.moveCenter(QPointF(r.right() - tag_cross_width, r.center().y()))
    return cross


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
        self._blink_timer = QTimer()
        self._blink_timer.setInterval(
            QGuiApplication.styleHints().cursorFlashTime() // 2
        )
        self._blink_timer.timeout.connect(self._blink_timeout)
        self._blink_status = True

        self.setSizePolicy(QSizePolicy.Expanding, QSizePolicy.Fixed)
        self.setFocusPolicy(Qt.StrongFocus)
        self.setCursor(Qt.IBeamCursor)
        self.setAttribute(Qt.WA_InputMethodEnabled, True)
        self.setMouseTracking(True)
        self._completer = QCompleter()
        self.setup_completer()
        self.set_cursor_visible(self.hasFocus())
        self.update_display_text()

    def set_cursor_visible(self, visible: bool) -> None:
        if self._blink_timer.isActive():
            self._blink_timer.stop()
            self._blink_status = True

        if visible:
            self._blink_timer.start()
        else:
            self._blink_status = False

    def setup_completer(self) -> None:
        self._completer.setWidget(self)
        self._completer.activated.connect(self._completer_activated)

    def current_text(self) -> str:
        return self._tags[self._editing_index].text

    def _completer_activated(self, t: str) -> None:
        self._tags[self._editing_index].text = t
        self.move_cursor(len(self.current_text()), False)
        self.update_display_text()
        self.calc_rects()
        self.update()
        # self.tagsEdited.emit()

    def update_display_text(self) -> None:
        self._text_layout.clearLayout()
        self._text_layout.setText(self.current_text())
        self._text_layout.beginLayout()
        self._text_layout.createLine()
        self._text_layout.endLayout()

    def init_style_option(self, option: QStyleOptionFrame) -> None:
        option.initFrom(self)
        option.rect = self.contentsRect()
        option.lineWidth = self.style().pixelMetric(
            QStyle.PM_DefaultFrameWidth, option, self
        )
        option.midLineWidth = 0
        option.state |= QStyle.State_Sunken  # type: ignore
        option.features = QStyleOptionFrame.None_

    def c_rect(self) -> QRect:
        panel = QStyleOptionFrame()
        self.init_style_option(panel)
        r = self.style().subElementRect(QStyle.SE_LineEditContents, panel, self)
        r.adjust(
            left_text_margin, top_text_margin, -right_text_margin, -bottom_text_margin
        )
        return r

    def calc_rects(self) -> None:
        r = self.c_rect()
        lt = r.topLeft()

        if self.cursor_visible():
            self.calc_rects_2(lt, r.height(), self._tags[0 : self._editing_index])
            self.calc_editor_rect(lt, r.height())
            self.calc_rects_2(lt, r.height(), self._tags[self._editing_index + 1 :])
        else:
            self.calc_rects_2(lt, r.height(), self._non_empty_tags())

    def _non_empty_tags(self) -> List[Tag]:
        return [f for f in self._tags if f.text != ""]

    def calc_editor_rect(self, lt: QPoint, height: int) -> None:
        w = (
            self.fontMetrics().horizontalAdvance(self._text_layout.text())
            + tag_inner_left_padding
            + tag_inner_right_padding
        )
        self._tags[self._editing_index].rect = QRect(lt, QSize(w, height))
        lt += QPoint(w + tag_spacing, 0)

    def calc_rects_2(self, lt: QPoint, height: int, range_: List[Tag]) -> None:
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

    def move_cursor(self, pos: int, mark: bool) -> None:
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
            self.deselect_all()

        self._cursor = pos

    def select_all(self) -> None:
        self._select_start = 0
        self._select_size = len(self.current_text())

    def deselect_all(self) -> None:
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
            + top_margin
            + bottom_margin
        )
        w = (
            fm.boundingRect("x").width() * 17
            + 2 * horizontal_margin
            + left_margin
            + right_margin
        )
        opt = QStyleOptionFrame()
        self.init_style_option(opt)
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
            + top_margin
            + bottom_margin
        )
        w = fm.maxWidth() + left_margin + right_margin

        opt = QStyleOptionFrame()
        self.init_style_option(opt)
        return self.style().sizeFromContents(
            QStyle.CT_LineEdit,
            opt,
            QSize(w, h).expandedTo(QApplication.globalStrut()),
            self,
        )

    def completion(self, completions: List[str]) -> None:
        self._completer = QCompleter(completions)
        self.setup_completer()

    def edit_next_tag(self) -> None:
        if self._editing_index < len(self._tags) - 1:
            self.set_editing_index(self._editing_index + 1)
            self.move_cursor(0, False)

    def edit_new_tag(self) -> None:
        self._tags.append(Tag(text="", rect=QRect()))
        self.set_editing_index(len(self._tags) - 1)
        self.move_cursor(0, False)

    def set_editing_index(self, i: int) -> None:
        if not self.current_text():
            self._tags.pop(self._editing_index)
            if self._editing_index <= i:
                i -= 1
        self._editing_index = i

    def tags_str(self) -> List[str]:
        return [v.text for v in self._tags if v.text != ""]

    def tags(self, tags: List[str]) -> None:
        self._tags = [Tag(text="", rect=QRect())]
        self._tags.extend(Tag(t, QRect()) for t in tags)
        self._editing_index = 0
        self.move_cursor(0, False)
        self.edit_new_tag()
        self.update_display_text()
        self.calc_rects()

        self.update()

    def current_rect(self) -> QRect:
        return self._tags[self._editing_index].rect

    def paintEvent(self, event: QPaintEvent) -> None:
        p = QPainter(self)

        panel = QStyleOptionFrame()
        self.init_style_option(panel)

        self.style().drawPrimitive(QStyle.PE_PanelLineEdit, panel, p, self)

        rect = self.c_rect()

        p.setClipRect(rect)

        if self.cursor_visible():
            r = self.current_rect()
            txt_p = QPointF(r.topLeft()) + QPointF(
                tag_inner_left_padding, ((r.height() - self.fontMetrics().height()) / 2)
            )

            self.calc_h_scroll(r)

            self.draw_tags(p, self._tags[0 : self._editing_index])

            formatting = self.formatting()
            self._text_layout.draw(p, txt_p - QPointF(self._hscroll, 0), formatting)

            if self._blink_status:
                self._text_layout.drawCursor(
                    p, txt_p - QPointF(self._hscroll, 0), self._cursor
                )

            self.draw_tags(p, self._tags[self._editing_index + 1 :])
        else:
            self.draw_tags(p, self._non_empty_tags())

    def formatting(self) -> List[QTextLayout.FormatRange]:
        if self._select_size == 0:
            return []

        selection = QTextLayout.FormatRange()
        selection.start = self._select_start
        selection.length = self._select_size
        selection.format.setBackground(self.palette().brush(QPalette.Highlight))
        selection.format.setForeground(self.palette().brush(QPalette.HighlightedText))
        return [selection]

    def draw_tags(self, p: QPainter, ts: List[Tag]) -> None:
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

            i_cross_r = _cross_rect(i_r)

            pen = p.pen()

            pen.setWidth(2)

            p.save()
            p.setPen(pen)
            p.setRenderHint(QPainter.Antialiasing)
            p.drawLine(QLineF(i_cross_r.topLeft(), i_cross_r.bottomRight()))
            p.drawLine(QLineF(i_cross_r.bottomLeft(), i_cross_r.topRight()))
            p.restore()

    def natural_width(self) -> float:
        return self._tags[-1].rect.right() - self._tags[0].rect.left()

    def cursor_to_x(self) -> Tuple[float, int]:
        return self._text_layout.lineAt(0).cursorToX(self._cursor)

    def calc_h_scroll(self, r: QRect) -> None:
        rect = self.c_rect()
        width_used = round(self.natural_width()) + 1
        cix = r.x() + round(self.cursor_to_x()[0])
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

    def _blink_timeout(self) -> None:
        self._blink_status = not self._blink_status
        self.update()

    def mousePressEvent(self, event: QMouseEvent) -> None:
        found = False
        for i in range(len(self._tags)):
            if self.in_cross_area(i, event.pos()):
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
                self.move_cursor(
                    self._text_layout.lineAt(0).xToCursor(
                        (
                            event.pos()
                            - self.current_rect()
                            .translated(-self._hscroll, 0)
                            .topLeft()
                        ).x()
                    ),
                    False,
                )
            else:
                self.edit_tag(i)

            found = True
            break

        if not found:
            self.edit_new_tag()
            event.accept()

        if event.isAccepted():
            self.update_display_text()
            self.calc_rects()
            self.update_cursor_blinking()
            self.update()
            # self.tagsEdited.emit()

    def update_cursor_blinking(self) -> None:
        self.set_cursor_visible(self.cursor_visible())

    def cursor_visible(self) -> bool:
        return self._blink_timer.isActive()

    def resizeEvent(self, event: QResizeEvent) -> None:
        self.calc_rects()

    def focusInEvent(self, event: QFocusEvent) -> None:
        self.set_cursor_visible(True)
        self.update_display_text()
        self.calc_rects()
        self.update()

    def focusOutEvent(self, event: QFocusEvent) -> None:
        self.set_cursor_visible(False)
        self.update_display_text()
        self.calc_rects()
        self.update()
        self.tagsEdited.emit()

    def keyPressEvent(self, event: QKeyEvent) -> None:
        event.setAccepted(False)
        unknown = False

        if event == QKeySequence.SelectAll:  # type:ignore
            self.select_all()
            event.accept()
        elif event == QKeySequence.SelectPreviousChar:  # type:ignore
            self.move_cursor(
                self._text_layout.previousCursorPosition(self._cursor), True
            )
            event.accept()
        elif event == QKeySequence.SelectNextChar:  # type:ignore
            self.move_cursor(self._text_layout.nextCursorPosition(self._cursor), True)
            event.accept()
        else:
            if event.key() == Qt.Key_Left:
                if self._cursor == 0:
                    self.edit_previous_tag()
                else:
                    self.move_cursor(
                        self._text_layout.previousCursorPosition(self._cursor), False
                    )
                event.accept()
            elif event.key() == Qt.Key_Right:
                if self._cursor == len(self.current_text()):
                    self.edit_next_tag()
                else:
                    self.move_cursor(
                        self._text_layout.nextCursorPosition(self._cursor), False
                    )
                event.accept()
            elif event.key() == Qt.Key_Home:
                if self._cursor == 0:
                    self.edit_tag(0)
                else:
                    self.move_cursor(0, False)
                event.accept()
            elif event.key() == Qt.Key_End:
                if self._cursor == len(self.current_text()):
                    self.edit_tag(len(self._tags) - 1)
                else:
                    self.move_cursor(len(self.current_text()), False)
                event.accept()
            elif event.key() == Qt.Key_Backspace:
                if self.current_text():
                    self.remove_backward_one()
                elif self._editing_index > 0:
                    self.edit_previous_tag()
                event.accept()
            elif event.key() == Qt.Key_Space:
                if self.current_text():
                    self._tags.insert(
                        self._editing_index + 1, Tag(text="", rect=QRect())
                    )
                    self.edit_next_tag()
                event.accept()
            else:
                unknown = True

        # event.text() might contain newlines due to somebody pressing "return"
        eventText = event.text().strip()
        if unknown and eventText:
            if self.has_selection():
                self.remove_selection()
            cur = self._tags[self._editing_index].text
            self._tags[self._editing_index].text = (
                cur[: self._cursor] + eventText + cur[self._cursor :]
            )
            self._cursor += len(event.text())
            event.accept()

        if event.isAccepted():
            self.update_display_text()
            self.calc_rects()
            self.update_cursor_blinking()

            self._completer.setCompletionPrefix(self.current_text())
            self._completer.complete()

            self.update()

    def has_selection(self) -> bool:
        return self._select_size > 0

    def edit_previous_tag(self) -> None:
        if self._editing_index > 0:
            self.set_editing_index(self._editing_index - 1)
            self.move_cursor(len(self.current_text()), False)

    def remove_selection(self) -> None:
        self._cursor = self._select_start
        self._tags[self._editing_index].text = _remove(
            self._tags[self._editing_index].text, self._cursor, self._select_size
        )
        self.deselect_all()

    def remove_backward_one(self) -> None:
        if self.has_selection():
            self.remove_selection()
        else:
            self._cursor -= 1
            self._tags[self._editing_index].text = _remove(
                self._tags[self._editing_index].text, self._cursor, 1
            )

    def mouseMoveEvent(self, event: QMouseEvent) -> None:
        for i in range(len(self._tags)):
            if self.in_cross_area(i, event.pos()):
                self.setCursor(Qt.ArrowCursor)
                return
        self.setCursor(Qt.IBeamCursor)

    def in_cross_area(self, tag_index: int, point: QPoint) -> bool:
        return _cross_rect(QRectF(self._tags[tag_index].rect)).adjusted(
            -2, 0, 0, 0
        ).translated(-self._hscroll, 0).contains(point) and (
            not self.cursor_visible() or tag_index != self._editing_index
        )

    def edit_tag(self, i: int) -> None:
        self.set_editing_index(i)
        self.move_cursor(len(self.current_text()), False)
