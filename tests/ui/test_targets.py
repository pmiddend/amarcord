from typing import Any

from PyQt5.QtCore import Qt

from amarcord.db.tables import create_tables
from amarcord.modules.context import Context
from amarcord.modules.dbcontext import CreationMode, DBContext
from amarcord.modules.spb.targets import Targets


def test_target_tab(qtbot: Any) -> None:
    dbcontext = DBContext("sqlite://")
    uicontext = None
    # noinspection PyTypeChecker
    context = Context(config={}, ui=uicontext, db=dbcontext)  # type: ignore
    tables = create_tables(context.db)
    dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)
    widget = Targets(context, tables)
    widget.show()

    qtbot.addWidget(widget)

    assert not widget._add_button.isEnabled()
    assert widget._target_table.rowCount() == 0

    qtbot.keyClicks(widget._short_name_edit, "MPro")

    assert not widget._add_button.isEnabled()

    qtbot.keyClicks(widget._name_edit, "Main Protease")

    assert widget._add_button.isEnabled()

    qtbot.mouseClick(widget._add_button, Qt.LeftButton)

    assert widget._short_name_edit.text() == ""
    assert widget._name_edit.text() == ""
    assert widget._target_table.rowCount() == 1
