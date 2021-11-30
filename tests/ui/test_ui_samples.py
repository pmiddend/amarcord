from typing import Any

from amarcord.db.proposal_id import ProposalId
from amarcord.db.tables import create_tables
from amarcord.modules.context import Context
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.spb.samples import Samples


def test_add_sample(qtbot: Any) -> None:
    dbcontext = DBContext("sqlite://")
    uicontext = None
    config = {"filesystem": {"base_path": "/tmp"}}
    # noinspection PyTypeChecker
    context = Context(config=config, ui=uicontext, db=dbcontext)  # type: ignore
    tables = create_tables(context.db)
    dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)
    widget = Samples(context, tables, ProposalId(1))
    widget.show()

    qtbot.addWidget(widget)
