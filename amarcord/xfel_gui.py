import logging
import sys
from functools import partial

from amarcord.config import load_config
from amarcord.db.associated_table import AssociatedTable
from amarcord.modules.context import Context
from amarcord.modules.dbcontext import CreationMode, DBContext
from amarcord.db.tables import create_tables
from amarcord.db.sample_data import create_sample_data
from amarcord.modules.spb.attributi_crud import AttributiCrud
from amarcord.modules.spb.factories import (
    proposal_chooser,
    retrieve_proposal_ids,
    run_details,
    run_table,
)
from amarcord.db.proposal_id import ProposalId
from amarcord.modules.spb.samples import Samples
from amarcord.modules.spb.targets import Targets
from amarcord.modules.uicontext import UIContext

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

if __name__ == "__main__":
    config = load_config()

    dbcontext = DBContext(config["db"]["url"])
    context = Context(config=config, ui=UIContext(sys.argv), db=dbcontext)

    tables = create_tables(context.db)

    dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

    create_sample_data(dbcontext, tables)

    proposal_ids = retrieve_proposal_ids(context, tables)

    proposal_id = config.get("proposal_id", None)

    if proposal_id is None:
        if len(proposal_ids) == 1:
            proposal_id = next(iter(proposal_ids))
        else:
            proposal_id = proposal_chooser(proposal_ids)
            if proposal_id is None:
                sys.exit(0)
    else:
        proposal_id = ProposalId(proposal_id)
    context.ui.set_application_suffix(f"proposal {proposal_id}")

    targets_index = context.ui.register_tab(
        "Targets",
        Targets(context, tables),
        context.ui.icon("SP_MediaStop"),
    )

    samples_tab = Samples(context, tables)
    samples_index = context.ui.register_tab(
        "Samples",
        samples_tab,
        context.ui.icon("SP_DialogResetButton"),
    )

    run_table_tab = run_table(context, tables, proposal_id)
    context.ui.register_tab(
        "Experiment Overview",
        run_table_tab,
        context.ui.icon("SP_ComputerIcon"),
    )
    run_details_tab = run_details(context, tables, proposal_id)
    # run_details_tab.run_changed.connect(run_table_tab.run_changed)
    run_details_index = context.ui.register_tab(
        "Run details",
        run_details_tab,
        context.ui.icon("SP_FileDialogContentsView"),
    )

    def change_run(
        run_id: int,
    ) -> None:
        run_details_tab.select_run(run_id)
        context.ui.select_tab(run_details_index)

    run_table_tab.run_selected.connect(change_run)

    attributi_crud_tab = AttributiCrud(context, tables)
    attributi_crud_index = context.ui.register_tab(
        "Attributi",
        attributi_crud_tab,
        context.ui.icon("SP_DirIcon"),
    )

    def open_new_attributo(table: AssociatedTable):
        attributi_crud_tab.regenerate_for_table(table)
        context.ui.select_tab(attributi_crud_index)

    run_details_tab.new_attributo.connect(
        partial(open_new_attributo, AssociatedTable.RUN)
    )
    samples_tab.new_attributo.connect(
        partial(open_new_attributo, AssociatedTable.SAMPLE)
    )

    context.ui.exec_()
