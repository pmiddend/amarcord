import argparse
import logging
import sys
from dataclasses import dataclass
from functools import partial

from PyQt5.QtCore import Qt
from PyQt5.QtGui import QIcon
from PyQt5.QtWidgets import QMessageBox

import resources
from amarcord.config import load_user_config
from amarcord.config import remove_user_config
from amarcord.config import write_user_config
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.constants import CONTACT_INFO
from amarcord.db.proposal_id import ProposalId
from amarcord.db.sample_data import create_sample_data
from amarcord.db.tables import create_tables
from amarcord.modules.connection_wizard import show_connection_dialog
from amarcord.modules.context import Context
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.spb.analysis_view import AnalysisView
from amarcord.modules.spb.attributi_crud import AttributiCrud
from amarcord.modules.spb.factories import proposal_chooser
from amarcord.modules.spb.factories import retrieve_proposal_ids
from amarcord.modules.spb.factories import run_details
from amarcord.modules.spb.factories import run_table
from amarcord.modules.spb.samples import Samples
from amarcord.modules.spb.targets import Targets
from amarcord.modules.uicontext import UIContext

try:
    # noinspection PyStatementEffect
    resources.qInitResources
except:
    pass

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)


@dataclass(frozen=True)
class Arguments:
    admin_mode: bool


def parse_arguments() -> Arguments:
    parser = argparse.ArgumentParser(description="Main GUI for AMARCORD")

    parser.add_argument("--admin", action="store_true")

    args = parser.parse_args()

    return Arguments(args.admin)


class XFELGui:
    def __init__(self) -> None:
        self._ui_context = UIContext(sys.argv)
        self._user_config = load_user_config()

        cli_args = parse_arguments()

        self.restart = False

        db_url = self._user_config["db_url"]
        if db_url is None:
            db_url = show_connection_dialog(cli_args.admin_mode, self._ui_context)
            if db_url is None:
                sys.exit(1)

        self._db_context = DBContext(db_url)
        self._context = Context(self._user_config, self._ui_context, self._db_context)
        self._tables = create_tables(self._db_context)
        self._db_context.create_all(creation_mode=CreationMode.CHECK_FIRST)
        if self._user_config["create_sample_data"]:
            create_sample_data(self._db_context, self._tables)
        self._proposal_ids = retrieve_proposal_ids(self._db_context, self._tables)

        if not self._proposal_ids:
            box = QMessageBox(  # type: ignore
                QMessageBox.Critical,
                "No proposals",
                f"<p>There are no proposals in the database! This means it has not been initialized properly.</p><p>If "
                f"you know how to do "
                f"that, add a proposal. If you don't know what's going on, please contact:</p>{CONTACT_INFO}",
                QMessageBox.Ok,
                None,
            )
            box.setTextFormat(Qt.RichText)
            box.exec()
            # This is a "hack" for now. Ideally, we would want to display something like "Change database" in case there
            # are no proposals anymore. But we assume this doesn't happen often, so let's just remove the user config.
            remove_user_config()
            sys.exit(1)

        self._proposal_id = self._user_config["proposal_id"]

        if self._proposal_id is None:
            if len(self._proposal_ids) == 1:
                self._proposal_id = next(iter(self._proposal_ids))
            else:
                self._proposal_id = proposal_chooser(self._proposal_ids)
                if self._proposal_id is None:
                    sys.exit(0)
        else:
            if self._proposal_id not in self._proposal_ids:
                QMessageBox.critical(
                    None,
                    "Proposal not found",
                    "I have a prior proposal with ID {self._proposal_Id}, but I cannot find it in the current DB. I "
                    "will delete your user configuration and you have to restart AMARCORD.",
                )
                remove_user_config()
                sys.exit(1)
            self._proposal_id = ProposalId(self._proposal_id)
        self._user_config["proposal_id"] = self._proposal_id
        self._user_config["db_url"] = db_url
        write_user_config(self._user_config)
        self._ui_context.add_menu_item(
            "Change &database",
            self._slot_change_database,
        )
        self._ui_context.add_menu_item(
            "&Change proposal",
            self._slot_change_proposal,
        )
        self._ui_context.set_application_suffix(f"proposal {self._proposal_id}")

        _targets_index = self._ui_context.register_tab(
            "Targets",
            Targets(self._context, self._tables),
            QIcon(":/icons/bullseye-solid.png"),
        )

        samples_tab = Samples(self._context, self._tables)
        _samples_index = self._ui_context.register_tab(
            "Samples", samples_tab, QIcon(":/icons/flask-solid.png")
        )

        run_table_tab = run_table(self._context, self._tables, self._proposal_id)
        self._ui_context.register_tab(
            "Experiment Overview", run_table_tab, QIcon(":/icons/table-solid.png")
        )
        run_details_tab = run_details(self._context, self._tables, self._proposal_id)
        # run_details_tab.run_changed.connect(run_table_tab.run_changed)
        run_details_index = self._ui_context.register_tab(
            "Run details", run_details_tab, QIcon(":/icons/running-solid.png")
        )

        def change_run(
            run_id: int,
        ) -> None:
            run_details_tab.select_run(run_id)
            self._ui_context.select_tab(run_details_index)

        run_table_tab.run_selected.connect(change_run)

        analysis_tab = AnalysisView(
            self._context, self._tables, proposal_id=self._proposal_id
        )
        _analysis_index = self._ui_context.register_tab(
            "Analysis", analysis_tab, QIcon(":/icons/chart-line-solid.png")
        )

        attributi_crud_tab = AttributiCrud(
            self._context, self._tables, self._proposal_id
        )
        attributi_crud_index = self._ui_context.register_tab(
            "Attributi", attributi_crud_tab, QIcon(":/icons/book-solid.png")
        )

        def open_new_attributo(table: AssociatedTable):
            attributi_crud_tab.regenerate_for_table(table)
            self._ui_context.select_tab(attributi_crud_index)

        run_details_tab.new_attributo.connect(
            partial(open_new_attributo, AssociatedTable.RUN)
        )
        samples_tab.new_attributo.connect(
            partial(open_new_attributo, AssociatedTable.SAMPLE)
        )

    def exec(self) -> None:
        self._ui_context.exec_()

    def _slot_change_database(self) -> None:
        self._user_config["proposal_id"] = None
        self._user_config["db_url"] = None
        write_user_config(self._user_config)
        self.restart = True
        self._ui_context.close()

    def _slot_change_proposal(self) -> None:
        self._user_config["proposal_id"] = None
        write_user_config(self._user_config)
        self.restart = True
        self._ui_context.close()


def mymain():
    while True:
        gui = XFELGui()
        gui.exec()
        if not gui.restart:
            break
        gui = XFELGui()
        gui.exec()


if __name__ == "__main__":
    mymain()
