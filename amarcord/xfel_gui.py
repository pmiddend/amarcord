import sys
import os
from pathlib import Path
import logging
from functools import partial
import yaml
from amarcord.modules.context import Context
from amarcord.modules.uicontext import UIContext
from amarcord.modules.spb.tables import create_tables, create_sample_data
from amarcord.modules.spb import run_table
from amarcord.modules.spb import run_details
from amarcord.modules.spb.run_details import RunDetails
from amarcord.modules.dbcontext import DBContext, CreationMode

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

if __name__ == "__main__":
    config_file_name = os.environ.get("AMARCORD_CONFIG_FILE", "config.yml")
    config_file = Path(config_file_name)
    if not config_file.exists():
        print(
            f"Expected a configuration file called “{config_file_name}” but didn't find one"
        )
        sys.exit(1)
    with config_file.open() as f:
        config = yaml.load(f.read(), Loader=yaml.SafeLoader)
        dbcontext = DBContext(config["db"]["url"])
        context = Context(config=config, ui=UIContext(sys.argv), db=dbcontext)

        tables = create_tables(context.db)

        dbcontext.create_all(creation_mode=CreationMode.CHECK_FIRST)

        create_sample_data(dbcontext, tables)

        run_table_tab = run_table(context, tables)
        context.ui.register_tab(
            "Runs",
            run_table_tab,
            context.ui.icon("SP_ComputerIcon"),
        )
        run_details_tab = run_details(context, tables)
        run_details_tab.run_changed.connect(run_table_tab.run_changed)
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
        context.ui.exec_()
