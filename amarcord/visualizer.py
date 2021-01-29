import sys
import os
from pathlib import Path
import logging
import yaml
from amarcord.modules.context import Context
from amarcord.modules.uicontext import UIContext
from amarcord.modules.xfel_visualizer import XFELVisualizer

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
        context = Context(config=config, ui=UIContext(sys.argv))
        visualizer = XFELVisualizer(context)
        context.ui.exec_()
