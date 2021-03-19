import os
import sys
from pathlib import Path
from typing import Any
from typing import Dict

import yaml


def load_config() -> Dict[str, Any]:
    config_file_name = os.environ.get("AMARCORD_CONFIG_FILE", "config.yml")
    config_file = Path(config_file_name)
    if not config_file.exists():
        sys.stderr.write(
            f"Expected a configuration file called “{config_file_name}” but didn't find one\n"
        )
        sys.exit(1)
    with config_file.open() as f:
        return yaml.load(f.read(), Loader=yaml.SafeLoader)
