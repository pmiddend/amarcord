import logging
import os
import sys
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import TypedDict
from typing import Union

import yaml
from xdg import xdg_config_home

from amarcord.python_schema import load_and_validate

CONFIG_YML = xdg_config_home() / "AMARCORD" / "config.yml"

logger = logging.getLogger(__name__)


class UserConfig(TypedDict):
    db_url: Optional[str]
    create_sample_data: Optional[bool]
    proposal_id: Optional[int]


def remove_user_config() -> None:
    CONFIG_YML.unlink(missing_ok=True)


def load_user_config() -> UserConfig:
    if not CONFIG_YML.exists():
        return UserConfig(db_url=None, create_sample_data=None, proposal_id=None)
    with CONFIG_YML.open("r") as f:
        result: Union[List[str], Any] = load_and_validate(f.read(), UserConfig)
        if isinstance(result, list):
            raise Exception(
                f"configuration file {CONFIG_YML} invalid: " + (", ".join(result))
            )
        return result


def write_user_config(uc: UserConfig) -> None:
    logger.info("updating user configuration...")
    CONFIG_YML.parent.mkdir(parents=True, exist_ok=True)
    with CONFIG_YML.open("w") as f:
        f.write(yaml.dump(uc, Dumper=yaml.Dumper))


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
