import logging
from pathlib import Path
from typing import Optional

import yaml
from pydantic import BaseModel
from xdg import xdg_config_home


# this is deliberately a function so that it neatly works with pyfakefs (and perforamnce doesn't matter)
def user_config_path() -> Path:
    return xdg_config_home() / "AMARCORD" / "config.yml"


logger = logging.getLogger(__name__)


class UserConfig(BaseModel):
    db_url: Optional[str]
    create_sample_data: Optional[bool]
    proposal_id: Optional[int]


def remove_user_config() -> None:
    user_config_path().unlink(missing_ok=True)


def load_user_config() -> UserConfig:
    if not user_config_path().exists():
        return UserConfig(db_url=None, create_sample_data=None, proposal_id=None)
    with user_config_path().open("r") as f:
        return UserConfig(**yaml.load(f, Loader=yaml.SafeLoader))


def write_user_config(uc: UserConfig) -> None:
    user_config_path().parent.mkdir(parents=True, exist_ok=True)
    with user_config_path().open("w") as f:
        f.write(yaml.dump(uc.dict(), Dumper=yaml.Dumper))
