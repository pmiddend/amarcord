from pathlib import Path

import pytest

from amarcord.config import UserConfig
from amarcord.config import load_user_config
from amarcord.config import user_config_path
from amarcord.config import write_user_config


def test_load_valid_config(fs) -> None:
    fs.add_real_file(
        Path(__file__).parent / "valid-config.yml",
        target_path=user_config_path(),
    )
    c = load_user_config()
    assert c.db_url is not None
    assert c.db_url.startswith("mysql")
    assert c.proposal_id == 2696
    assert c.create_sample_data


def test_load_invalid_config(fs) -> None:
    fs.add_real_file(
        Path(__file__).parent / "invalid-config.yml",
        target_path=user_config_path(),
    )
    with pytest.raises(Exception):
        try:
            load_user_config()
        except Exception as e:
            print(e)
            raise


def test_write_config() -> None:
    input_uc = UserConfig(db_url="foobar", create_sample_data=True, proposal_id=1)
    write_user_config(input_uc)
    output_uc = load_user_config()
    assert input_uc == output_uc
