from pathlib import Path
from typing import Any
from typing import Dict

import pytest

from amarcord.amici.xfel.karabo_online import attributo2karabo
from amarcord.amici.xfel.karabo_online import load_configuration
from amarcord.amici.xfel.karabo_online import parse_configuration

KEY = "newkey"

SOURCE = "newsource"

RUN_CONTROL = "SPB_DAQ_DATA/DM/RUN_CONTROL"


def load_standard_config() -> Dict[str, Any]:
    config_path = Path(__file__).parent / "config.yml"
    config = load_configuration(str(config_path))
    return config["Karabo_bridge"]["attributi_definition"]


def test_attributi2karabo_simple_usage_with_success() -> None:
    attributi, _ = parse_configuration(load_standard_config())

    result = attributo2karabo(
        attributi,
        {RUN_CONTROL: {"runDetails.runId.value": 3}},
        "run",
        "index",
        int,
    )

    assert result.value == 3
    assert result.attributo.identifier == "index"


def test_attributi2karabo_simple_usage_with_wrong_type() -> None:
    """
    Test if the attributo is found in the Karabo data, but has the wrong type (string instead of int).
    """
    attributi, _ = parse_configuration(load_standard_config())

    with pytest.raises(ValueError):
        attributo2karabo(
            attributi,
            {RUN_CONTROL: {"runDetails.runId.value": "foo"}},
            "run",
            "index",
            int,
        )


def test_attributi2karabo_wrong_group() -> None:
    """
    Test if the attributo is not in the right group (daq instead of run)
    """
    attributi, _ = parse_configuration(load_standard_config())

    with pytest.raises(ValueError):
        attributo2karabo(
            attributi,
            {RUN_CONTROL: {"runDetails.runId.value": "foo"}},
            "daq",
            "index",
            int,
        )


def test_attributi2karabo_value_not_found() -> None:
    """
    Test if the attributo has the wrong key (value-invalid instead of value)
    """
    attributi, _ = parse_configuration(load_standard_config())

    with pytest.raises(ValueError):
        attributo2karabo(
            attributi,
            {RUN_CONTROL: {"runDetails.runId.value-invalid": 3}},
            "run",
            "index",
            int,
        )
