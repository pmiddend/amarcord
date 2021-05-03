from pathlib import Path

import pytest

from amarcord.amici.xfel.karabo_configuration import parse_karabo_configuration_file
from amarcord.amici.xfel.karabo_online import attributo2karabo

KEY = "newkey"

SOURCE = "newsource"

RUN_CONTROL = "SPB_DAQ_DATA/DM/RUN_CONTROL"

STANDARD_CONFIG = Path(__file__).parent / "config.yml"


def test_attributi2karabo_simple_usage_with_success() -> None:
    config = parse_karabo_configuration_file(STANDARD_CONFIG)

    result = attributo2karabo(
        config.attributi,
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
    config = parse_karabo_configuration_file(STANDARD_CONFIG)

    with pytest.raises(ValueError):
        attributo2karabo(
            config.attributi,
            {RUN_CONTROL: {"runDetails.runId.value": "foo"}},
            "run",
            "index",
            int,
        )


def test_attributi2karabo_wrong_group() -> None:
    """
    Test if the attributo is not in the right group (daq instead of run)
    """
    config = parse_karabo_configuration_file(STANDARD_CONFIG)

    with pytest.raises(ValueError):
        attributo2karabo(
            config.attributi,
            {RUN_CONTROL: {"runDetails.runId.value": "foo"}},
            "daq",
            "index",
            int,
        )


def test_attributi2karabo_value_not_found() -> None:
    """
    Test if the attributo has the wrong key (value-invalid instead of value)
    """
    config = parse_karabo_configuration_file(STANDARD_CONFIG)

    with pytest.raises(ValueError):
        attributo2karabo(
            config.attributi,
            {RUN_CONTROL: {"runDetails.runId.value-invalid": 3}},
            "run",
            "index",
            int,
        )
