from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_configuration import parse_karabo_configuration

DESCRIPTION = "d0"

DAQ_RUN_CONTROL = "daq/run_control"
KEY = "key0"


def test_parse_karabo_configuration_minimalistic_item() -> None:
    attributi = parse_karabo_configuration(
        {
            "config_version": 2,
            "attributi_definition": {
                "run": {
                    "index": {"key": KEY, "source": DAQ_RUN_CONTROL, "type": "int"},
                }
            },
        }
    ).attributi

    assert "run" in attributi
    assert "index" in attributi["run"]
    assert attributi["run"]["index"].source == DAQ_RUN_CONTROL
    assert attributi["run"]["index"].key == KEY


def test_parse_karabo_configuration_multiple_groups() -> None:
    attributi = parse_karabo_configuration(
        {
            "config_version": 2,
            "attributi_definition": {
                "group1": {
                    "index1": {"key": KEY, "source": DAQ_RUN_CONTROL, "type": "int"},
                },
                "group2": {
                    "index2": {"key": KEY, "source": DAQ_RUN_CONTROL, "type": "int"},
                },
            },
        }
    ).attributi

    assert "group1" in attributi
    assert "index1" in attributi["group1"]
    assert "group2" in attributi
    assert "index2" in attributi["group2"]


def test_parse_karabo_configuration_source_global() -> None:
    attributi = parse_karabo_configuration(
        {
            "config_version": 2,
            "attributi_definition": {
                "group1": {
                    "source": "global",
                    "index1": {"key": KEY, "source": "local", "type": "int"},
                    "index2": {"key": KEY, "type": "int"},
                },
            },
        }
    ).attributi

    assert attributi["group1"]["index1"].source == "local"
    assert attributi["group1"]["index2"].source == "global"


def test_parse_karabo_configuration_action_global() -> None:
    """Tests if setting a global action propagates to the children, but doesn't override it"""
    attributi = parse_karabo_configuration(
        {
            "config_version": 2,
            "attributi_definition": {
                "group1": {
                    "action": "store_last",
                    "index1": {
                        "key": KEY,
                        "source": "source1",
                        "action": "check_if_constant",
                        "type": "int",
                    },
                    "index2": {"key": KEY, "source": "source1", "type": "int"},
                },
            },
        }
    ).attributi

    assert (
        attributi["group1"]["index1"].action == KaraboAttributoAction.CHECK_IF_CONSTANT
    )
    assert attributi["group1"]["index2"].action == KaraboAttributoAction.STORE_LAST


def test_parse_karabo_configuration_expected_attributi() -> None:
    config = parse_karabo_configuration(
        {
            "config_version": 2,
            "attributi_definition": {
                "group1": {
                    "index1": {"key": "key0", "source": "source1", "type": "int"},
                    "index2": {"key": "key1", "source": "source2", "type": "int"},
                },
                "group2": {
                    "index3": {"key": "key2", "source": "source3", "type": "int"},
                    "index4": {"key": "key3", "source": "source4", "type": "int"},
                    "index5": {"key": "key4", "source": "source2", "type": "int"},
                },
            },
        }
    )

    expected_attributi = config.expected_attributi

    assert "source1" in expected_attributi
    assert "source2" in expected_attributi
    assert "source3" in expected_attributi
    assert "source4" in expected_attributi

    assert "key0" in expected_attributi["source1"]
    assert "key1" in expected_attributi["source2"]
    assert "key2" in expected_attributi["source3"]
    assert "key3" in expected_attributi["source4"]

    assert expected_attributi["source1"]["key0"]["attributo"].source == "source1"
    assert expected_attributi["source2"]["key4"]["attributo"].source == "source2"
