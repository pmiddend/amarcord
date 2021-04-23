import pytest

from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_online import parse_attributo

IDENTIFIER = "identifier"


def test_parse_attributo_minimal_no_globals() -> None:
    result = parse_attributo(
        identifier=IDENTIFIER,
        global_source=None,
        global_action=None,
        action_axis=None,
        ai_content={"key": "key1", "source": "source1"},
    )

    assert result.source == "source1"
    assert result.key == "key1"
    assert result.type_ == "decimal"
    assert result.store


def test_parse_attributo_local_overrides_global() -> None:
    result = parse_attributo(
        identifier=IDENTIFIER,
        global_source="global",
        global_action=KaraboAttributoAction.STORE_LAST.value,
        action_axis=1,
        ai_content={
            "key": "key1",
            "source": "local_source",
            "action": KaraboAttributoAction.CHECK_IF_CONSTANT.value,
            "action_axis": 2,
        },
    )

    assert result.source == "local_source"
    assert result.action == KaraboAttributoAction.CHECK_IF_CONSTANT
    assert result.action_axis == 2


def test_parse_attributo_global_supplements_local() -> None:
    result = parse_attributo(
        identifier=IDENTIFIER,
        global_source="global",
        global_action=KaraboAttributoAction.STORE_LAST.value,
        action_axis=1,
        ai_content={
            "key": "key1",
        },
    )

    assert result.source == "global"
    assert result.action == KaraboAttributoAction.STORE_LAST
    assert result.action_axis == 1


def test_parse_attributo_invalid_action() -> None:
    with pytest.raises(ValueError):
        parse_attributo(
            identifier=IDENTIFIER,
            global_source="global",
            global_action="mist",
            action_axis=1,
            ai_content={
                "key": "key1",
            },
        )


def test_parse_attributo_valid_type() -> None:
    result = parse_attributo(
        identifier=IDENTIFIER,
        global_source=None,
        global_action=None,
        action_axis=None,
        ai_content={"key": "key1", "source": "foo", "type": "int"},
    )

    assert result.type_ == "int"


def test_parse_attributo_invalid_type() -> None:
    with pytest.raises(ValueError):
        parse_attributo(
            identifier=IDENTIFIER,
            global_source=None,
            global_action=None,
            action_axis=None,
            ai_content={"key": "key1", "source": "foo", "type": "int1"},
        )
