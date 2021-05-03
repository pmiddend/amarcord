import pytest

from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction

# noinspection PyProtectedMember
from amarcord.amici.xfel.karabo_configuration import _parse_attributo

IDENTIFIER = "identifier"


def test_parse_attributo_minimal_no_globals() -> None:
    result = _parse_attributo(
        identifier=IDENTIFIER,
        global_source=None,
        global_action=None,
        ai_content={"key": "key1", "source": "source1", "type": "int"},
    )

    assert result.source == "source1"
    assert result.key == "key1"
    assert result.type_ == "int"
    assert result.store


def test_parse_attributo_local_overrides_global() -> None:
    result = _parse_attributo(
        identifier=IDENTIFIER,
        global_source="global",
        global_action=KaraboAttributoAction.STORE_LAST.value,
        ai_content={
            "key": "key1",
            "source": "local_source",
            "type": "int",
            "action": KaraboAttributoAction.CHECK_IF_CONSTANT.value,
        },
    )

    assert result.source == "local_source"
    assert result.action == KaraboAttributoAction.CHECK_IF_CONSTANT


def test_parse_attributo_global_supplements_local() -> None:
    result = _parse_attributo(
        identifier=IDENTIFIER,
        global_source="global",
        global_action=KaraboAttributoAction.STORE_LAST.value,
        ai_content={
            "key": "key1",
            "type": "int",
        },
    )

    assert result.source == "global"
    assert result.action == KaraboAttributoAction.STORE_LAST


def test__parse_attributo_invalid_action() -> None:
    with pytest.raises(ValueError):
        _parse_attributo(
            identifier=IDENTIFIER,
            global_source="global",
            global_action="mist",
            ai_content={
                "key": "key1",
                "type": "int",
            },
        )


def test__parse_attributo_valid_type() -> None:
    result = _parse_attributo(
        identifier=IDENTIFIER,
        global_source=None,
        global_action=None,
        ai_content={"key": "key1", "source": "foo", "type": "int"},
    )

    assert result.type_ == "int"


def test__parse_attributo_invalid_type() -> None:
    with pytest.raises(ValueError):
        _parse_attributo(
            identifier=IDENTIFIER,
            global_source=None,
            global_action=None,
            ai_content={"key": "key1", "source": "foo", "type": "int1"},
        )
