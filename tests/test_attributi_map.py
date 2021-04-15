from typing import Final

import pytest

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDouble
from amarcord.db.constants import MANUAL_SOURCE_NAME
from amarcord.db.constants import OFFLINE_SOURCE_NAME
from amarcord.db.constants import ONLINE_SOURCE_NAME
from amarcord.db.dbattributo import DBAttributo
from amarcord.db.raw_attributi_map import RawAttributiMap
from amarcord.numeric_range import NumericRange

ATTRIBUTO_ID = AttributoId("test")


def test_validate_double() -> None:
    metadata = {
        ATTRIBUTO_ID: DBAttributo(
            ATTRIBUTO_ID,
            "description",
            AssociatedTable.SAMPLE,
            AttributoTypeDouble(NumericRange(0, True, 10, True)),
        )
    }

    raw_map = RawAttributiMap({})
    raw_map.set_single_manual(ATTRIBUTO_ID, -10)

    with pytest.raises(ValueError):
        AttributiMap(metadata, raw_map)

    raw_map.set_single_manual(ATTRIBUTO_ID, 5)

    AttributiMap(metadata, raw_map)


def test_validate_choice() -> None:
    metadata = {
        ATTRIBUTO_ID: DBAttributo(
            ATTRIBUTO_ID,
            "description",
            AssociatedTable.SAMPLE,
            AttributoTypeChoice([("a", "a"), ("b", "b")]),
        )
    }

    raw_map = RawAttributiMap({})
    raw_map.set_single_manual(ATTRIBUTO_ID, "c")

    with pytest.raises(ValueError):
        AttributiMap(metadata, raw_map)

    raw_map.set_single_manual(ATTRIBUTO_ID, "a")

    AttributiMap(metadata, raw_map)


def test_online_offline_manual_order() -> None:
    m = RawAttributiMap({})
    manual_constant: Final = 1
    m.append_to_source(
        MANUAL_SOURCE_NAME,
        {
            AttributoId("all"): manual_constant,
            AttributoId("manual_and_online"): manual_constant,
            AttributoId("manual_and_offline"): manual_constant,
            AttributoId("only_manual"): manual_constant,
        },
    )
    offline_constant: Final = 2
    m.append_to_source(
        OFFLINE_SOURCE_NAME,
        {
            AttributoId("all"): offline_constant,
            AttributoId("manual_and_offline"): offline_constant,
            AttributoId("online_and_offline"): offline_constant,
            AttributoId("only_offline"): offline_constant,
        },
    )
    online_constant: Final = 3
    m.append_to_source(
        ONLINE_SOURCE_NAME,
        {
            AttributoId("all"): online_constant,
            AttributoId("manual_and_online"): online_constant,
            AttributoId("online_and_offline"): online_constant,
            AttributoId("only_online"): online_constant,
        },
    )

    assert m.select_int(AttributoId("all")) == manual_constant
    assert m.select_int(AttributoId("manual_and_online")) == manual_constant
    assert m.select_int(AttributoId("manual_and_offline")) == manual_constant
    assert m.select_int(AttributoId("online_and_offline")) == offline_constant
    assert m.select_int(AttributoId("only_manual")) == manual_constant
    assert m.select_int(AttributoId("only_offline")) == offline_constant
    assert m.select_int(AttributoId("only_online")) == online_constant


def test_remove_manual_attributo() -> None:
    m = RawAttributiMap({})
    m.set_single_manual(AttributoId("test"), 1)
    assert m.select_int(AttributoId("test")) == 1
    m.remove_manual_attributo(AttributoId("test"))
    assert m.select_int(AttributoId("test")) is None


def test_remove_attributo() -> None:
    m = RawAttributiMap({})
    m.append_single_to_source(ONLINE_SOURCE_NAME, AttributoId("test"), 1)
    assert m.select_int(AttributoId("test")) == 1
    m.remove_attributo(AttributoId("test"), ONLINE_SOURCE_NAME)
    assert m.select_int(AttributoId("test")) is None
