import pytest

from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_id import AttributoId
from amarcord.db.attributo_type import AttributoTypeChoice
from amarcord.db.attributo_type import AttributoTypeDouble
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
