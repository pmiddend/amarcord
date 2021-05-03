import datetime
from typing import Final

import numpy as np

from amarcord.amici.xfel.karabo_attributo import KaraboAttributo
from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_general import karabo_attributi_to_attributi_map
from amarcord.db.attributo_id import AttributoId
from amarcord.db.raw_attributi_map import RawAttributiMap

ATTRIBUTO_ID = "attributo_id"


def test_karabo_attributi_to_attributi_map_simple_int() -> None:
    """Test if the conversion of a single Karabo int attributo to a DB attributo works"""
    VALUE: Final = 1
    amap, _images = karabo_attributi_to_attributi_map(
        "online",
        RawAttributiMap({}),
        {
            "ignore": {
                "ignore": KaraboAttributo(
                    identifier=ATTRIBUTO_ID,
                    source="source",
                    key="key",
                    description="description",
                    type_="int",
                    karabo_type=None,
                    store=True,
                    action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
                    processor=None,
                    unit="",
                    filling_value=None,
                    value=VALUE,
                    role=None,
                )
            }
        },
        existing_db_attributi={AttributoId(ATTRIBUTO_ID)},
    )

    assert amap.select_int(AttributoId(ATTRIBUTO_ID)) == VALUE


def test_karabo_attributi_to_attributi_map_nonexisting_attributo() -> None:
    """
    It might be that we get an attributi map with attributi in them that are not in the DB; those should be filtered out
    """
    VALUE: Final = 1
    amap, _images = karabo_attributi_to_attributi_map(
        "online",
        RawAttributiMap({}),
        {
            "ignore": {
                "ignore": KaraboAttributo(
                    identifier=ATTRIBUTO_ID,
                    source="source",
                    key="key",
                    description="description",
                    type_="int",
                    karabo_type=None,
                    store=True,
                    action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
                    processor=None,
                    unit="",
                    filling_value=None,
                    value=VALUE,
                    role=None,
                )
            }
        },
        # Note: empty!
        existing_db_attributi={},
    )

    assert amap.select_int(AttributoId(ATTRIBUTO_ID)) is None


def test_karabo_attributi_to_attributi_map_simple_int_no_store() -> None:
    """Test if the "store" attribute works"""
    VALUE: Final = 1
    amap, _images = karabo_attributi_to_attributi_map(
        "online",
        RawAttributiMap({}),
        {
            "ignore": {
                "ignore": KaraboAttributo(
                    identifier=ATTRIBUTO_ID,
                    source="source",
                    key="key",
                    description="description",
                    type_="int",
                    karabo_type=None,
                    store=False,
                    action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
                    processor=None,
                    unit="",
                    filling_value=None,
                    value=VALUE,
                    role=None,
                )
            }
        },
        existing_db_attributi={AttributoId(ATTRIBUTO_ID)},
    )

    assert amap.select_int(AttributoId(ATTRIBUTO_ID)) is None


def test_karabo_attributi_to_attributi_map_datetime() -> None:
    """
    Test if the conversion of a single Karabo int attributo to a DB attributo works.
    """
    VALUE: Final = datetime.datetime.utcnow()
    amap, _images = karabo_attributi_to_attributi_map(
        "online",
        RawAttributiMap({}),
        {
            "ignore": {
                "ignore": KaraboAttributo(
                    ATTRIBUTO_ID,
                    "source",
                    "key",
                    "description",
                    "datetime",
                    store=True,
                    action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
                    processor=None,
                    unit="",
                    filling_value=None,
                    karabo_type=None,
                    value=VALUE,
                    role=None,
                )
            }
        },
        existing_db_attributi={AttributoId(ATTRIBUTO_ID)},
    )

    assert amap.select_value(AttributoId(ATTRIBUTO_ID)) == VALUE.isoformat()


def test_karabo_attributi_to_attributi_map_image() -> None:
    """
    Test if converting an image works (they are treated specially)
    """
    amap, images = karabo_attributi_to_attributi_map(
        "online",
        RawAttributiMap({}),
        {
            "ignore": {
                "ignore": KaraboAttributo(
                    ATTRIBUTO_ID,
                    "source",
                    "key",
                    "description",
                    "image",
                    store=True,
                    action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
                    unit="",
                    processor=None,
                    filling_value=None,
                    value=np.empty([2, 2]),
                    karabo_type=None,
                    role=None,
                )
            }
        },
        existing_db_attributi={AttributoId(ATTRIBUTO_ID)},
    )

    assert not amap.items()
    assert len(images) == 1
    assert images[0].data.shape == (2, 2)
