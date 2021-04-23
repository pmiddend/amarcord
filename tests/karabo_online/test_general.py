import datetime
from typing import Final

import numpy as np

from amarcord.amici.xfel.karabo_attributo import KaraboAttributo
from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_general import karabo_attributi_to_attributi_map
from amarcord.amici.xfel.karabo_special_role import KaraboSpecialRole
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
                    ATTRIBUTO_ID,
                    "source",
                    "key",
                    "description",
                    "int",
                    store=True,
                    action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
                    action_axis=None,
                    unit="",
                    filling_value=None,
                    value=VALUE,
                    role=None,
                )
            }
        },
    )

    assert amap.select_int(AttributoId(ATTRIBUTO_ID)) == VALUE


def test_karabo_attributi_to_attributi_map_special_role_is_not_in_map() -> None:
    """
    A special role is not supposed to be stored in the attributi map, but separately (or not stored at all, depending)
    """
    VALUE: Final = 1
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
                    "int",
                    store=True,
                    action=KaraboAttributoAction.COMPUTE_ARITHMETIC_MEAN,
                    action_axis=None,
                    unit="",
                    filling_value=None,
                    value=VALUE,
                    role=KaraboSpecialRole.RUN_ID,
                )
            }
        },
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
                    action_axis=None,
                    unit="",
                    filling_value=None,
                    value=VALUE,
                    role=None,
                )
            }
        },
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
                    action_axis=None,
                    unit="",
                    filling_value=None,
                    value=np.empty([2, 2]),
                    role=None,
                )
            }
        },
    )

    assert not amap.items()
    assert len(images) == 1
    assert images[0].data.shape == (2, 2)
