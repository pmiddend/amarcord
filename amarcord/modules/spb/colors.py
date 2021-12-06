from typing import Final

from PyQt5 import QtGui

# see https://forum.qt.io/topic/103009/table-view-custom-alternate-row-colors/3

COLOR_MANUAL_ATTRIBUTO: Final = QtGui.QColor("#ecd5e9")

PREDEFINED_COLORS_TO_HEX: Final = {
    "orange": "#f18f1f",
    "aquamarine": "#4CE0B3",
    "bittersweet": "#ED6A5E",
    "desy": "#009FDF",
    "crayola": "#9772c4",
}

HEX_TO_PREDEFINED_COLORS: Final = {v: k for k, v in PREDEFINED_COLORS_TO_HEX.items()}


def name_to_hex(s: str) -> str:
    return PREDEFINED_COLORS_TO_HEX.get(s, "orange")


def hex_to_name(hex_: str) -> str:
    return HEX_TO_PREDEFINED_COLORS.get(hex_, "#f18f1f")
