from typing import TypedDict

from amarcord.amici.xfel.karabo_attributo import KaraboAttributo


class AttributoAndGroup(TypedDict):
    attributo: KaraboAttributo
    group: str
