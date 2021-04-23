from typing import Dict

from amarcord.amici.xfel.karabo_attributo import KaraboAttributo
from amarcord.amici.xfel.karabo_group import KaraboGroup

KaraboAttributi = Dict[KaraboGroup, Dict[str, KaraboAttributo]]
