from typing import Dict

from amarcord.amici.xfel.karabo_attributo_and_group import AttributoAndGroup
from amarcord.amici.xfel.karabo_key import KaraboKey
from amarcord.amici.xfel.karabo_source import KaraboSource

KaraboExpectedAttributi = Dict[KaraboSource, Dict[KaraboKey, AttributoAndGroup]]
