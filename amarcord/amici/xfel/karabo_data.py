from typing import Dict

from amarcord.amici.xfel.karabo_key import KaraboKey
from amarcord.amici.xfel.karabo_source import KaraboSource
from amarcord.amici.xfel.karabo_value import KaraboValue

KaraboData = Dict[KaraboSource, Dict[KaraboKey, KaraboValue]]
