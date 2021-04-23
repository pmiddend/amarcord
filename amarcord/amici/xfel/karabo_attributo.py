from dataclasses import dataclass
from typing import Any
from typing import Optional

from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_special_role import KaraboSpecialRole


@dataclass
class KaraboAttributo:
    identifier: str
    source: str
    key: str
    description: str
    type_: str
    store: bool
    action: KaraboAttributoAction
    role: Optional[KaraboSpecialRole]
    action_axis: Optional[int]
    unit: str
    filling_value: Any
    value: Any
