from dataclasses import dataclass
from typing import Any
from typing import Optional

from amarcord.amici.xfel.karabo_attributo_action import KaraboAttributoAction
from amarcord.amici.xfel.karabo_processor import KaraboProcessor
from amarcord.amici.xfel.karabo_special_role import KaraboSpecialRole


@dataclass
class KaraboAttributo:
    identifier: str
    source: str
    key: str
    description: str
    type_: str
    karabo_type: Optional[str]
    store: bool
    action: KaraboAttributoAction
    processor: Optional[KaraboProcessor]
    role: Optional[KaraboSpecialRole]
    unit: str
    filling_value: Any
    value: Any
