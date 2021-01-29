from typing import Dict
from typing import Any
from dataclasses import dataclass
from amarcord.modules.uicontext import UIContext


@dataclass(frozen=True)
class Context:
    config: Dict[str, Any]
    ui: UIContext
