from dataclasses import dataclass
from typing import Any
from typing import Dict

from amarcord.modules.dbcontext import DBContext
from amarcord.modules.uicontext import UIContext


@dataclass(frozen=True)
class Context:
    config: Dict[str, Any]
    ui: UIContext
    db: DBContext
