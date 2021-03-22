from dataclasses import dataclass

from amarcord.config import UserConfig
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.uicontext import UIContext


@dataclass(frozen=True)
class Context:
    config: UserConfig
    ui: UIContext
    db: DBContext
