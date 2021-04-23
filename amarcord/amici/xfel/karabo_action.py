from dataclasses import dataclass
from typing import Union

from amarcord.amici.xfel.karabo_attributi import KaraboAttributi


@dataclass(frozen=True)
class KaraboAttributiUpdate:
    run_id: int
    proposal_id: int
    attributi: KaraboAttributi


@dataclass(frozen=True)
class KaraboRunEnd:
    run_id: int
    proposal_id: int
    attributi: KaraboAttributi


@dataclass(frozen=True)
class KaraboRunStart:
    run_id: int
    proposal_id: int
    attributi: KaraboAttributi


KaraboAction = Union[KaraboAttributiUpdate, KaraboRunEnd, KaraboRunStart]
