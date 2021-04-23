from dataclasses import dataclass

import numpy as np

from amarcord.amici.xfel.karabo_attributo import KaraboAttributo


@dataclass(frozen=True)
class KaraboImage:
    data: np.ndarray
    attributo: KaraboAttributo
