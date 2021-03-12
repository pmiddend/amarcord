from dataclasses import dataclass

from amarcord.db.attributo_value import AttributoValue
from amarcord.db.raw_attributi_map import Source


@dataclass(frozen=True)
class AttributoValueWithSource:
    value: AttributoValue
    source: Source
