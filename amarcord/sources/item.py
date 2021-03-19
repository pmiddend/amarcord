from dataclasses import dataclass
from typing import Union


@dataclass(frozen=True)
class Item:
    name: str
    value: Union[str, float, int]
