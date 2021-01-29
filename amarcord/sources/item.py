from typing import Union
from dataclasses import dataclass

@dataclass(frozen=True)
class Item:
    name: str
    value: Union[str, float, int]
