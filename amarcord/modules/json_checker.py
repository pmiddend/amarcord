from pathlib import Path
from typing import Any
from typing import Optional

from amarcord.modules.json import JSONDict


class JSONChecker:
    def __init__(self, d: JSONDict, description: str) -> None:
        self.d = d
        self.description = description

    def optional_str(self, key: str) -> Optional[str]:
        result = self.d.get(key, None)
        if result is None:
            return None
        if not isinstance(result, str):
            raise Exception(
                f'{self.description} result: value "{key}" not a string but {result}'
            )
        return result

    def optional_path(self, key: str) -> Optional[Path]:
        result = self.optional_str(key)
        return Path(result) if result is not None else None

    def optional_float(self, key: str) -> Optional[float]:
        result = self.d.get(key, None)
        if result is None:
            return None
        if not isinstance(result, (float, int)):
            raise Exception(
                f'{self.description} result: value "{key}" not a float but {result}'
            )
        return float(result)

    def optional_int(self, key: str) -> Optional[int]:
        result = self.d.get(key, None)
        if result is None:
            return None
        if not isinstance(result, int):
            raise Exception(
                f'{self.description} result: value "{key}" not an int but {result}'
            )
        return result

    def safe_path(self, key: str) -> Path:
        result = self.retrieve_safe(key)
        if not isinstance(result, str):
            raise Exception(
                f'{self.description} result: value for "{key}" is not string (path) but {result}'
            )
        return Path(result)

    def retrieve_safe(self, key: str) -> Any:
        result = self.d.get(key, None)
        if result is None:
            raise Exception(
                f'{self.description} result: couldn\'t get value "{key}", dict is: {self.d}'
            )
        return result

    def retrieve_safe_float(self, key: str) -> float:
        v = self.retrieve_safe(key)
        if not isinstance(v, (float, int)):
            raise Exception(
                f'{self.description} result: value "{key}" not a number: {v}'
            )
        return float(v)

    def retrieve_safe_int(self, key: str) -> int:
        v = self.retrieve_safe(key)
        if not isinstance(v, int):
            raise Exception(f'{self.description} result: value "{key}" not an int: {v}')
        return v

    def retrieve_safe_dict(self, key: str) -> JSONDict:
        v = self.retrieve_safe(key)
        if not isinstance(v, dict):
            raise Exception(f'{self.description} result: value "{key}" not a dict: {v}')
        return v

    def retrieve_safe_str(self, key: str) -> str:
        v = self.retrieve_safe(key)
        if not isinstance(v, str):
            raise Exception(
                f'{self.description} result: value "{key}" not a string: {v}'
            )
        return v
