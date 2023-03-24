from pathlib import Path
from typing import Any
from typing import TypeVar

from amarcord.json_types import JSONArray
from amarcord.json_types import JSONDict

T = TypeVar("T")


class JSONChecker:
    """
    In the web server, but also other places, we have the need to extract values of different types from a JSON
    dictionary. To do not repeat ourselves, this little class provides some nice error messages and type-safe getter
    functions.
    """

    def __init__(self, d: JSONDict, description: str) -> None:
        self.d = d
        self.description = description

    def optional_str(self, key: str) -> str | None:
        result = self.d.get(key, None)
        if result is None:
            return None
        if not isinstance(result, str):
            raise Exception(
                f'{self.description} result: value "{key}" not a string but {result}'
            )
        return result

    def optional_dict(self, key: str) -> JSONDict | None:
        result = self.d.get(key)
        if result is None:
            return None
        if not isinstance(result, dict):
            raise Exception(
                f'{self.description} result: value "{key}" not a dict but {result}'
            )
        return result

    def optional_path(self, key: str) -> Path | None:
        result = self.optional_str(key)
        return Path(result) if result is not None else None

    def optional_float(self, key: str) -> float | None:
        result = self.d.get(key, None)
        if result is None:
            return None
        if not isinstance(result, (float, int)):
            raise Exception(
                f'{self.description} result: value "{key}" not a float but {result}'
            )
        return float(result)

    def optional_int(self, key: str) -> int | None:
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

    def retrieve_safe_list(self, key: str) -> JSONArray:
        v = self.retrieve_safe(key)
        if not isinstance(v, list):
            raise Exception(f'{self.description} result: value "{key}" not a list: {v}')
        return v

    def retrieve_safe_str(self, key: str) -> str:
        v = self.retrieve_safe(key)
        if not isinstance(v, str):
            raise Exception(
                f'{self.description} result: value "{key}" not a string: {v}'
            )
        return v

    def retrieve_safe_boolean(self, key: str) -> bool:
        v = self.retrieve_safe(key)
        if not isinstance(v, bool):
            raise Exception(f'{self.description} result: value "{key}" not a bool: {v}')
        return v

    def retrieve_safe_dict(self, key: str) -> JSONDict:
        v = self.retrieve_safe(key)
        if not isinstance(v, dict):
            raise Exception(
                f'{self.description} result: value "{key}" not a dictionary: {v}'
            )
        return v

    def retrieve_safe_array(self, key: str) -> list[T]:
        json_array = self.d.get(key, None)
        if json_array is None:
            raise Exception(f"{self.description}: {key} not found")
        if not isinstance(json_array, list):
            raise Exception(f"{self.description}: {key} not an array but: {json_array}")
        return json_array

    def retrieve_safe_int_array(self, key: str) -> list[int]:
        json_array: list[Any] = self.retrieve_safe_array(key)
        result: list[int] = []
        for i, number in enumerate(json_array):
            if not isinstance(number, int):
                raise Exception(
                    f"{self.description}: {key}[{i}] contains non-integer: {number}"
                )
            result.append(number)
        return result

    def retrieve_safe_string_array(self, key: str) -> list[str]:
        json_array: list[Any] = self.retrieve_safe_array(key)
        result: list[str] = []
        for i, s in enumerate(json_array):
            if not isinstance(s, str):
                raise Exception(
                    f"{self.description}: {key}[{i}] contains non-string: {s}"
                )
            result.append(s)
        return result
