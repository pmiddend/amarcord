import pathlib
from typing import Any

from sqlalchemy import types
from sqlalchemy.engine import Dialect


# pylint: disable=abstract-method
class Path(types.TypeDecorator[pathlib.Path]):
    impl = types.Text

    cache_ok = True

    def process_bind_param(self, value: pathlib.Path | None, dialect: Dialect) -> Any:
        return str(value) if value is not None else None

    def process_result_value(self, value: Any, dialect: Dialect) -> pathlib.Path | None:
        return pathlib.Path(value) if value is not None else None

    def copy(self, **kw: Any) -> "Path":
        return Path()
