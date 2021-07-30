import pathlib
from typing import Any
from typing import Optional

from sqlalchemy import types
from sqlalchemy.engine import Dialect


# pylint: disable=abstract-method
class VarcharPath(types.TypeDecorator):
    impl = types.String

    cache_ok = True

    def process_bind_param(
        self, value: Optional[pathlib.Path], dialect: Dialect
    ) -> Any:
        return str(value) if value is not None else None

    def process_result_value(
        self, value: Any, dialect: Dialect
    ) -> Optional[pathlib.Path]:
        return pathlib.Path(value) if value is not None else None

    def copy(self, **kw: Any) -> "VarcharPath":
        return VarcharPath(self.impl.length)


# pylint: disable=abstract-method
class Path(types.TypeDecorator):
    impl = types.Text

    cache_ok = True

    def process_bind_param(
        self, value: Optional[pathlib.Path], dialect: Dialect
    ) -> Any:
        return str(value) if value is not None else None

    def process_result_value(
        self, value: Any, dialect: Dialect
    ) -> Optional[pathlib.Path]:
        return pathlib.Path(value) if value is not None else None

    def copy(self, **kw: Any) -> "Path":
        return Path()
