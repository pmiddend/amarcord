import fcntl
from typing import Any
from typing import Optional


class Locker:
    def __init__(self) -> None:
        self.fp: Optional[Any] = None

    def __enter__(self):
        # pylint: disable=consider-using-with
        self.fp = open("./lockfile.lck", "wb")
        fcntl.flock(self.fp.fileno(), fcntl.LOCK_EX)

    def __exit__(self, _type: Any, value: Any, tb: Any) -> None:
        if self.fp is not None:
            fcntl.flock(self.fp.fileno(), fcntl.LOCK_UN)
            self.fp.close()
