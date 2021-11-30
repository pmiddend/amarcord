from pathlib import Path

from hypothesis import given
from hypothesis.strategies import characters
from hypothesis.strategies import text

from amarcord.util import deglob_path


def test_deglob_path() -> None:
    assert deglob_path(Path("foo/bar/*.mp3")) == Path("foo/bar/")
    assert deglob_path(Path("foo/bar/*mp3")) == Path("foo/bar/")


@given(text(alphabet=characters(blacklist_characters="*")))
def test_deglob_path_nothing(f: str) -> None:
    assert deglob_path(Path(f)) == Path(f)


@given(text())
def test_idempotent(f: str) -> None:
    assert deglob_path(Path(f)) == deglob_path(deglob_path(Path(f)))
