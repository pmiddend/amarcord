import datetime
from pathlib import Path

from amarcord.util import (
    remove_duplicates_stable,
    dict_union,
    last_line_of_file,
    read_file_to_string,
    sha256_file,
    sha256_file_bytes,
    sha256_files,
    local_time_to_utc,
    group_by,
    replace_illegal_path_characters,
)


def test_remove_duplicates_stable():
    assert remove_duplicates_stable([4, 3, 4, 5, 6, 6]) == [4, 3, 5, 6]


def test_dict_union():
    assert dict_union([{"a": 1}, {"b": 2}, {"a": 3}]) == {"a": 3, "b": 2}


def test_last_line_of_file():
    assert last_line_of_file(Path(__file__).parent / "test-file.txt") == "last"


def test_read_file_to_string():
    assert (
        read_file_to_string(Path(__file__).parent / "test-file.txt")
        == "first\nsecond\nlast"
    )


def test_sha256_file() -> None:
    # Hash retrieved via sha256sum
    assert (
        sha256_file(Path(__file__).parent / "test-file-no-newlines.txt")
        == "c65cf1cfc0c34c720d70dbb0cb3a8432cf22a66fd1d9f998269004caad683ffe"
    )

    assert (
        sha256_file_bytes(Path(__file__).parent / "test-file-no-newlines.txt")
        == b'\xc6\\\xf1\xcf\xc0\xc3Lr\rp\xdb\xb0\xcb:\x842\xcf"\xa6o\xd1\xd9\xf9\x98&\x90\x04\xca\xadh?\xfe'
    )

    assert (
        sha256_files(
            [
                Path(__file__).parent / "test-file-no-newlines.txt",
                Path(__file__).parent / "test-file-no-newlines.txt",
            ]
        )
        == "32ac0b1fc6f1b04aa5d8e1486afc3f3777c10784471f60143ef7616848e15db1"
    )


def test_local_time_to_utc():
    # We just check if the hour has changed, not by how much.
    hour_before = 5
    assert (
        local_time_to_utc(
            datetime.datetime(2022, 2, 1, hour_before, 0, 0, 0),
            current_time_zone="Europe/Berlin",
        ).hour
        != hour_before
    )


def test_group_by() -> None:
    result = group_by([(1, "foo"), (2, "bar"), (1, "baz")], lambda x: x[0])
    assert result == {1: [(1, "foo"), (1, "baz")], 2: [(2, "bar")]}


def test_remove_illegal_path_characters() -> None:
    assert replace_illegal_path_characters("foo\\bar|baz") == "foo_bar_baz"
