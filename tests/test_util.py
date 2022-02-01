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


def test_sha256_file():
    # Hash retrieved via sha256sum
    assert (
        sha256_file(Path(__file__).parent / "test-file.txt")
        == "f569ec22eff0ed5db9f940f6ccb602572f40002763b4623f3135feb9b01a7d96"
    )

    assert (
        sha256_file_bytes(Path(__file__).parent / "test-file.txt")
        == b"\xf5i\xec\"\xef\xf0\xed]\xb9\xf9@\xf6\xcc\xb6\x02W/@\x00'c\xb4b?15\xfe\xb9\xb0\x1a}\x96"
    )

    assert (
        sha256_files(
            [
                Path(__file__).parent / "test-file.txt",
                Path(__file__).parent / "test-file.txt",
            ]
        )
        == "d2cfb2d28bff9f3754fb4300df4de177b806c1cccae989340dcad55246b4917b"
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
