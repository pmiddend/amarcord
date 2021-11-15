import gzip
from pathlib import Path
from shutil import copyfileobj

import pytest

from amarcord.amici.crystfel.parser import read_crystfel_streams
from amarcord.amici.crystfel.parser import read_harvest_json


def _extract_file(tmp_path: Path, source: str) -> Path:
    with (Path(__file__).parent / source).open("rb") as read_file:
        with gzip.open(read_file) as read_file_gzip:
            with (tmp_path / source[0:-3]).open("wb") as write_file:
                copyfileobj(read_file_gzip, write_file)
                return tmp_path / source[0:-3]


def test_parse_nonexisting_harvest_json() -> None:
    with pytest.raises(Exception):
        read_harvest_json(Path(__file__).parent / "parameters.json2")


def test_parse_harvest_json() -> None:
    params = read_harvest_json(Path(__file__).parent / "parameters.json")

    assert params is not None
    assert params.integration is not None


def test_parser(tmp_path: Path) -> None:
    stream = _extract_file(tmp_path, "test001.stream.gz")
    metadata = read_crystfel_streams([stream])

    assert (
        metadata.command_line
        == "/home/taw/crystfel/build/indexamajig -i events001.lst -o test001.stream -g ginn6.geom -j 3 --peaks=cxi "
        "--profile -p CPV.cell --harvest-file=parameters.json --min-peaks=20 --multi"
    )
