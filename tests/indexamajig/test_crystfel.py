import gzip
from pathlib import Path
from shutil import copyfile
from shutil import copyfileobj

from amarcord.amici.crystfel.injest import harvest_folder
from amarcord.amici.crystfel.parser import read_crystfel_streams
from amarcord.amici.crystfel.parser import read_harvest_json
from amarcord.db.table_classes import DBDataSource
from amarcord.db.table_classes import DBHitFindingResult
from amarcord.db.table_classes import DBLinkedDataSource
from amarcord.db.table_classes import DBLinkedHitFindingResult
from amarcord.db.table_classes import DBPeakSearchParameters


def _extract_file(tmp_path: Path, source: str) -> Path:
    with (Path(__file__).parent / source).open("rb") as read_file:
        with gzip.open(read_file) as read_file_gzip:
            with (tmp_path / source[0:-3]).open("wb") as write_file:
                copyfileobj(read_file_gzip, write_file)
                return tmp_path / source[0:-3]


def test_parse_nonexisting_harvest_json() -> None:
    params = read_harvest_json(Path(__file__).parent / "parameters.json2")

    assert params is None


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


def test_harvest_folder_without_cxi(tmp_path: Path) -> None:
    copyfile(Path(__file__).parent / "parameters.json", tmp_path / "parameters.json")

    _extract_file(tmp_path, "test001.stream.gz")

    resulting_ds = harvest_folder(
        [], tmp_path, 1, "*stream*", tmp_path / "parameters.json", tag=None
    )

    assert len(resulting_ds) == 1

    ds = resulting_ds[0]
    assert ds.data_source.run_id == 1
    assert ds.data_source.number_of_frames > 0
    assert len(ds.hit_finding_results) == 1
    hfr = ds.hit_finding_results[0]
    assert len(hfr.indexing_results) == 1


def test_harvest_folder_without_indexing(tmp_path: Path) -> None:
    copyfile(
        Path(__file__).parent / "parameters-no-indexing.json",
        tmp_path / "parameters.json",
    )

    _extract_file(tmp_path, "test001.stream.gz")

    resulting_ds = harvest_folder(
        [], tmp_path, 1, "*stream*", tmp_path / "parameters.json", tag=None
    )

    assert not resulting_ds


def test_harvest_folder_cxi(tmp_path: Path) -> None:
    copyfile(
        Path(__file__).parent / "parameters-from-cxi.json", tmp_path / "parameters.json"
    )

    _extract_file(tmp_path, "test001.stream.gz")

    resulting_ds = harvest_folder(
        [], tmp_path, 1, "*stream*", tmp_path / "parameters.json", tag=None
    )

    assert len(resulting_ds) == 0

    resulting_ds = harvest_folder(
        [
            DBLinkedDataSource(
                DBDataSource(
                    id=1,
                    run_id=1,
                    number_of_frames=1,
                    source={},
                    tag=None,
                    comment=None,
                ),
                hit_finding_results=[
                    DBLinkedHitFindingResult(
                        hit_finding_result=DBHitFindingResult(
                            id=1,
                            peak_search_parameters_id=1,
                            hit_finding_parameters_id=1,
                            data_source_id=1,
                            result_filename="/run/media/taw/Data SSD 2020/2021/CPV/cxis0613-r0003.cxi",
                            peaks_filename=None,
                            result_type="stream",
                            average_peaks_event=1,
                            average_resolution=2,
                            number_of_hits=1,
                            hit_rate=1,
                            tag=None,
                            comment=None,
                        ),
                        peak_search_parameters=DBPeakSearchParameters(
                            id=None, method="dummy", software="dummy"
                        ),
                        # Not needed for this test, we just want to check if the cxi file is really compared and the
                        # hit finding result found
                        hit_finding_parameters=None,  # type: ignore
                        indexing_results=[],
                    )
                ],
            )
        ],
        tmp_path,
        1,
        "*stream*",
        tmp_path / "parameters.json",
        tag=None,
    )

    assert resulting_ds
    assert resulting_ds[0].hit_finding_results
    assert resulting_ds[0].hit_finding_results[0].indexing_results
