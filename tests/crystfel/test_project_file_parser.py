from pathlib import Path

from amarcord.amici.crystfel.project_parser import parse_crystfel_project_file


def test_parse_valid_project_file() -> None:
    result = parse_crystfel_project_file(Path(__file__).parent / "valid.project")

    # Just random tests to see if it's sort of right
    assert result.info_lines["geom"] == "./5HT2B-Liu-2013.geom"
    assert result.info_lines["peak_search_params.method"] == "zaef"

    assert (
        "cxidb-21-run0131/data1/LCLS_2013_Mar23_r0131_003835_c2c7.h5 //"
        in result.file_lines
    )
