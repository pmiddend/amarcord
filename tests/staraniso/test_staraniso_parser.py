from pathlib import Path

from amarcord.amici.staraniso.parser import parse_staraniso_directory


def test_parse_sample_file() -> None:
    analysis_result = parse_staraniso_directory(Path(__file__).parent)
    assert analysis_result.space_group == "P 61 2 2"
    assert analysis_result.mtz_file is not None
    assert analysis_result.resolution_cc == 0.689
    assert analysis_result.rfactor == 0.122
