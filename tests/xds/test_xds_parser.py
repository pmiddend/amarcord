from pathlib import Path

from pint import UnitRegistry

from amarcord.amici.xds.parser import parse_correctlp
from amarcord.amici.xds.parser import parse_resultsfile


def test_parse_correct_lp() -> None:
    info_file = parse_correctlp(Path(__file__).parent / "CORRECT.LP", UnitRegistry())

    assert info_file.a.magnitude == 113.93
    assert info_file.b.magnitude == 53.73
    assert info_file.c.magnitude == 44.67
    assert info_file.alpha.magnitude == 90
    assert info_file.beta.magnitude == 101.837
    assert info_file.gamma.magnitude == 90
    assert info_file.space_group == 5
    assert info_file.resolution_isigma.magnitude == 1.88
    assert info_file.resolution_cc.magnitude == 1.58
    assert info_file.isigi == 6.27
    assert info_file.rfactor == 6.7
    assert info_file.rmeas == 7.8
    assert info_file.cchalf == 99.9


def test_parse_results() -> None:
    info_file = parse_resultsfile(Path(__file__).parent / "xds-results.txt")

    assert info_file.wilson_b == 38.53
