from pathlib import Path

from amarcord.amici.xia2.parser import parse_xia2_directory


def test_xia2_parser() -> None:
    result = parse_xia2_directory(Path(__file__).parent)

    # Just a random assortment of comparisons
    assert result.resolution_isigma == 0.3101918802779456
    assert result.rfactor == 0.1148903970335763
