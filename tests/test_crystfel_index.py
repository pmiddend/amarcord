import pytest

from amarcord.cli.crystfel_index import parse_millepede_output


def test_parse_millepede_output() -> None:
    millepede_output = """
...

Millepede succeeded.

Group all:
    x-translation -0.002296 mm
    y-translation +0.012169 mm
    z-translation +0.189550 mm
       x-rotation -0.013392 deg
       y-rotation -0.008520 deg
Group panel0:
    x-translation +0.000000 mm
    y-translation +0.000000 mm
    z-translation +0.000000 mm
       x-rotation +0.000000 deg
       y-rotation +0.000000 deg
       z-rotation +0.000000 deg
    """

    output = parse_millepede_output(millepede_output)

    assert isinstance(output, dict)

    assert "all" in output
    assert "panel0" in output

    assert output["all"].x_translation_mm == pytest.approx(-0.002296)
    assert output["all"].y_translation_mm == pytest.approx(0.012169)
    assert output["all"].z_translation_mm == pytest.approx(0.189550)
    assert output["all"].x_rotation_deg == pytest.approx(-0.013392)
    assert output["all"].y_rotation_deg == pytest.approx(-0.008520)

    assert output["panel0"].x_translation_mm == pytest.approx(0)
