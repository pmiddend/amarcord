import pytest

from amarcord.web.router_merging import validate_ambigator_command_line


def test_validate_ambigator_command_line() -> None:
    # empty is fine
    validate_ambigator_command_line("")

    # valid point group
    validate_ambigator_command_line("-w 222")

    # invalid point group
    with pytest.raises(ValueError):
        validate_ambigator_command_line("-w doof")

    # no point group at all
    with pytest.raises(ValueError):
        validate_ambigator_command_line("-w")

    validate_ambigator_command_line("--really-random")
    with pytest.raises(ValueError):
        validate_ambigator_command_line("--unknown argument")

    validate_ambigator_command_line("--symmetry=4/mmm")

    with pytest.raises(ValueError):
        validate_ambigator_command_line("--symmetry=doof2")

    validate_ambigator_command_line("--operator=h,k,l;h,-k,-l")
    with pytest.raises(ValueError):
        validate_ambigator_command_line("--operator=h,k,l;doof")

    validate_ambigator_command_line("--iterations=5")

    with pytest.raises(ValueError):
        validate_ambigator_command_line("--iterations=5.5")

    validate_ambigator_command_line("--ncorr=5")

    with pytest.raises(ValueError):
        validate_ambigator_command_line("--ncorr=5.5")

    with pytest.raises(ValueError):
        validate_ambigator_command_line("--iterations=b")

    validate_ambigator_command_line("--highres=1.2")
    with pytest.raises(ValueError):
        validate_ambigator_command_line("--highres=a")

    validate_ambigator_command_line("--lowres=1.2")
    with pytest.raises(ValueError):
        validate_ambigator_command_line("--lowres=a")
