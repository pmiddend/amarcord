from amarcord.workflows.command_line import CommandLineInputType
from amarcord.workflows.command_line import parse_command_line


def test_parse_command_line() -> None:
    result = parse_command_line("foo ${bar} ${baz} ${diffraction.path}")
    assert len(result.inputs) == 3
    assert result.inputs[0].name == "bar"
    assert result.inputs[0].type_ == CommandLineInputType.STRING
    assert result.inputs[1].name == "baz"
    assert result.inputs[1].type_ == CommandLineInputType.STRING
    assert result.inputs[2].type_ == CommandLineInputType.DIFFRACTION_PATH
