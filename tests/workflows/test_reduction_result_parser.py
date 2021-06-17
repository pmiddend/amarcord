from pathlib import Path

from amarcord.workflows.parser import parse_workflow_result_file


def test_parse_valid_reduction_result_space_group_int() -> None:
    result = parse_workflow_result_file(Path(__file__).parent / "amarcord-output.json")
    assert len(result) == 1
    assert result[0].space_group == 150
    assert result[0].a * result[0].b * result[0].c != 0
    assert result[0].alpha * result[0].beta * result[0].gamma != 0


def test_parse_valid_reduction_result_space_group_string() -> None:
    result = parse_workflow_result_file(
        Path(__file__).parent / "amarcord-output-sg-string.json"
    )
    assert len(result) == 1
    assert result[0].space_group == 16
