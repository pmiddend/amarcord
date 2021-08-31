import os
from pathlib import Path
from shutil import copyfile

from amarcord.amici.dimple.parser import parse_dimple_output_directory
from amarcord.amici.dimple.parser import to_amarcord_output
from amarcord.amici.p11.refinement_result import RefinementResult
from amarcord.workflows.parser import parse_workflow_result_file


def test_parse_dimple() -> None:
    output = parse_dimple_output_directory(
        Path(__file__).parent / "data", "mymetadata", check_paths_exist=False
    )
    assert output["rfree"] > 0
    assert output["rwork"] > 0
    assert output["initial-pdb-path"] is not None
    assert output["final-pdb-path"] is not None


def test_parse_and_ingest(tmp_path) -> None:
    data_files = list((Path(__file__).parent / "data").iterdir())

    os.chdir(tmp_path)

    for df in data_files:
        copyfile(df, Path(".") / df.name)

    to_amarcord_output()

    amarcord_output = Path("amarcord-output.json")
    assert amarcord_output.is_file()

    parsed = parse_workflow_result_file(amarcord_output)

    assert isinstance(parsed, list)
    assert len(parsed) == 1
    assert isinstance(parsed[0], RefinementResult)
