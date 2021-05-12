from pathlib import Path

import pytest

from amarcord.amici.p11.analyze_filesystem import parse_p11_crystals
from amarcord.amici.p11.analyze_filesystem import parse_p11_targets


def test_parse_p11_targets_fails_without_raw_dir(fs) -> None:
    fs.create_dir("/proposal")

    with pytest.raises(Exception):
        parse_p11_targets(Path("/proposal"))


def test_parse_p11_targets_empty_raw(fs) -> None:
    fs.create_dir("/proposal/raw")

    targets, has_warnings = parse_p11_targets(Path("/proposal"))
    assert not targets
    assert not has_warnings


def test_parse_p11_targets_target_without_pucks(fs) -> None:
    fs.create_dir("/proposal/raw/target1")

    targets, has_warnings = parse_p11_targets(Path("/proposal"))
    assert len(targets) == 1
    # Warning about missing pucks in a target
    assert has_warnings


def test_parse_p11_targets_target_with_puck_without_runs(fs) -> None:
    fs.create_dir("/proposal/raw/target1/puck1_01")

    targets, has_warnings = parse_p11_targets(Path("/proposal"))
    assert len(targets) == 1
    assert len(targets[0].pucks) == 1
    # Warning about no runs in the target
    assert has_warnings


def test_parse_p11_targets_target_with_puck_invalid_directory(fs) -> None:
    fs.create_dir("/proposal/raw/target1/puck1_a")

    targets, has_warnings = parse_p11_targets(Path("/proposal"))
    assert len(targets) == 1
    assert not targets[0].pucks
    # Warning about weird path in target
    assert has_warnings


def test_parse_p11_targets_target_with_puck_with_invalid_run(fs) -> None:
    fs.create_dir("/proposal/raw/target1/puck1_01/invalid_run")

    targets, has_warnings = parse_p11_targets(Path("/proposal"))
    assert len(targets) == 1
    assert len(targets[0].pucks) == 1
    # Warning about weird path in puck
    assert has_warnings


def test_parse_p11_targets_target_with_puck_with_run_different_puck_id(fs) -> None:
    fs.create_dir("/proposal/raw/target1/puck1_13/puck2_13_001")

    targets, has_warnings = parse_p11_targets(Path("/proposal"))
    assert len(targets) == 1
    assert len(targets[0].pucks) == 1
    assert targets[0].pucks[0].puck_id == "puck1"
    assert targets[0].pucks[0].position == 13
    assert not targets[0].pucks[0].runs
    # Warning about weird path in puck
    assert has_warnings


def test_parse_p11_targets_target_with_puck_with_valid_run_but_no_info(fs) -> None:
    fs.create_dir("/proposal/raw/target1/puck1_13/puck1_13_001")

    targets, has_warnings = parse_p11_targets(Path("/proposal"))
    assert len(targets) == 1
    assert len(targets[0].pucks) == 1
    assert targets[0].pucks[0].puck_id == "puck1"
    assert targets[0].pucks[0].position == 13
    assert not targets[0].pucks[0].runs
    # Warning about weird path in puck
    assert has_warnings


def test_parse_p11_targets_target_with_puck_with_valid_run_and_valid_info(fs) -> None:
    fs.add_real_file(
        Path(__file__).parent / "info.txt",
        target_path="/proposal/raw/target1/puck1_13/puck1_13_001/info.txt",
    )

    targets, has_warnings = parse_p11_targets(Path("/proposal"))
    assert len(targets) == 1
    assert len(targets[0].pucks) == 1
    assert targets[0].pucks[0].puck_id == "puck1"
    assert targets[0].pucks[0].position == 13
    assert len(targets[0].pucks[0].runs) == 1
    assert targets[0].pucks[0].runs[0].run_id == 1
    assert targets[0].pucks[0].runs[0].processed_path is None
    assert targets[0].pucks[0].runs[0].microscope_image_filename_pattern is None
    assert targets[0].pucks[0].runs[0].data_raw_filename_pattern is None
    assert targets[0].pucks[0].runs[0].info_file.run_type == "regular"
    assert not has_warnings


def test_parse_p11_targets_target_with_puck_with_valid_run_and_valid_info_and_valid_other_files(
    fs,
) -> None:
    base_path = "/proposal/raw/target1/puck1_13/puck1_13_001"
    fs.add_real_file(
        Path(__file__).parent / "info.txt",
        target_path=f"{base_path}/info.txt",
    )
    # So the microscope filename will be set
    fs.create_file(f"{base_path}/foo.jpg")
    # So the raw filename will be set
    fs.create_file(f"{base_path}/foo.h5")
    # Note the "processed" here
    fs.create_dir("/proposal/processed/target1/puck1_13/puck1_13_001")

    targets, has_warnings = parse_p11_targets(Path("/proposal"))
    assert targets[0].pucks[0].runs[0].microscope_image_filename_pattern is not None
    assert targets[0].pucks[0].runs[0].data_raw_filename_pattern is not None
    assert targets[0].pucks[0].runs[0].processed_path is not None
    assert not has_warnings


def test_parse_p11_crystals(fs) -> None:
    base_path = "/proposal/raw/crystal_id/crystal_id_001"
    fs.add_real_file(
        Path(__file__).parent / "info.txt",
        target_path=f"{base_path}/info.txt",
    )

    crystals, has_warnings = parse_p11_crystals(Path("/proposal"))

    assert not has_warnings
    assert len(crystals) == 1
    assert crystals[0].crystal_id == "crystal_id"
    assert len(crystals[0].runs) == 1
    assert crystals[0].runs[0].run_id == 1
    assert crystals[0].runs[0].info_file.run_type == "regular"
