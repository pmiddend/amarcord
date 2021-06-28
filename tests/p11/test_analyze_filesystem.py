from pathlib import Path

import pytest

from amarcord.amici.p11.analyze_filesystem import parse_p11_crystals


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


def test_parse_p11_crystals_invalid_proposal_path(fs) -> None:
    with pytest.raises(Exception):
        parse_p11_crystals(Path("/proposal"))

    with pytest.raises(Exception):
        fs.add_real_file(
            Path(__file__).parent / "info.txt",
            target_path=f"/proposal",
        )
        parse_p11_crystals(Path("/proposal"))


def test_parse_p11_crystals_skip_files_in_dir(fs) -> None:
    base_path = "/proposal/raw/"
    fs.add_real_file(
        Path(__file__).parent / "info.txt",
        target_path=f"{base_path}/info.txt",
    )

    crystals, has_warnings = parse_p11_crystals(Path("/proposal"))

    assert not has_warnings
    assert len(crystals) == 0


def test_parse_p11_crystal_ignore_run_dirs_that_are_files(fs) -> None:
    base_path = "/proposal/raw/crystal_id"
    fs.add_real_file(
        Path(__file__).parent / "info.txt",
        target_path=f"{base_path}/crystal_id_001/info.txt",
    )
    fs.add_real_file(
        Path(__file__).parent / "info.txt",
        target_path=f"{base_path}/crystal_id_002",
    )

    crystals, has_warnings = parse_p11_crystals(Path("/proposal"))

    assert not has_warnings


def test_parse_p11_crystal_invalid_run_dir_name_produces_warning(fs) -> None:
    base_path = "/proposal/raw/crystal_id/crystal_id_invalid_001"
    fs.add_real_file(
        Path(__file__).parent / "info.txt",
        target_path=f"{base_path}/info.txt",
    )

    _crystals, has_warnings = parse_p11_crystals(Path("/proposal"))

    assert has_warnings


def test_parse_p11_crystal_invalid_run_id_produces_warning(fs) -> None:
    base_path = "/proposal/raw/crystal_id/crystal_id_a"
    fs.add_real_file(
        Path(__file__).parent / "info.txt",
        target_path=f"{base_path}/info.txt",
    )

    _crystals, has_warnings = parse_p11_crystals(Path("/proposal"))

    assert has_warnings


def test_parse_p11_crystal_without_runs(fs) -> None:
    base_path = "/proposal/raw/crystal_id"
    fs.create_dir(base_path)

    _crystals, has_warnings = parse_p11_crystals(Path("/proposal"))

    assert has_warnings


def test_parse_p11_crystal_without_info_txt(fs) -> None:
    base_path = "/proposal/raw/crystal_id/crystal_id_001"
    fs.create_dir(base_path)

    _crystals, has_warnings = parse_p11_crystals(Path("/proposal"))

    assert has_warnings
