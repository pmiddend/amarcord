from pathlib import Path

from amarcord.cli.crystfel_index import crystfel_geometry_hash
from amarcord.cli.crystfel_index import crystfel_geometry_retrieve_masks


def test_retrieve_masks(tmp_path: Path) -> None:
    main_geometry_file = tmp_path / "test.geom"
    mask1_file = tmp_path / "mask1.h5"
    with main_geometry_file.open("w", encoding="utf-8") as f:
        f.write(
            f"""
clen = 0.1986

photon_energy = 12000 ;12050
mask1_file = {mask1_file}
        """,
        )

    masks = crystfel_geometry_retrieve_masks(main_geometry_file)

    assert masks == [mask1_file]


def test_geometry_with_mask_that_changes_should_also_change_hash(
    tmp_path: Path,
) -> None:
    main_geometry_file = tmp_path / "test.geom"
    mask1_file = tmp_path / "mask1.h5"
    with main_geometry_file.open("w", encoding="utf-8") as f:
        f.write(
            f"""
clen = 0.1986

photon_energy = 12000 ;12050
mask1_file = {mask1_file}
        """,
        )

    with mask1_file.open("w", encoding="utf-8") as f:
        f.write("test1234")

    hash_with_first_mask = crystfel_geometry_hash(main_geometry_file)

    with mask1_file.open("w", encoding="utf-8") as f:
        f.write("12345")

    hash_with_second_mask = crystfel_geometry_hash(main_geometry_file)

    assert hash_with_first_mask != hash_with_second_mask
