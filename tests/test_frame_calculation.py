from pathlib import Path

import h5py
import pytest

from amarcord.amici.om.client import (
    ATTRIBUTO_NUMBER_OF_FRAMES,
    ATTRIBUTO_NUMBER_OF_OM_HITS,
    ATTRIBUTO_NUMBER_OF_HITS,
    ATTRIBUTO_NUMBER_OF_OM_FRAMES,
)
from amarcord.amici.p11.frame_calculation import (
    update_frames_in_run,
    retrieve_frames_for_data_file,
    retrieve_frames_for_raw_file_glob,
)
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.attributi_map import AttributiMap
from amarcord.db.attributo_type import AttributoTypeInt
from amarcord.db.dbattributo import DBAttributo


@pytest.mark.parametrize(
    "input_frames, input_om_frames, input_om_hits, output_hits",
    [
        (100, None, None, None),
        (100, 10, None, None),
        (100, None, 10, None),
        (100, 10, 1, 10),
        (100, 0, 1, None),
    ],
)
def test_update_frames_in_run(
    input_frames: int,
    input_om_frames: int | None,
    input_om_hits: int | None,
    output_hits: int | None,
) -> None:
    attributi = AttributiMap.from_types_and_json(
        [
            DBAttributo(
                x,
                "",
                "",
                AssociatedTable.RUN,
                AttributoTypeInt(),
            )
            for x in (
                ATTRIBUTO_NUMBER_OF_HITS,
                ATTRIBUTO_NUMBER_OF_FRAMES,
                ATTRIBUTO_NUMBER_OF_OM_HITS,
                ATTRIBUTO_NUMBER_OF_OM_FRAMES,
            )
        ],
        sample_ids=[],
        raw_attributi={
            ATTRIBUTO_NUMBER_OF_OM_HITS: input_om_hits,
            ATTRIBUTO_NUMBER_OF_OM_FRAMES: input_om_frames,
        },
    )
    update_frames_in_run(attributi, input_frames)
    assert attributi.select_int(ATTRIBUTO_NUMBER_OF_HITS) == output_hits


@pytest.mark.parametrize("input_frame_count", [1, 1000, 1337])
def test_retrieve_frames_for_data_file(tmp_path: Path, input_frame_count: int) -> None:
    h5_filename = tmp_path / "test.h5"

    with h5py.File(h5_filename, "w") as f:
        entry = f.create_group("entry")
        data = entry.create_group("data")
        data.create_dataset("data", shape=(input_frame_count, 30, 30), dtype="i")

    assert retrieve_frames_for_data_file(h5_filename) == input_frame_count


def test_retrieve_frames_for_data_file_group_missing(tmp_path: Path) -> None:
    h5_filename = tmp_path / "test.h5"

    with h5py.File(h5_filename, "w") as f:
        entry = f.create_group("entry")
        # deliberately the wrong group
        data = entry.create_group("data2")
        data.create_dataset("data", shape=(1000, 30, 30), dtype="i")

    with pytest.raises(Exception):
        retrieve_frames_for_data_file(h5_filename)


def test_retrieve_frames_for_raw_file_glob(tmp_path: Path) -> None:
    for fn in [tmp_path / "data_01.h5", tmp_path / "data_02.h5"]:
        with h5py.File(fn, "w") as f:
            entry = f.create_group("entry")
            data = entry.create_group("data")
            data.create_dataset("data", shape=(500, 30, 30), dtype="i")

    # Create a dummy file so we test if the glob is really respected
    with (tmp_path / "test").open("w") as f:
        f.write("hehe")

    assert retrieve_frames_for_raw_file_glob(f"{tmp_path}/data*h5") == 1000
