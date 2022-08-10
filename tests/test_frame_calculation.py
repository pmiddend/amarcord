from pathlib import Path

import h5py
import pytest
import structlog

from amarcord.amici.p11.frame_calculation import retrieve_frames_for_data_file
from amarcord.amici.p11.frame_calculation import retrieve_frames_for_raw_file_glob

logger = structlog.stdlib.get_logger(__name__)


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
