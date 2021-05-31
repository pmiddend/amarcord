from pathlib import Path

import pytest

from amarcord.amici.p11.spreadsheet_reader import COLUMN_COMMENT
from amarcord.amici.p11.spreadsheet_reader import COLUMN_DIRECTORY
from amarcord.amici.p11.spreadsheet_reader import COLUMN_NAME
from amarcord.amici.p11.spreadsheet_reader import COLUMN_OUTCOME
from amarcord.amici.p11.spreadsheet_reader import COLUMN_RUN_ID
from amarcord.amici.p11.spreadsheet_reader import convert_row
from amarcord.amici.p11.spreadsheet_reader import read_crystal_spreadsheet


def _read_test_file(p: Path) -> Path:
    return Path(__file__).parent / p


def test_convert_row_successful() -> None:
    convert_row(
        Path(),
        {
            COLUMN_NAME: 0,
            COLUMN_DIRECTORY: 1,
            COLUMN_OUTCOME: 2,
            COLUMN_COMMENT: 3,
            COLUMN_RUN_ID: 4,
        },
        ["a", "b", "success", "d", "1"],
        1,
    )


def test_convert_row_empty_name() -> None:
    with pytest.raises(Exception):
        convert_row(
            Path(),
            {
                COLUMN_NAME: 0,
                COLUMN_DIRECTORY: 1,
                COLUMN_OUTCOME: 2,
                COLUMN_COMMENT: 3,
                COLUMN_RUN_ID: 4,
            },
            ["", "b", "success", "d", "1"],
            1,
        )


def test_convert_row_empty_directory() -> None:
    with pytest.raises(Exception):
        convert_row(
            Path(),
            {
                COLUMN_NAME: 0,
                COLUMN_DIRECTORY: 1,
                COLUMN_OUTCOME: 2,
                COLUMN_COMMENT: 3,
                COLUMN_RUN_ID: 4,
            },
            ["a", "", "success", "d", "1"],
            1,
        )


def test_convert_row_invalid_outcome() -> None:
    with pytest.raises(Exception):
        convert_row(
            Path(),
            {
                COLUMN_NAME: 0,
                COLUMN_DIRECTORY: 1,
                COLUMN_OUTCOME: 2,
                COLUMN_COMMENT: 3,
                COLUMN_RUN_ID: 4,
            },
            ["a", "b", "successk", "d", "1"],
            1,
        )


def test_convert_row_invalid_run_id() -> None:
    with pytest.raises(Exception):
        convert_row(
            Path(),
            {
                COLUMN_NAME: 0,
                COLUMN_DIRECTORY: 1,
                COLUMN_OUTCOME: 2,
                COLUMN_COMMENT: 3,
                COLUMN_RUN_ID: 4,
            },
            ["a", "b", "successk", "d", "a"],
            1,
        )


def test_convert_whole_file() -> None:
    read_crystal_spreadsheet(_read_test_file(Path("spreadsheet-test.csv")))
