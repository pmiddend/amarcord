import csv
from dataclasses import dataclass
from pathlib import Path
from typing import Final
from typing import List
from typing import Mapping

from amarcord.amici.p11.db import DiffractionType
from amarcord.amici.p11.db_ingest import MetadataRetriever
from amarcord.util import find_by

COLUMN_COMMENT: Final = "comment"
COLUMN_OUTCOME: Final = "outcome"
COLUMN_NAME: Final = "name"
COLUMN_DIRECTORY: Final = "directory"
COLUMN_ESTIMATED_RESOLUTION: Final = "estimated resolution"
COLUMN_RUN_ID: Final = "run id"


@dataclass(frozen=True)
class CrystalLine:
    line_number: int
    directory: str
    name: str
    run_id: int
    outcome: DiffractionType
    comment: str
    estimated_resolution: str


def _retrieve_line_for_crystal_and_run_id(
    lines: List[CrystalLine], crystal_id: str, run_id: int
) -> CrystalLine:
    result = find_by(lines, lambda x: x.name == crystal_id and x.run_id == run_id)
    if result is None:
        raise Exception(
            f"couldn't find spreadsheet line for crystal ID {crystal_id}, run id {run_id}"
        )
    return result


def metadata_retriever_from_lines(
    lines: List[CrystalLine], detector_name: str
) -> MetadataRetriever:
    return MetadataRetriever(
        lambda cid, rid: _retrieve_line_for_crystal_and_run_id(lines, cid, rid).outcome,
        lambda cid, rid: _retrieve_line_for_crystal_and_run_id(lines, cid, rid).comment,
        lambda cid, rid: _retrieve_line_for_crystal_and_run_id(
            lines, cid, rid
        ).estimated_resolution,
        detector_name,
    )


def read_crystal_spreadsheet(path: Path) -> List[CrystalLine]:
    with path.open("r", newline="") as csv_file:
        reader = csv.reader(csv_file, delimiter="|")
        header = next(reader)
        expected_columns = {
            COLUMN_DIRECTORY,
            COLUMN_NAME,
            COLUMN_ESTIMATED_RESOLUTION,
            COLUMN_OUTCOME,
            COLUMN_COMMENT,
            COLUMN_RUN_ID,
        }
        if set(header) != expected_columns:
            raise Exception(
                f"{path}: expected the following columns: {', '.join(expected_columns)}, got {', '.join(header)}"
            )
        header_to_index_prime = {header: idx for idx, header in enumerate(header)}
        return [
            convert_row(path, header_to_index_prime, row, row_idx + 2)
            for row_idx, row in enumerate(reader)
        ]


def convert_row(
    path, header_to_index: Mapping[str, int], row: List[str], row_idx: int
) -> CrystalLine:
    directory = row[header_to_index[COLUMN_DIRECTORY]].strip()
    name = row[header_to_index[COLUMN_NAME]].strip()
    estimated_resolution = row[header_to_index[COLUMN_ESTIMATED_RESOLUTION]].strip()
    run_id_str = row[header_to_index[COLUMN_RUN_ID]].strip()
    try:
        run_id = int(run_id_str)
    except:
        raise Exception(
            f"{path}, line {row_idx}: {COLUMN_RUN_ID} is not an integer: {run_id_str}"
        )
    if name == "":
        raise Exception(f"{path}, line {row_idx}: {COLUMN_NAME} is empty!")
    if directory == "":
        raise Exception(f"{path}, line {row_idx}: {COLUMN_DIRECTORY} is empty!")
    try:
        outcome = DiffractionType(row[header_to_index[COLUMN_OUTCOME]].strip())
    except:
        raise Exception(
            f"{path}, line {row_idx}: {COLUMN_OUTCOME} is not valid, expecting one of "
            + ", ".join(o.value for o in DiffractionType)
        )
    comment = row[header_to_index[COLUMN_COMMENT]].strip()
    return CrystalLine(
        line_number=row_idx,
        directory=directory,
        name=name,
        outcome=outcome,
        comment=comment,
        estimated_resolution=estimated_resolution,
        run_id=run_id,
    )
