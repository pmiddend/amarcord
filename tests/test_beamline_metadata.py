import datetime
from pathlib import Path

import pytest

from amarcord.amici.petra3.beamline_metadata import parse_beamline_metadata
from amarcord.amici.workload_manager.job import Job
from amarcord.amici.workload_manager.job import JobMetadata
from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.slurm_remote_workload_manager import (
    decode_job_list_result,
)


def test_parse_beamline_metadata() -> None:
    metadata = parse_beamline_metadata(
        Path(__file__).parent / "beamline_metadata" / "test-metadata.json",
    )
    assert metadata.beamtimeId == "11010000"
    assert len(metadata.onlineAnalysis.reservedNodes) == 2


@pytest.mark.parametrize(
    "input_lines,jobs_or_none",
    [
        (
            "HEADER_PLEASE_IGNORE\n12697363,R,2022-09-22T09:02:31",
            [
                Job(
                    status=JobStatus.RUNNING,
                    started=datetime.datetime(
                        year=2022,
                        month=9,
                        day=22,
                        hour=9,
                        minute=2,
                        second=31,
                    ),
                    id=12697363,
                    metadata=JobMetadata({}),
                ),
            ],
        ),
        ("HEADER_PLEASE_IGNORE\nabc,R,2022-09-22T09:02:31", None),
    ],
)
def test_decode_job_list_result(
    input_lines: str,
    jobs_or_none: None | list[Job],
) -> None:
    if jobs_or_none is None:
        with pytest.raises(Exception):
            decode_job_list_result(input_lines, b"")
    else:
        assert decode_job_list_result(input_lines, b"") == jobs_or_none
