import datetime
from dataclasses import dataclass
from pathlib import Path
from typing import Iterable
from typing import List
from typing import Optional
from uuid import UUID
from uuid import uuid4

import pytest
import sqlalchemy as sa
from hypothesis import given
from hypothesis.strategies import characters
from hypothesis.strategies import integers
from hypothesis.strategies import text

from amarcord.amici.p11.db import DiffractionType
from amarcord.amici.p11.db import table_crystals
from amarcord.amici.p11.db import table_data_reduction
from amarcord.amici.p11.db import table_diffractions
from amarcord.amici.p11.db import table_pucks
from amarcord.amici.p11.db import table_reduction_jobs
from amarcord.amici.p11.db import table_tools
from amarcord.clock import Clock
from amarcord.clock import MockClock
from amarcord.modules.dbcontext import Connection
from amarcord.modules.dbcontext import CreationMode
from amarcord.modules.dbcontext import DBContext
from amarcord.modules.json import JSONDict
from amarcord.workflows.job import Job
from amarcord.workflows.job_controller import JobController
from amarcord.workflows.job_controller import JobStartResult
from amarcord.workflows.job_status import JobStatus
from amarcord.workflows.workflow_synchronize import check_jobs
from amarcord.workflows.workflow_synchronize import process_tool_command_line


@given(text(alphabet=characters(blacklist_characters="$")))
def test_process_command_line_simple_string(s: str) -> None:
    # If we don't use the special character $, we shouldn't see any difference in replacement
    assert process_tool_command_line(s, Path(), {}) == s


def test_process_string_input() -> None:
    assert (
        process_tool_command_line("foo${bar}baz", Path(), {"bar": "xyz"}) == "fooxyzbaz"
    )


@given(integers())
def test_process_int_input(i: int) -> None:
    assert process_tool_command_line("foo${bar}baz", Path(), {"bar": i}) == f"foo{i}baz"


def test_process_unconsumed_string_input() -> None:
    assert process_tool_command_line("foo${bar}baz", Path(), {}) == "foo${bar}baz"


def test_process_diffraction_path() -> None:
    assert (
        process_tool_command_line("foo${diffraction.path}baz", Path("/tmp"), {})
        == "foo/tmpbaz"
    )


def test_process_array_input() -> None:
    with pytest.raises(Exception):
        process_tool_command_line("foo${bar}baz", Path(), {"bar": []})


@dataclass
class MockJob:
    relative_sub_path: Path
    executable: Path
    command_line: str
    extra_files: List[Path]
    status: JobStatus
    started: datetime.datetime
    metadata: JSONDict
    job_id: UUID


class MockJobController(JobController):
    def __init__(self, clock: Clock, output_dir: Path = Path("/tmp/")) -> None:
        self.output_dir = output_dir
        self.clock = clock
        self.started_jobs: List[MockJob] = []

    def start_job(
        self,
        relative_sub_path: Path,
        executable: Path,
        command_line: str,
        extra_files: List[Path],
    ) -> JobStartResult:
        job_id = uuid4()
        metadata = {"job_id": str(job_id)}
        self.started_jobs.append(
            MockJob(
                relative_sub_path,
                executable,
                command_line,
                extra_files,
                JobStatus.RUNNING,
                self.clock.now(),
                metadata,
                job_id,
            )
        )
        return JobStartResult(metadata=metadata, output_directory=self.output_dir)

    def list_jobs(self) -> Iterable[Job]:
        return [Job(j.status, j.started, j.metadata) for j in self.started_jobs]

    def equals(self, metadata_a: JSONDict, metadata_b: JSONDict) -> bool:
        return metadata_a["job_id"] == metadata_b["job_id"]


TEST_CRYSTAL_ID = "crystal_id"
TEST_RUN_ID = 1
TEST_EXECUTABLE_PATH = "/usr/bin/test"
TEST_COMMAND_LINE = ""
TEST_EXTRA_FILES = ["/tmp/extra"]


class SynchronizerTestScenario:
    def __init__(
        self, diffraction_filename_pattern: Optional[str] = "/tmp/*.cbf"
    ) -> None:
        self.dbcontext = DBContext("sqlite://")
        self.clock = MockClock(datetime.datetime(1987, 8, 21, 15, 0, 0, 0))
        self.job_controller = MockJobController(self.clock)

        with self.dbcontext.connect() as conn:
            self.table_pucks = table_pucks(self.dbcontext.metadata)
            self.table_crystals = table_crystals(
                self.dbcontext.metadata, self.table_pucks
            )
            self.table_tools = table_tools(self.dbcontext.metadata)
            self.table_diffractions = table_diffractions(
                self.dbcontext.metadata, self.table_crystals
            )
            self.table_reduction_jobs = table_reduction_jobs(
                self.dbcontext.metadata, self.table_tools, self.table_diffractions
            )
            self.table_data_reduction = table_data_reduction(
                self.dbcontext.metadata, self.table_crystals
            )
            self.dbcontext.create_all(CreationMode.DONT_CHECK)
            conn.execute(
                sa.insert(self.table_crystals).values(crystal_id=TEST_CRYSTAL_ID)
            )
            tool_result = conn.execute(
                sa.insert(self.table_tools).values(
                    name="test_tool",
                    executable_path=TEST_EXECUTABLE_PATH,
                    extra_files=TEST_EXTRA_FILES,
                    command_line=TEST_COMMAND_LINE,
                    description="",
                )
            )
            conn.execute(
                sa.insert(self.table_diffractions).values(
                    crystal_id=TEST_CRYSTAL_ID,
                    run_id=TEST_RUN_ID,
                    diffraction=DiffractionType.success,
                    data_raw_filename_pattern=diffraction_filename_pattern,
                )
            )
            self.tool_id = tool_result.inserted_primary_key[0]
            job_result = conn.execute(
                sa.insert(self.table_reduction_jobs).values(
                    status=JobStatus.QUEUED,
                    queued=self.clock.now(),
                    run_id=TEST_RUN_ID,
                    crystal_id=TEST_CRYSTAL_ID,
                    tool_id=self.tool_id,
                    tool_inputs={},
                )
            )
            self.job_id = job_result.inserted_primary_key[0]

    def check_jobs(self, conn: Connection) -> None:
        check_jobs(
            self.job_controller,
            conn,
            self.table_tools,
            self.table_reduction_jobs,
            self.table_diffractions,
            self.table_data_reduction,
        )


def test_check_jobs_no_diffraction_path() -> None:
    scenario = SynchronizerTestScenario(diffraction_filename_pattern=None)

    with scenario.dbcontext.connect() as conn:
        # Job shouldn't be started, because we have no path!
        scenario.check_jobs(conn)

        assert not scenario.job_controller.started_jobs

        job_after_start = conn.execute(
            sa.select(
                [
                    scenario.table_reduction_jobs.c.status,
                    scenario.table_reduction_jobs.c.failure_reason,
                ]
            )
        ).fetchone()

        assert job_after_start["status"] == JobStatus.COMPLETED
        assert job_after_start["failure_reason"]


def test_check_jobs_result_file_doesnt_exist() -> None:
    scenario = SynchronizerTestScenario()

    with scenario.dbcontext.connect() as conn:
        # Now let the daemon run a single time. It should start the job using the controller, logging that in the DB
        scenario.check_jobs(conn)

        assert len(scenario.job_controller.started_jobs) == 1
        assert scenario.job_controller.started_jobs[0].extra_files == [
            Path(p) for p in TEST_EXTRA_FILES
        ]
        assert scenario.job_controller.started_jobs[0].command_line == TEST_COMMAND_LINE

        job_after_start = conn.execute(
            sa.select([scenario.table_reduction_jobs.c.status])
        ).fetchone()

        assert job_after_start["status"] == JobStatus.RUNNING

        # Now simulate the job completing
        scenario.job_controller.started_jobs[0].status = JobStatus.COMPLETED

        scenario.check_jobs(conn)

        # This should fail, since we haven't specified correct paths
        job_after_start = conn.execute(
            sa.select(
                [
                    scenario.table_reduction_jobs.c.status,
                    scenario.table_reduction_jobs.c.failure_reason,
                ]
            )
        ).fetchone()

        assert job_after_start["status"] == JobStatus.COMPLETED
        assert job_after_start["failure_reason"]


def test_check_jobs_invalid_json_file(fs) -> None:
    scenario = SynchronizerTestScenario()

    with scenario.dbcontext.connect() as conn:
        # Start job
        scenario.check_jobs(conn)

        # Complete it
        scenario.job_controller.started_jobs[0].status = JobStatus.COMPLETED

        # Fake result
        fs.add_real_file(
            Path(__file__).parent / "amarcord-output-invalid.json",
            target_path=str(
                scenario.job_controller.output_dir / "amarcord-output.json"
            ),
        )

        scenario.check_jobs(conn)

        # This should fail, since we haven't specified correct paths
        job_after_start = conn.execute(
            sa.select(
                [
                    scenario.table_reduction_jobs.c.status,
                    scenario.table_reduction_jobs.c.failure_reason,
                ]
            )
        ).fetchone()

        assert job_after_start["status"] == JobStatus.COMPLETED
        assert job_after_start["failure_reason"]

        assert (
            conn.execute(
                sa.select([scenario.table_data_reduction.c.data_reduction_id])
            ).fetchone()
            is None
        )


def test_check_jobs_valid_result(fs) -> None:
    scenario = SynchronizerTestScenario()

    with scenario.dbcontext.connect() as conn:
        # Start job
        scenario.check_jobs(conn)

        # Complete it
        scenario.job_controller.started_jobs[0].status = JobStatus.COMPLETED

        # Fake result
        fs.add_real_file(
            Path(__file__).parent / "amarcord-output.json",
            target_path=str(
                scenario.job_controller.output_dir / "amarcord-output.json"
            ),
        )

        scenario.check_jobs(conn)

        job_after_start = conn.execute(
            sa.select(
                [
                    scenario.table_reduction_jobs.c.status,
                    scenario.table_reduction_jobs.c.failure_reason,
                ]
            )
        ).fetchone()

        assert job_after_start["status"] == JobStatus.COMPLETED
        assert not job_after_start["failure_reason"]

        assert conn.execute(
            sa.select([scenario.table_data_reduction.c.data_reduction_id])
        ).fetchone()


def test_check_jobs_valid_result_but_forgotten_by_job_controller(fs) -> None:
    scenario = SynchronizerTestScenario()

    with scenario.dbcontext.connect() as conn:
        # Start job
        scenario.check_jobs(conn)

        # Now remove it. This might happen if the daemon crashes. The entry will be in the DB, but not in the
        # job list anymore.
        scenario.job_controller.started_jobs.clear()
        scenario.clock.advance_minutes(10)

        # Fake result
        fs.add_real_file(
            Path(__file__).parent / "amarcord-output.json",
            target_path=str(
                scenario.job_controller.output_dir / "amarcord-output.json"
            ),
        )

        scenario.check_jobs(conn)

        job_after_start = conn.execute(
            sa.select(
                [
                    scenario.table_reduction_jobs.c.status,
                    scenario.table_reduction_jobs.c.failure_reason,
                ]
            )
        ).fetchone()

        assert job_after_start["status"] == JobStatus.COMPLETED
        assert not job_after_start["failure_reason"]

        assert conn.execute(
            sa.select([scenario.table_data_reduction.c.data_reduction_id])
        ).fetchone()
