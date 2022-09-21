import asyncio
from datetime import timedelta
from pathlib import Path

from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.local_workload_manager import LocalWorkloadManager


async def test_main_loop(tmp_path: Path) -> None:
    jc = LocalWorkloadManager()

    await jc.start_job(
        working_directory=tmp_path,
        executable=Path(__file__).parent / "test-script.sh",
        command_line="",
        time_limit=timedelta(),
    )

    completed = False
    for _ in range(3):
        jobs = list(await jc.list_jobs())
        assert jobs

        if jobs[0].status == JobStatus.SUCCESSFUL:
            completed = True
            break

        await asyncio.sleep(1)

    if not completed:
        raise Exception("too slow!")
