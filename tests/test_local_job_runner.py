import asyncio
from datetime import timedelta
from pathlib import Path

from amarcord.amici.slurm.job_status import JobStatus
from amarcord.amici.slurm.local_job_controller import LocalJobController


async def test_main_loop(tmp_path: Path) -> None:
    jc = LocalJobController()

    await jc.start_job(
        path=tmp_path,
        executable=Path(__file__).parent / "test-script.sh",
        command_line="",
        time_limit=timedelta(),
        extra_files=[],
    )

    completed = False
    for _ in range(3):
        jobs = list(await jc.list_jobs())
        assert jobs

        if jobs[0].status == JobStatus.COMPLETED:
            completed = True
            break

        await asyncio.sleep(1)

    if not completed:
        raise Exception("too slow!")
