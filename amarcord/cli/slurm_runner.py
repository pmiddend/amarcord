import asyncio
import json
from datetime import timedelta
from pathlib import Path
from typing import Optional

import structlog
from tap import Tap

from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager_factory import (
    create_workload_manager,  # NOQA
)
from amarcord.amici.workload_manager.workload_manager_factory import (
    parse_workload_manager_uri,  # NOQA
)

logger = structlog.stdlib.get_logger(__name__)


class Arguments(Tap):
    workload_manager_uri: str
    working_directory: Path
    executable: Path
    command_line: str
    time_limit_minutes: int
    stdout: Optional[Path] = None
    stderr: Optional[Path] = None
    explicit_node: Optional[str] = None


async def _main_loop(args: Arguments) -> None:
    workload_manager = create_workload_manager(
        parse_workload_manager_uri(args.workload_manager_uri)
    )

    start_result = await workload_manager.start_job(
        args.working_directory,
        args.executable,
        args.command_line,
        timedelta(minutes=args.time_limit_minutes),
        args.stdout,
        args.stderr,
    )

    logger.info(
        f"started job; id {start_result.job_id}, metadata: {json.dumps(start_result.metadata)}"
    )

    while True:
        logger.info("checking job status...")

        for job in await workload_manager.list_jobs():
            if job.id != start_result.job_id:
                continue
            if job.status == JobStatus.RUNNING:
                logger.info("job is still running")
            elif job.status == JobStatus.QUEUED:
                logger.info("job is queued")
            elif job.status == JobStatus.FAILED:
                logger.info("job has failed")
                return
            else:
                logger.info("job complete")
                return

        await asyncio.sleep(3.0)


def main() -> None:
    asyncio.run(_main_loop(Arguments(underscores_to_dashes=True).parse_args()))


if __name__ == "__main__":
    main()
