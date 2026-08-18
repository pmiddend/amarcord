import asyncio
import json
import shlex
from datetime import timedelta

import structlog
from anyio import Path
from typed_argparse import TypedArgs
from typed_argparse import arg

from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.workload_manager_factory import (
    create_workload_manager,
)
from amarcord.amici.workload_manager.workload_manager_factory import (
    parse_workload_manager_config,
)

logger = structlog.stdlib.get_logger(__name__)


class Arguments(TypedArgs):
    workload_manager_uri: str = arg(
        help="Determines how and where jobs are started; refer to the manual on how this URL should look like"
    )
    working_directory: Path = arg(help="Working directory for the started job")
    executable: Path = arg(help="Which program to start")
    command_line: str = arg(
        help="Command line (one single string) to give to the program"
    )
    name: str = arg(help="Name of the job to start on the workload manager")
    time_limit_minutes: int = arg(help="Time limit of the job")
    stdout: Path | None = arg(default=None)
    stderr: Path | None = arg(default=None)
    explicit_node: str | None = arg(default=None)


async def _main_loop(args: Arguments) -> None:
    workload_manager = create_workload_manager(
        parse_workload_manager_config(args.workload_manager_uri),
    )

    start_result = await workload_manager.start_job(
        args.working_directory,
        name=args.name,
        script=f"""#!/bin/sh

set -eu
set -o pipefail

{args.executable} {shlex.join(args.command_line)}
        """,
        time_limit=timedelta(minutes=args.time_limit_minutes),
        environment={},
        stdout=args.stdout,
        stderr=args.stderr,
    )

    logger.info(
        f"started job; id {start_result.job_id}, metadata: {json.dumps(start_result.metadata)}",
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


def main() -> None:  # pragma: no cover
    def runner(args: Arguments) -> None:
        asyncio.run(_main_loop(args))


if __name__ == "__main__":  # pragma: no cover
    main()
