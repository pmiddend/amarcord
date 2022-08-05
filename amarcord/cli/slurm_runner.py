import asyncio
import logging
from datetime import timedelta
from pathlib import Path

import structlog.stdlib
from tap import Tap

from amarcord.amici.slurm.job_status import JobStatus
from amarcord.amici.slurm.slurm_rest_job_controller import DynamicTokenRetriever
from amarcord.amici.slurm.slurm_rest_job_controller import SlurmRestJobController
from amarcord.amici.slurm.slurm_rest_job_controller import retrieve_jwt_token

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = structlog.stdlib.get_logger(__name__)


class Arguments(Tap):
    partition: str = "cfel-cdi"
    user_id: int
    user_name: str
    destination_path: Path
    executable: Path
    command_line: str
    time_limit_minutes: int = 60


args = Arguments(underscores_to_dashes=True).parse_args()
job_controller = SlurmRestJobController(
    partition=args.partition,
    token_retriever=DynamicTokenRetriever(retrieve_jwt_token),
    user_id=args.user_id,
    rest_user=args.user_name,
)


async def run_main() -> None:
    result = await job_controller.start_job(
        path=args.destination_path,
        executable=args.executable,
        command_line=args.command_line,
        time_limit=timedelta(minutes=args.time_limit_minutes),
        extra_files=[],
    )

    logger.info(
        f"job started, output dir {result.output_directory}, metadata: {result.metadata}"
    )

    done = False
    while not done:
        await asyncio.sleep(1.0)
        jobs = await job_controller.list_jobs()
        logger.info(f"found {len(jobs)} job(s)...")
        for job in jobs:
            logger.info(f"job {job}")
            if job_controller.equals(job.metadata, result.metadata):
                logger.info(f"found our job, status: {job.status}")
                if job.status == JobStatus.COMPLETED:
                    logger.info("completed, breaking")
                    done = True
                    break


asyncio.run(run_main())
