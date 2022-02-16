import asyncio
import logging
from datetime import timedelta
from pathlib import Path
from typing import List

from tap import Tap

from amarcord.amici.slurm.local_job_controller import LocalJobController

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)


class Arguments(Tap):
    executables: List[str]
    output_dir: Path


async def main_loop(args: Arguments) -> None:
    jc = LocalJobController()

    for exe in args.executables:
        logger.info(f"starting job {exe}")
        result = await jc.start_job(
            path=args.output_dir,
            executable=Path(exe),
            command_line="",
            time_limit=timedelta(),
            extra_files=[],
        )
        logger.info(f"{exe}: pid {result.metadata}")

    while True:
        await asyncio.sleep(1.0)
        for job in await jc.list_jobs():
            logger.info(f"{job.metadata}: {job.status}")


asyncio.run(main_loop(Arguments(underscores_to_dashes=True).parse_args()))
