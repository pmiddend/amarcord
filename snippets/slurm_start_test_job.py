import json
import logging
from pathlib import Path
from time import sleep
from time import time

from amarcord.workflows.job_status import JobStatus
from amarcord.workflows.slurm_job_controller import SlurmJobController

logging.basicConfig(
    format="%(asctime)-15s %(levelname)s %(message)s", level=logging.INFO
)

logger = logging.getLogger(__name__)

controller = SlurmJobController(
    Path("/gpfs/cfel/cxi/scratch/user/pmidden/workflows/new-slurm"), "cfel"
)

logger.info("starting test job")

job_id = controller.start_job(
    Path(f"relative/path/{int(time())}"),
    json.dumps({"foo_id": 3}),
    Path("/home/pmidden/workflow-research/new-slurm/test-job.sh"),
    "some commands",
    [Path("/home/pmidden/workflow-research/new-slurm/extra-file.txt")],
)

logger.info("done, job ID %s", job_id)

finished = False
while not finished:
    logger.info("sleeping")
    sleep(1)

    jobs = controller.list_jobs()

    logger.info("%s job(s)", len(jobs))
    for job in jobs:
        # logger.info("job: %s %s", job.job_id, job.status)
        if job.job_id == job_id:
            logger.info("found our job %s", job.job_id)
            if job.status == JobStatus.COMPLETED:
                logger.info("job %s completed, finishing", job.job_id)
                finished = True
                break
