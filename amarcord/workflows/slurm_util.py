from amarcord.workflows.job_status import JobStatus


def parse_job_state(s: str) -> JobStatus:
    return (
        JobStatus.RUNNING
        if s in ("PENDING", "RUNNING", "REQUEUED", "RESIZING", "SUSPENDED")
        else JobStatus.COMPLETED
    )


def build_sbatch(content: str) -> str:
    return f"""#!/bin/sh

{content}
    """
