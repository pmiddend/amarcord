from amarcord.amici.slurm.job_status import JobStatus


def parse_job_state(s: str) -> JobStatus:
    return (
        JobStatus.RUNNING
        if s in ("PENDING", "RUNNING", "REQUEUED", "RESIZING", "SUSPENDED")
        else JobStatus.COMPLETED
    )


def build_sbatch(content: str) -> str:
    # Note bash -l here. It's pretty tricky to get a "normal" environment on SLURM, especially
    # via the REST interface. We need the -l here to force reading, for example, ~/.bashrc.
    return f"""#!/bin/bash -l

{content}
    """
