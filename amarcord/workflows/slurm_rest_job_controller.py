import datetime
import json
import logging
from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import TypedDict

import requests
from requests import Response

from amarcord.modules.json import JSONDict
from amarcord.workflows.job import Job
from amarcord.workflows.job_controller import JobController
from amarcord.workflows.job_controller import JobStartResult
from amarcord.workflows.slurm_util import build_sbatch
from amarcord.workflows.slurm_util import parse_job_state

logger = logging.getLogger(__name__)


def _convert_job(job: Dict[str, Any]) -> Job:
    logger.info("got job %s, state %s", job["job_id"], job["job_state"])
    return Job(
        status=parse_job_state(job["job_state"]),
        started=datetime.datetime.utcfromtimestamp(job["start_time"]),
        metadata={"job_id": job["job_id"]},
    )


class SlurmError(TypedDict):
    error_code: int
    error: str


class SlurmHttpWrapper:
    # pylint: disable=unused-argument,no-self-use
    def post(self, url: str, headers: Dict[str, Any], data: str) -> Response:
        ...

    # pylint: disable=unused-argument,no-self-use
    def get(self, url: str, headers: Dict[str, Any]) -> Response:
        ...


class SlurmRequestsHttpWrapper(SlurmHttpWrapper):
    def post(self, url: str, headers: Dict[str, Any], data: str) -> Response:
        return requests.post(url, headers, data)

    def get(self, url: str, headers: Dict[str, Any]) -> Response:
        return requests.get(url, headers)


class SlurmRestJobController(JobController):
    def __init__(
        self,
        output_base_dir: Path,
        partition: str,
        jwt_token: str,
        user_id: int,
        rest_url: str = "http://max-portal.desy.de:6820/slurm/v0.0.36",
        rest_user: str = "pmidden",
        request_wrapper: SlurmHttpWrapper = SlurmRequestsHttpWrapper(),
    ) -> None:
        super().__init__()
        self._output_base_dir = output_base_dir
        self._partition = partition
        self._jwt_token = jwt_token
        self._rest_url = rest_url
        self._rest_user = rest_user
        self._user_id = user_id
        self._request_wrapper = request_wrapper

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-SLURM-USER-NAME": self._rest_user,
            "X-SLURM-USER-TOKEN": self._jwt_token,
        }

    def start_job(
        self,
        relative_sub_path: Path,
        executable: Path,
        command_line: str,
        extra_files: List[Path],
    ) -> JobStartResult:
        chdir = self._output_base_dir / relative_sub_path
        copy_string = "set -eux\n"
        copy_string += f"mkdir -p {chdir}\n"
        copy_string += f"cd {chdir}\n"
        for file in [executable] + extra_files:
            copy_string += f'[ ! -f "{file.name}" ] && cp "{file}" .\n'
        copy_string += f"\n./{executable.name} {command_line} >stdout.txt 2>stderr.txt"
        sbatch_content = build_sbatch(content=copy_string)
        url = f"{self._rest_url}/job/submit"
        logger.info(
            "sending the following sbatch script to %s (headers %s): %s",
            url,
            json.dumps(self._headers()),
            sbatch_content,
        )
        json_request = {
            "script": sbatch_content,
            "job": {
                "nodes": 1,
                "current_working_directory": str(self._output_base_dir),
                "environment": {
                    "HOME": f"/home/{self._rest_user}",
                    "SHELL": "/bin/bash",
                    "USER": self._rest_user,
                    "PATH": "/bin:/usr/bin:/usr/local/bin",
                    "LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib",
                },
                "partition": self._partition,
            },
        }
        response = self._request_wrapper.post(
            url, headers=self._headers(), data=json.dumps(json_request)
        )
        logger.info("response was %s: %s", response.status_code, response.text)
        response_json = response.json()
        errors: List[SlurmError] = response_json.get("errors", None)
        if errors is not None and errors:
            raise Exception(
                "there were slurm errors: "
                + ",".join(f"{s['error_code']}: {s['error']}" for s in errors)
            )
        job_id = response_json.get("job_id", None)
        if job_id is None:
            raise Exception(
                "slurm response didn't contain a job ID: " + json.dumps(response_json)
            )
        return JobStartResult({"job_id": job_id}, chdir)

    def job_matches(self, job: Dict[str, Any]) -> bool:
        return job.get("user_id", 0) == self._user_id

    def list_jobs(self) -> List[Job]:
        response = self._request_wrapper.get(
            f"{self._rest_url}/jobs", headers=self._headers()
        ).json()
        errors = response.get("errors", None)
        if errors is not None and errors:
            raise Exception("list job request contained errors: " + ",".join(errors))
        return [
            _convert_job(job)
            for job in response.get("jobs", [])
            if self.job_matches(job)
        ]

    def equals(self, metadata_a: JSONDict, metadata_b: JSONDict) -> bool:
        return metadata_a.get("job_id", None) == metadata_b.get("job_id", None)
