import datetime
import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Callable
from typing import Dict
from typing import List
from typing import TypedDict
from typing import Union

import requests
from requests import Response

from amarcord.modules.json import JSONDict
from amarcord.modules.p11.job import Job
from amarcord.modules.p11.slurm_util import build_sbatch
from amarcord.modules.p11.slurm_util import parse_job_state

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class JobStartResult:
    job_id: int
    output_directory: Path


class JobStartError(Exception):
    def __init__(self, message: str) -> None:
        super().__init__()
        self.message = message


def _convert_job(job: Dict[str, Any]) -> Job:
    return Job(
        status=parse_job_state(job["job_state"]),
        started=datetime.datetime.utcfromtimestamp(job["start_time"]),
        job_id=job["job_id"],
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
        return requests.post(url, headers=headers, data=data)

    def get(self, url: str, headers: Dict[str, Any]) -> Response:
        return requests.get(url, headers=headers)


@dataclass(eq=True, frozen=True)
class TokenRetrievalError:
    message: str


def retrieve_jwt_token(lifespan_seconds: int) -> Union[str, TokenRetrievalError]:
    try:
        result = subprocess.run(
            ["scontrol", "token", f"lifespan={lifespan_seconds}"],
            capture_output=True,
            check=True,
            encoding="utf-8",
        )

        if not result.stdout.startswith("SLURM_JWT="):
            return TokenRetrievalError(
                f"scontrol output did not start with SLURM_JWT= but was {result.stdout}"
            )

        return result.stdout[10:].strip()
    except FileNotFoundError:
        return TokenRetrievalError(
            'Couldn\'t find the "scontrol" tool! Are you on Maxwell?'
        )
    except subprocess.CalledProcessError as e:
        return TokenRetrievalError(
            f"scontrol gave an error trying to get a token! standard output is {e.stdout}, standard error is {e.stderr}"
        )


TokenRetriever = Callable[[], str]


class DynamicTokenRetriever:
    def __init__(
        self, retriever: Callable[[int], Union[str, TokenRetrievalError]]
    ) -> None:
        self._token_lifetime_seconds = 86400
        self._retriever = retriever
        token = self._retriever(self._token_lifetime_seconds)
        if isinstance(token, TokenRetrievalError):
            raise Exception(f"couldn't retrieve token: {token.message}")
        self._token: str = token
        self._last_retrieval = datetime.datetime.utcnow()

    def __call__(self) -> str:
        now = datetime.datetime.utcnow()
        if (now - self._last_retrieval).total_seconds() > self._token_lifetime_seconds:
            logger.info("renewing jwt token")
            token = self._retriever(self._token_lifetime_seconds)
            if isinstance(token, TokenRetrievalError):
                raise Exception(f"couldn't retrieve token: {token.message}")
            self._token = token
        return self._token


class SlurmRestJobController:
    def __init__(
        self,
        output_base_dir: Path,
        partition: str,
        token_retriever: TokenRetriever,
        user_id: int,
        rest_user: str,
        rest_url: str = "http://max-portal.desy.de/sapi/slurm/v0.0.36",
        request_wrapper: SlurmHttpWrapper = SlurmRequestsHttpWrapper(),
    ) -> None:
        super().__init__()
        self._output_base_dir = output_base_dir
        self._partition = partition
        self._token_retriever = token_retriever
        self._rest_url = rest_url
        self._rest_user = rest_user
        self._user_id = user_id
        self._request_wrapper = request_wrapper

    def _headers(self) -> Dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-SLURM-USER-NAME": self._rest_user,
            "X-SLURM-USER-TOKEN": self._token_retriever(),
        }

    def start_job(
        self,
        relative_sub_path: Path,
        command_line: str,
        extra_files: List[Path],
    ) -> JobStartResult:
        chdir = self._output_base_dir / relative_sub_path
        copy_string = "set -eux\n"
        copy_string += f"mkdir -p {chdir}\n"
        copy_string += f"cd {chdir}\n"
        for file in extra_files:
            copy_string += f'[ ! -f "{file.name}" ] && cp "{file}" .\n'
        copy_string += f"\n{command_line} >stdout.txt 2>stderr.txt"
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
                "time_limit": 24 * 60,
                "environment": {
                    # Weirdly enough, HOME isn't really set when we don't set it here.
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
            raise JobStartError(
                "there were slurm errors: "
                + ",".join(f"{s['error_code']}: {s['error']}" for s in errors)
            )
        job_id = response_json.get("job_id", None)
        if job_id is None:
            raise JobStartError(
                "slurm response didn't contain a job ID: " + json.dumps(response_json)
            )
        return JobStartResult(job_id, chdir)

    def job_matches(self, job: Dict[str, Any]) -> bool:
        """Check if the given job dict (from SLURM) is "relevant for us" """
        return job.get("user_id", 0) == self._user_id

    def list_jobs(self) -> List[Job]:
        response_raw = self._request_wrapper.get(
            f"{self._rest_url}/jobs", headers=self._headers()
        )
        try:
            response = response_raw.json()
        except:
            logger.exception("couldn't decode json response %s", response_raw.text)
            raise
        errors = response.get("errors", None)
        if errors is not None and errors:
            raise Exception("list job request contained errors: " + ",".join(errors))
        if "jobs" not in response:
            raise Exception(
                "didn't get any jobs in the response: " + json.dumps(response)
            )
        jobs = response.get("jobs", [])
        if not jobs:
            logger.info(
                "jobs array actually empty (token expired probably): %s",
                json.dumps(response),
            )
            raise Exception("jobs array empty, token expired?")
        return [_convert_job(job) for job in jobs if self.job_matches(job)]

    def is_our_job(self, metadata_a: JSONDict) -> bool:
        return metadata_a.get("user_id", None) == self._user_id
