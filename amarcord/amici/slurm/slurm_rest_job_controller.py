import asyncio
import datetime
import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import TypedDict

import aiohttp

from amarcord.amici.slurm.job import Job
from amarcord.amici.slurm.job import JobMetadata
from amarcord.amici.slurm.job_controller import JobController
from amarcord.amici.slurm.job_controller import JobStartError
from amarcord.amici.slurm.job_controller import JobStartResult
from amarcord.amici.slurm.slurm_util import build_sbatch
from amarcord.amici.slurm.slurm_util import parse_job_state
from amarcord.json_types import JSONDict
from amarcord.util import last_line_of_file

_DESY_SLURM_URL = "https://max-portal.desy.de/sapi/slurm/v0.0.36"

logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class TokenRetrievalError:
    message: str


def slurm_token_command(lifespan_minutes: int | float) -> list[str]:
    return ["slurm_token", "-l", str(int(lifespan_minutes))]


async def retrieve_jwt_token(lifespan_seconds: int) -> str | TokenRetrievalError:
    try:
        result = await (
            asyncio.create_subprocess_shell(
                " ".join(slurm_token_command(lifespan_seconds // 60)),
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
            )
        )

        stdout, stderr = await result.communicate()

        if result.returncode != 0:
            return TokenRetrievalError(
                f"slurm_token gave an error trying to get a token! standard output is {stdout.decode('utf-8')}, standard error is {stderr.decode('utf-8')}"
            )

        PREFIX = "SLURM_TOKEN="
        if not stdout.startswith(PREFIX.encode("utf-8")):
            return TokenRetrievalError(
                f"scontrol output did not start with SLURM_JWT= but was {result.stdout}"
            )

        return stdout.decode("utf-8")[len(PREFIX) :].strip()
    except FileNotFoundError:
        return TokenRetrievalError(
            'Couldn\'t find the "slurm_token" tool! Are you on Maxwell?'
        )


TokenRetriever = Callable[[], Awaitable[str]]


class ConstantTokenRetriever:
    def __init__(self, token: str) -> None:
        self.token = token

    async def __call__(self) -> str:
        return self.token


class DynamicTokenRetriever:
    def __init__(
        self, retriever: Callable[[int], Awaitable[str | TokenRetrievalError]]
    ) -> None:
        self._token_lifetime_seconds = 86400
        self._retriever = retriever
        self._token: str | None = None
        self._last_retrieval = datetime.datetime.utcnow()

    async def __call__(self) -> str:
        now = datetime.datetime.utcnow()
        if (
            self._token is None
            or (now - self._last_retrieval).total_seconds()
            > self._token_lifetime_seconds
        ):
            logger.info("renewing jwt token")
            token = await self._retriever(self._token_lifetime_seconds)
            if isinstance(token, TokenRetrievalError):
                raise Exception(f"couldn't retrieve token: {token.message}")
            self._token = token
        return self._token


def _convert_job(job: JSONDict) -> Job | None:
    job_state = job.get("job_state", None)
    if job_state is None:
        return None
    assert isinstance(job_state, str)
    job_start_time = job.get("start_time", None)
    if job_start_time is None:
        return None
    assert isinstance(job_start_time, (float, int)), f"start time is {job_start_time}"
    job_id = job.get("job_id", None)
    if job_id is None:
        return None
    assert isinstance(job_id, int)
    return Job(
        status=parse_job_state(job_state),
        started=datetime.datetime.utcfromtimestamp(job_start_time),
        metadata=JobMetadata({"job_id": job_id}),
    )


class SlurmError(TypedDict):
    error_code: int
    error: str


class SlurmHttpWrapper:
    # pylint: disable=unused-argument
    async def post(self, url: str, headers: dict[str, Any], data: JSONDict) -> JSONDict:
        ...

    # pylint: disable=unused-argument
    async def get(self, url: str, headers: dict[str, Any]) -> JSONDict:
        ...


class SlurmRequestsHttpWrapper(SlurmHttpWrapper):
    async def post(self, url: str, headers: dict[str, Any], data: JSONDict) -> JSONDict:
        async with aiohttp.ClientSession() as session:
            async with session.post(url, headers=headers, json=data) as response:
                return await response.json()  # type: ignore

    async def get(self, url: str, headers: dict[str, Any]) -> JSONDict:
        async with aiohttp.ClientSession() as session:
            async with session.get(url, headers=headers) as response:
                return await response.json()  # type: ignore


def slurm_file_contains_preemption(p: Path) -> bool:
    if not p.is_file():
        return False
    return "DUE TO PREEMPTION ***" in last_line_of_file(p)


class SlurmRestJobController(JobController):
    # Super class is Protocol which gives an error (protocols aren't instantiated)
    # pylint: disable=super-init-not-called
    def __init__(
        self,
        partition: str,
        token_retriever: TokenRetriever,
        user_id: int,
        rest_user: str,
        rest_url: str = _DESY_SLURM_URL,
        request_wrapper: SlurmHttpWrapper = SlurmRequestsHttpWrapper(),
    ) -> None:
        self._partition = partition
        self._token_retriever = token_retriever
        self._rest_url = rest_url
        self._rest_user = rest_user
        self._user_id = user_id
        self._request_wrapper = request_wrapper

    async def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-SLURM-USER-NAME": self._rest_user,
            "X-SLURM-USER-TOKEN": await self._token_retriever(),
        }

    def should_restart(self, job_id: int, output_directory: Path) -> bool:
        return slurm_file_contains_preemption(output_directory / "stderr.txt")

    async def start_job(
        self,
        path: Path,
        executable: Path,
        command_line: str,
        time_limit: datetime.timedelta,
        extra_files: list[Path],
    ) -> JobStartResult:
        # the path may be non-empty due to preemption and restart (see next line why we're not doing that in the script
        # currently)
        if path.is_dir():
            path.rmdir()

        # Long-term, this is bad, because the reason we're using the REST API is that we then don't
        # need real physical access to the file system. This mkdir call enforces it, however.
        # The reason this is here is that if we don't create the basedir, we have to have a location for
        # the slurm.out file (which predates the shell mkdir call below)
        path.mkdir(parents=True, exist_ok=True)

        lines = ["set -eux"]

        # See the comment for mkdir above. Either we create the file in the script, or here.
        # copy_string += f"mkdir -p {chdir}\n"
        # copy_string += f"cd {chdir}\n"

        # pylint: disable=use-list-copy
        for file in [executable] + extra_files:
            lines.append(f'[ ! -f "{file.name}" ] && cp "{file}" .')
        lines.append(f"./{executable.name} {command_line}")
        sbatch_content = build_sbatch(content="\n".join(lines))
        url = f"{self._rest_url}/job/submit"
        logger.info(
            "sending the following sbatch script to %s (headers %s): %s",
            url,
            json.dumps(await self._headers()),
            sbatch_content,
        )
        json_request: JSONDict = {
            "script": sbatch_content,
            "job": {
                "nodes": 1,
                "current_working_directory": str(path),
                "time_limit": int(time_limit.total_seconds()) // 60,
                "requeue": True,
                "environment": {
                    "SHELL": "/bin/bash",
                    "PATH": "/bin:/usr/bin:/usr/local/bin",
                    "LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib",
                },
                "partition": self._partition,
                "standard_output": str(path / "stdout.txt"),
                "standard_error": str(path / "stderr.txt"),
            },
        }
        logger.info(
            f"sending the following request: {json_request}",
        )
        try:
            response = await self._request_wrapper.post(
                url, headers=await self._headers(), data=json_request
            )
        except Exception as e:
            raise JobStartError(f"error starting job {e}")
        logger.info("response was %s", json.dumps(response))
        response_json = response
        errors: list[SlurmError] = response_json.get("errors", None)  # type: ignore
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
        return JobStartResult(
            JobMetadata({"job_id": job_id, "user_id": self._user_id}), path
        )

    def job_matches(self, job: JSONDict) -> bool:
        """Check if the given job dict (from SLURM) is "relevant for us" """
        return job.get("user_id", 0) == self._user_id

    async def list_jobs(self) -> list[Job]:
        response = await self._request_wrapper.get(
            f"{self._rest_url}/jobs", headers=await self._headers()
        )
        errors = response.get("errors", None)
        assert errors is None or isinstance(errors, list)
        if errors is not None and errors:
            raise Exception("list job request contained errors: " + ",".join(errors))
        if "jobs" not in response:
            raise Exception(
                "didn't get any jobs in the response: " + json.dumps(response)
            )
        jobs = response.get("jobs", [])
        assert isinstance(jobs, list)
        if not jobs:
            logger.info(
                "jobs array actually empty (token expired probably): %s",
                json.dumps(response),
            )
            raise Exception("jobs array empty, token expired?")
        return [
            j
            for j in (_convert_job(job) for job in jobs if self.job_matches(job))
            if j is not None
        ]

    def equals(self, metadata_a: JobMetadata, metadata_b: JobMetadata) -> bool:
        """This is used to bring together jobs in the database and jobs coming from SLURM"""
        return metadata_a.get("job_id", None) == metadata_b.get("job_id", None)

    def is_our_job(self, metadata_a: JobMetadata) -> bool:
        # Used to be that we get all sorts of jobs from Maxwell. Now we only get our own, so no check needed anymore
        return True
        # return metadata_a.get("user_id", None) == self._user_id
