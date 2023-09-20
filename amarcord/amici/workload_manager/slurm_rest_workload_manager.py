import asyncio
import datetime
import getpass
import json
import logging
import subprocess
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Final
from typing import TypedDict

import aiohttp
from aiohttp import BasicAuth
from aiohttp import ContentTypeError

from amarcord.amici.workload_manager.job import Job
from amarcord.amici.workload_manager.job import JobMetadata
from amarcord.amici.workload_manager.slurm_util import parse_job_state
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import JobStartResult
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.json_types import JSONDict
from amarcord.util import last_line_of_file

MAXWELL_PREFIX: Final = "https://max-portal.desy.de"
MAXWELL_SLURM_URL: Final = f"{MAXWELL_PREFIX}/sapi/slurm/v0.0.36"
MAXWELL_SLURM_TOOLS_VERSION: Final = "4.6.0"

logger = logging.getLogger(__name__)


@dataclass(eq=True, frozen=True)
class TokenRetrievalError:
    message: str


def slurm_token_command(lifespan_minutes: int | float) -> list[str]:
    return ["slurm_token", "-l", str(int(lifespan_minutes))]


async def retrieve_jwt_token_externally(
    portal_token: str, user_name: str, lifespan_seconds: int
) -> str | TokenRetrievalError:
    try:
        async with aiohttp.ClientSession(
            auth=BasicAuth(user_name, portal_token)
        ) as session:
            async with session.get(
                f"{MAXWELL_PREFIX}/reservation/get_new_slurm_token?cli_api={MAXWELL_SLURM_TOOLS_VERSION}&lifespan={lifespan_seconds}&stu={user_name}"
            ) as response:
                try:
                    # Maxwell says "text/html", which makes the "json" function fail. But the content
                    # really is JSON, so we can ignore Content-Type.
                    json_content = await response.json(content_type=None)
                except ContentTypeError as e:
                    return TokenRetrievalError(f"response did not contain JSON: {e}")
                if json_content is None:
                    return TokenRetrievalError("response was empty")
                token = json_content.get("token")
                if token is None:
                    return TokenRetrievalError(
                        f'got no "token" in JSON response: {json_content}'
                    )
                if not isinstance(token, str):
                    return TokenRetrievalError(f"auth token is not string but: {token}")
                return token
    except Exception as e:
        return TokenRetrievalError(f"a very unexpected error occurred: {e}")


async def retrieve_jwt_token_on_maxwell_node(
    lifespan_seconds: int,
) -> str | TokenRetrievalError:
    try:
        result = await (
            asyncio.create_subprocess_shell(
                " ".join(slurm_token_command(lifespan_seconds)),
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
                f"scontrol output did not start with {PREFIX}= but was {result.stdout}"
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
        self._token: None | str = None
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


def _convert_job(job: JSONDict) -> None | Job:
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
        id=job_id,
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


class SlurmRestWorkloadManager(WorkloadManager):
    # Super class is Protocol which gives an error (protocols aren't instantiated)
    # pylint: disable=super-init-not-called
    def __init__(
        self,
        partition: str,
        reservation: None | str,
        explicit_node: None | str,
        token_retriever: TokenRetriever,
        request_wrapper: SlurmHttpWrapper,
        rest_url: str,
        rest_user: None | str = None,
    ) -> None:
        self._partition = partition
        self._reservation = reservation
        self._explicit_node = explicit_node
        self._token_retriever = token_retriever
        self._rest_url = rest_url
        self._rest_user = rest_user if rest_user is not None else getpass.getuser()
        self._request_wrapper = request_wrapper

    async def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-SLURM-USER-NAME": self._rest_user,
            "X-SLURM-USER-TOKEN": await self._token_retriever(),
        }

    async def start_job(
        self,
        working_directory: Path,
        script: str,
        time_limit: datetime.timedelta,
        stdout: None | Path = None,
        stderr: None | Path = None,
    ) -> JobStartResult:
        url = f"{self._rest_url}/job/submit"
        logger.info(
            "sending the following script to %s (headers %s): %s",
            url,
            json.dumps(await self._headers()),
            script,
        )
        job_dict: dict[str, int | str | dict[str, str] | list[str]] = {
            "nodes": 1,
            "current_working_directory": str(working_directory),
            "time_limit": int(time_limit.total_seconds()) // 60,
            "requeue": True,
            "environment": {
                "SHELL": "/bin/bash",
                "PATH": "/bin:/usr/bin:/usr/local/bin",
                "LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib",
            },
            "partition": self._partition,
            "standard_output": str(working_directory / "stdout.txt")
            if stdout is None
            else str(stdout),
            "standard_error": str(working_directory / "stderr.txt")
            if stderr is None
            else str(stderr),
        }
        if self._reservation is not None:
            job_dict["reservation"] = self._reservation
        if self._explicit_node is not None:
            job_dict["nodelist"] = [self._explicit_node]
        json_request: JSONDict = {
            "script": script,
            "job": job_dict,
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
        # We should use pydantic here instead of this "type error"
        errors: None | list[SlurmError] = response_json.get("errors")  # type: ignore
        if errors is not None and errors:
            raise JobStartError(
                "there were workload_manager errors: "
                + ",".join(f"{s['error_code']}: {s['error']}" for s in errors)
            )
        job_id = response_json.get("job_id", None)
        if job_id is None:
            raise JobStartError(
                "workload_manager response didn't contain a job ID: "
                + json.dumps(response_json)
            )
        if not isinstance(job_id, int):
            raise JobStartError(
                f"workload_manager's job ID was {job_id} instead of an integer"
            )
        return JobStartResult(
            job_id=job_id,
            metadata=JobMetadata({"job_id": job_id}),
        )

    async def list_jobs(self) -> list[Job]:
        response = await self._request_wrapper.get(
            f"{self._rest_url}/jobs", headers=await self._headers()
        )
        errors = response.get("errors", None)
        assert errors is None or isinstance(errors, list)
        if errors is not None and errors:
            raise Exception(
                "list job request contained errors: " + ",".join(str(e) for e in errors)
            )
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
        # pyright rightfully complains that this doesn't have to be a JSONDict
        return [
            j
            for j in (_convert_job(job) for job in jobs)  # pyright: ignore
            if j is not None
        ]
