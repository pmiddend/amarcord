import asyncio
import datetime
import getpass
import json
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from typing import Awaitable
from typing import Callable
from typing import Final
from typing import TypedDict

import aiohttp
import structlog
from aiohttp import BasicAuth
from aiohttp import ContentTypeError
from pydantic import BaseModel

from amarcord.amici.workload_manager.job import Job
from amarcord.amici.workload_manager.job import JobMetadata
from amarcord.amici.workload_manager.slurm_util import parse_job_state
from amarcord.amici.workload_manager.workload_manager import JobStartError
from amarcord.amici.workload_manager.workload_manager import JobStartResult
from amarcord.amici.workload_manager.workload_manager import WorkloadManager
from amarcord.json_types import JSONDict

_SLURM_TOKEN_PREFIX = "SLURM_TOKEN="  # noqa: S105
MAXWELL_PREFIX: Final = "https://max-slurm-rest.desy.de"
MAXWELL_SLURM_URL: Final = f"{MAXWELL_PREFIX}/sapi/slurm"
MAXWELL_SLURM_TOOLS_VERSION: Final = "4.6.0"

logger = structlog.stdlib.get_logger(__name__)


@dataclass(eq=True, frozen=True)
class TokenRetrievalError:
    message: str


def slurm_token_command(lifespan_minutes: int | float) -> list[str]:
    return ["slurm_token", "-l", str(int(lifespan_minutes))]


async def retrieve_jwt_token_externally(
    portal_token: str,
    user_name: str,
    lifespan_seconds: int,
) -> str | TokenRetrievalError:
    try:
        async with (
            aiohttp.ClientSession(
                auth=BasicAuth(user_name, portal_token),
            ) as session,
            session.get(
                # This currently hard-codes dynamic token retrieval to DESY's Maxwell, but it should be easy to generalize.
                f"{MAXWELL_PREFIX}/reservation/get_new_slurm_token?cli_api={MAXWELL_SLURM_TOOLS_VERSION}&lifespan={lifespan_seconds}&stu={user_name}",
            ) as response,
        ):
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
                    f'got no "token" in JSON response: {json_content}',
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
        result = await asyncio.create_subprocess_shell(
            " ".join(slurm_token_command(lifespan_seconds)),
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
        )

        stdout, stderr = await result.communicate()

        if result.returncode != 0:
            return TokenRetrievalError(
                f"slurm_token gave an error trying to get a token! standard output is {stdout.decode('utf-8')}, standard error is {stderr.decode('utf-8')}",
            )

        if not stdout.startswith(_SLURM_TOKEN_PREFIX.encode("utf-8")):
            return TokenRetrievalError(
                f"scontrol output did not start with {_SLURM_TOKEN_PREFIX}= but was {result.stdout}",
            )

        return stdout.decode("utf-8")[len(_SLURM_TOKEN_PREFIX) :].strip()
    except FileNotFoundError:
        return TokenRetrievalError(
            'Couldn\'t find the "slurm_token" tool! Are you on Maxwell?',
        )


TokenRetriever = Callable[[], Awaitable[str]]


class ConstantTokenRetriever:
    def __init__(self, token: str) -> None:
        self.token = token

    async def __call__(self) -> str:
        return self.token


class DynamicTokenRetriever:
    def __init__(
        self,
        retriever: Callable[[int], Awaitable[str | TokenRetrievalError]],
    ) -> None:
        self._token_lifetime_seconds = 86400
        self._retriever = retriever
        self._token: None | str = None
        self._last_retrieval = datetime.datetime.now(datetime.timezone.utc)

    async def __call__(self) -> str:
        now = datetime.datetime.now(datetime.timezone.utc)
        if (
            self._token is None
            or (now - self._last_retrieval).total_seconds()
            > self._token_lifetime_seconds
        ):
            logger.info("renewing jwt token")
            token = await self._retriever(self._token_lifetime_seconds)
            if isinstance(token, TokenRetrievalError):
                raise Exception(f"couldn't retrieve token: {token.message}")
            logger.info(f"got a new token: {token}")
            self._token = token
        return self._token


class JsonSlurmJobPre40(BaseModel):
    job_state: str
    start_time: int | float
    job_id: int


class JsonSlurmTime(BaseModel):
    set: bool
    infinite: bool
    number: int | float


class JsonSlurmJobState(BaseModel):
    current: list[str]


class JsonSlurmJobTime(BaseModel):
    start: int


class JsonSlurmJob(BaseModel):
    time: JsonSlurmJobTime
    state: JsonSlurmJobState
    job_id: int


def _convert_job(job_in: JSONDict) -> None | Job:
    try:
        job = JsonSlurmJobPre40(**job_in)  # type: ignore
        return Job(
            status=parse_job_state(job.job_state),
            started=datetime.datetime.fromtimestamp(
                job.start_time,
                tz=datetime.timezone.utc,
            ),
            metadata=JobMetadata({"job_id": job.job_id}),
            id=job.job_id,
        )
    except:
        try:
            job = JsonSlurmJob(**job_in)  # type: ignore
            return Job(
                status=parse_job_state(job.state.current[0]),
                started=datetime.datetime.fromtimestamp(
                    job.time.start,
                    tz=datetime.timezone.utc,
                ),
                metadata=JobMetadata({"job_id": job.job_id}),
                id=job.job_id,
            )
        except:
            logger.exception("couldn't deserialize job")
            return None


class SlurmError(TypedDict):
    error_code: int
    error: str


class SlurmHttpWrapper:
    async def post(
        self,
        url: str,
        headers: dict[str, Any],
        data: JSONDict,
    ) -> JSONDict: ...

    async def get(
        self,
        url: str,
        headers: dict[str, Any],
    ) -> JSONDict: ...


class SlurmRequestsHttpWrapper(SlurmHttpWrapper):
    async def post(self, url: str, headers: dict[str, Any], data: JSONDict) -> JSONDict:
        async with (
            aiohttp.ClientSession() as session,
            session.post(url, headers=headers, json=data) as response,
        ):
            return await response.json()  # type: ignore

    async def get(self, url: str, headers: dict[str, Any]) -> JSONDict:
        async with (
            aiohttp.ClientSession() as session,
            session.get(url, headers=headers) as response,
        ):
            return await response.json()  # type: ignore


class SlurmRestWorkloadManager(WorkloadManager):
    # Super class is Protocol which gives an error (protocols aren't instantiated)
    def __init__(
        self,
        partition: str,
        reservation: None | str,
        explicit_node: None | str,
        token_retriever: TokenRetriever,
        request_wrapper: SlurmHttpWrapper,
        api_version: str,
        rest_url: str,
        rest_user: None | str = None,
    ) -> None:
        self.partition = partition
        self._reservation = reservation
        self._explicit_node = explicit_node
        self._token_retriever = token_retriever
        self.rest_url = rest_url
        self._rest_user = rest_user if rest_user is not None else getpass.getuser()
        self._request_wrapper = request_wrapper
        self.api_version = api_version

    def name(self) -> str:
        return "Slurm REST"

    async def _headers(self) -> dict[str, str]:
        return {
            "Content-Type": "application/json",
            "X-SLURM-USER-NAME": self._rest_user,
            "X-SLURM-USER-TOKEN": await self._token_retriever(),
        }

    async def get_token(self) -> str:
        return await self._token_retriever()

    async def start_job(
        self,
        working_directory: Path,
        script: str,
        name: str,
        time_limit: datetime.timedelta,
        environment: dict[str, str],
        stdout: None | Path = None,
        stderr: None | Path = None,
    ) -> JobStartResult:
        url = f"{self.rest_url}/sapi/slurm/{self.api_version}/job/submit"
        headers_output = json.dumps(await self._headers())
        logger.info(
            f"sending the following script (excerpt) to {url} (headers {headers_output}): {script[0:50]}...",
        )
        time_limit_number = int(time_limit.total_seconds()) // 60
        environment_extended = {
            "SHELL": "/bin/bash",
            "PATH": "/bin:/usr/bin:/usr/local/bin",
            "LD_LIBRARY_PATH": "/lib/:/lib64/:/usr/local/lib",
        } | environment
        env_in_dict: dict[str, str] | list[str] = (
            environment_extended
            if self.api_version <= "v0.0.39"
            else [f"{k}={v}" for k, v in environment_extended.items()]
        )
        job_dict: dict[
            str,
            int
            | str
            | float
            | dict[str, str]
            | list[str]
            | dict[str, bool | int | float],
        ] = {
            "current_working_directory": str(working_directory),
            "time_limit": time_limit_number
            if self.api_version <= "v0.0.39"
            else {"set": True, "number": time_limit_number},
            "name": name,
            "environment": env_in_dict,
            "partition": self.partition,
            "standard_output": (
                str(working_directory / "stdout.txt") if stdout is None else str(stdout)
            ),
            "standard_error": (
                str(working_directory / "stderr.txt") if stderr is None else str(stderr)
            ),
        }
        if self._reservation is not None:
            job_dict["reservation"] = self._reservation
        if self._explicit_node is not None:
            job_dict["nodelist"] = self._explicit_node
        json_request: JSONDict = {
            "script": script,
            "job": job_dict,
        }
        logger.info(
            f"sending the following request job: {job_dict}",
        )
        try:
            response = await self._request_wrapper.post(
                url,
                headers=await self._headers(),
                data=json_request,
            )
        except Exception as e:
            raise JobStartError(f"error starting job {e}")
        logger.info(f"response was {json.dumps(response)}")
        response_json = response
        # We should use pydantic here instead of this "type error"
        errors: None | list[SlurmError] = response_json.get("errors")  # type: ignore
        if errors is not None and errors:
            raise JobStartError(
                "there were workload_manager errors: "
                + ",".join(f"{s['error_code']}: {s['error']}" for s in errors),
            )
        job_id = response_json.get("job_id", None)
        if job_id is None:
            raise JobStartError(
                "workload_manager response didn't contain a job ID: "
                + json.dumps(response_json),
            )
        if not isinstance(job_id, int):
            raise JobStartError(
                f"workload_manager's job ID was {job_id} instead of an integer",
            )
        return JobStartResult(
            job_id=job_id,
            metadata=JobMetadata({"job_id": job_id}),
        )

    async def list_jobs(self) -> list[Job]:
        # The default is to get all jobs (for the user), but this
        # might be years of job history. We artifically constrain this
        # to "the last month" for now. Let's see if we get more
        # requirements.
        one_month_s = 30 * 24 * 60 * 60
        start_time_s = time.time_ns() // 1000 // 1000 // 1000 - one_month_s
        request_url = f"{self.rest_url}/sapi/slurmdb/{self.api_version}/jobs?users={self._rest_user}&start_time={start_time_s}"
        response = await self._request_wrapper.get(
            request_url,
            headers=await self._headers(),
        )
        errors = response.get("errors", None)
        assert errors is None or isinstance(errors, list)
        if errors is not None and errors:
            raise Exception(
                f"list job request URL\n\n{request_url}\n\nContained errors: "
                + ",".join(str(e) for e in errors),
            )
        if "jobs" not in response:
            raise Exception(
                "didn't get any jobs in the response: " + json.dumps(response),
            )
        jobs = response.get("jobs", [])
        assert isinstance(jobs, list)
        # pyright rightfully complains that this doesn't have to be a JSONDict
        return [
            j
            for j in (_convert_job(job) for job in jobs)  # pyright: ignore
            if j is not None
        ]
