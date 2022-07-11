from datetime import timedelta

from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Tuple
from typing import cast

import pytest
from pytest_subprocess import FakeProcess

from amarcord.json import JSONDict
from amarcord.amici.slurm.job_status import JobStatus
from amarcord.amici.slurm.slurm_rest_job_controller import (
    DynamicTokenRetriever,
    slurm_token_command,
)
from amarcord.amici.slurm.slurm_rest_job_controller import SlurmHttpWrapper
from amarcord.amici.slurm.slurm_rest_job_controller import SlurmRestJobController
from amarcord.amici.slurm.slurm_rest_job_controller import retrieve_jwt_token

_TEST_USER_ID = 1
_TIME_LIMIT = timedelta(minutes=60)


# Is a function because then it's awaitable and we can use it for the token retriever callback
async def _TEST_TOKEN_RETRIEVER() -> str:
    return "token"


_TEST_PARTITION = "testpartition"

_REST_USER = "pmidden"


class MockResponse:
    def __init__(
        self, json_data: JSONDict | None, text: str | None, status_code: int
    ) -> None:
        self.json_data = json_data
        self.status_code = status_code
        self.text = text

    def json(self) -> JSONDict:
        return cast(JSONDict, self.json_data)


class MockHttpWrapper(SlurmHttpWrapper):
    def __init__(self) -> None:
        self.post_requests: List[Tuple[str, Dict[str, Any], JSONDict]] = []
        self.get_requests: List[Tuple[str, Dict[str, Any]]] = []
        self.responses: List[JSONDict] = []

    async def post(self, url: str, headers: Dict[str, Any], data: JSONDict) -> JSONDict:
        self.post_requests.append((url, headers, data))
        return self.responses.pop()

    async def get(self, url: str, headers: Dict[str, Any]) -> JSONDict:
        self.get_requests.append((url, headers))
        return self.responses.pop()


async def test_slurm_rest_job_controller_start_job() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestJobController(
        partition=_TEST_PARTITION,
        token_retriever=_TEST_TOKEN_RETRIEVER,
        user_id=_TEST_USER_ID,
        request_wrapper=http_wrapper,
        rest_user=_REST_USER,
    )

    http_wrapper.responses.append({"job_id": 1})
    await controller.start_job(
        path=Path("test"),
        executable=Path("/tmp/executable"),
        command_line="",
        time_limit=_TIME_LIMIT,
        extra_files=[],
    )

    assert http_wrapper.post_requests


async def test_slurm_rest_job_controller_list_jobs_with_errors() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestJobController(
        partition=_TEST_PARTITION,
        token_retriever=_TEST_TOKEN_RETRIEVER,
        user_id=_TEST_USER_ID,
        request_wrapper=http_wrapper,
        rest_user=_REST_USER,
    )

    http_wrapper.responses.append({"errors": ["hehe"]})

    with pytest.raises(Exception):
        await controller.list_jobs()


async def test_slurm_rest_job_controller_list_jobs_other_users_are_ignored() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestJobController(
        partition=_TEST_PARTITION,
        token_retriever=_TEST_TOKEN_RETRIEVER,
        user_id=_TEST_USER_ID,
        request_wrapper=http_wrapper,
        rest_user=_REST_USER,
    )

    http_wrapper.responses.append({"jobs": [{"user_id": 2}]})
    assert not await controller.list_jobs()


async def test_slurm_rest_job_controller_list_jobs_success() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestJobController(
        partition=_TEST_PARTITION,
        token_retriever=_TEST_TOKEN_RETRIEVER,
        user_id=_TEST_USER_ID,
        request_wrapper=http_wrapper,
        rest_user=_REST_USER,
    )

    http_wrapper.responses.append(
        {
            "jobs": [
                {
                    "user_id": 1,
                    "job_id": 1,
                    "job_state": "RUNNING",
                    "start_time": 1625570627,
                }
            ]
        }
    )
    jobs = await controller.list_jobs()
    assert len(jobs) == 1
    assert jobs[0].status == JobStatus.RUNNING


async def test_dynamic_token_retriever_wrong_output(fake_process: FakeProcess) -> None:
    fake_process.register_subprocess(slurm_token_command(24 * 60), stdout=b"lol")

    with pytest.raises(Exception):
        tr = DynamicTokenRetriever(retrieve_jwt_token)
        await tr()


async def test_dynamic_token_retriever_jwt_output(fake_process: FakeProcess) -> None:
    fake_process.register_subprocess(
        slurm_token_command(24 * 60), stdout=b"SLURM_TOKEN=lol"
    )

    tr = DynamicTokenRetriever(retrieve_jwt_token)

    assert await tr() == "lol"
