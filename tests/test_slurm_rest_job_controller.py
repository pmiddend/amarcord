from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import cast

import pytest
from pytest_subprocess import FakeProcess

from amarcord.amici.workload_manager.job_status import JobStatus
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    MAXWELL_SLURM_URL,  # NOQA
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    DynamicTokenRetriever,
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import SlurmHttpWrapper
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    SlurmRestWorkloadManager,  # NOQA
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    retrieve_jwt_token_on_maxwell_node,  # NOQA
)
from amarcord.amici.workload_manager.slurm_rest_workload_manager import (
    slurm_token_command,  # NOQA
)
from amarcord.json_types import JSONDict

_TEST_USER_ID = 1
_TIME_LIMIT = timedelta(minutes=60)


# Is a function because then it's awaitable and we can use it for the token retriever callback
async def _TEST_TOKEN_RETRIEVER() -> str:
    return "token"


_TEST_PARTITION = "testpartition"

_REST_USER = "pmidden"


class MockResponse:
    def __init__(
        self, json_data: None | JSONDict, text: None | str, status_code: int
    ) -> None:
        self.json_data = json_data
        self.status_code = status_code
        self.text = text

    def json(self) -> JSONDict:
        return cast(JSONDict, self.json_data)


class MockHttpWrapper(SlurmHttpWrapper):
    def __init__(self) -> None:
        self.post_requests: list[tuple[str, dict[str, Any], JSONDict]] = []
        self.get_requests: list[tuple[str, dict[str, Any]]] = []
        self.responses: list[JSONDict] = []

    async def post(self, url: str, headers: dict[str, Any], data: JSONDict) -> JSONDict:
        self.post_requests.append((url, headers, data))
        return self.responses.pop()

    async def get(self, url: str, headers: dict[str, Any]) -> JSONDict:
        self.get_requests.append((url, headers))
        return self.responses.pop()


async def test_slurm_rest_job_controller_start_job() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestWorkloadManager(
        partition=_TEST_PARTITION,
        reservation=None,
        explicit_node=None,
        token_retriever=_TEST_TOKEN_RETRIEVER,
        request_wrapper=http_wrapper,
        rest_user=_REST_USER,
        rest_url=MAXWELL_SLURM_URL,
    )

    http_wrapper.responses.append({"job_id": 1})
    await controller.start_job(
        working_directory=Path("test"),
        script='#!/bin/sh\n\necho "Done"',
        time_limit=_TIME_LIMIT,
    )

    assert http_wrapper.post_requests


async def test_slurm_rest_job_controller_list_jobs_with_errors() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestWorkloadManager(
        partition=_TEST_PARTITION,
        reservation=None,
        explicit_node=None,
        token_retriever=_TEST_TOKEN_RETRIEVER,
        request_wrapper=http_wrapper,
        rest_user=_REST_USER,
        rest_url=MAXWELL_SLURM_URL,
    )

    http_wrapper.responses.append({"errors": ["hehe"]})

    with pytest.raises(Exception):
        await controller.list_jobs()


async def test_slurm_rest_job_controller_list_jobs_other_users_are_ignored() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestWorkloadManager(
        partition=_TEST_PARTITION,
        reservation=None,
        explicit_node=None,
        token_retriever=_TEST_TOKEN_RETRIEVER,
        request_wrapper=http_wrapper,
        rest_user=_REST_USER,
        rest_url=MAXWELL_SLURM_URL,
    )

    http_wrapper.responses.append({"jobs": [{"user_id": 2}]})
    assert not await controller.list_jobs()


async def test_slurm_rest_job_controller_list_jobs_success() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestWorkloadManager(
        partition=_TEST_PARTITION,
        reservation=None,
        explicit_node=None,
        token_retriever=_TEST_TOKEN_RETRIEVER,
        request_wrapper=http_wrapper,
        rest_user=_REST_USER,
        rest_url=MAXWELL_SLURM_URL,
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
        tr = DynamicTokenRetriever(retrieve_jwt_token_on_maxwell_node)
        await tr()


async def test_dynamic_token_retriever_jwt_output(fake_process: FakeProcess) -> None:
    fake_process.register_subprocess(
        slurm_token_command(86400), stdout=b"SLURM_TOKEN=lol"
    )

    tr = DynamicTokenRetriever(retrieve_jwt_token_on_maxwell_node)

    assert await tr() == "lol"
