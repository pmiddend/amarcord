from pathlib import Path
from typing import Any
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import cast

import pytest
from requests import Response

from amarcord.modules.json import JSONDict
from amarcord.workflows.job_status import JobStatus
from amarcord.workflows.slurm_rest_job_controller import SlurmHttpWrapper
from amarcord.workflows.slurm_rest_job_controller import SlurmRestJobController


class MockResponse:
    def __init__(
        self, json_data: Optional[JSONDict], text: Optional[str], status_code: int
    ) -> None:
        self.json_data = json_data
        self.status_code = status_code
        self.text = text

    def json(self) -> JSONDict:
        return cast(JSONDict, self.json_data)


class MockHttpWrapper(SlurmHttpWrapper):
    def __init__(self) -> None:
        self.post_requests: List[Tuple[str, Dict[str, Any], str]] = []
        self.get_requests: List[Tuple[str, Dict[str, Any]]] = []
        self.responses: List[Response] = []

    def post(self, url: str, headers: Dict[str, Any], data: str) -> Response:
        self.post_requests.append((url, headers, data))
        return self.responses.pop()

    def get(self, url: str, headers: Dict[str, Any]) -> Response:
        self.get_requests.append((url, headers))
        return self.responses.pop()


def test_slurm_rest_job_controller_start_job() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestJobController(
        output_base_dir=Path("/output"),
        partition="testpartition",
        jwt_token="token",
        user_id=1,
        request_wrapper=http_wrapper,
    )

    http_wrapper.responses.append(MockResponse(status_code=200, json_data={"job_id": 1}, text=None))  # type: ignore
    controller.start_job(Path("test"), Path("/tmp/executable"), "", [])

    assert http_wrapper.post_requests


def test_slurm_rest_job_controller_list_jobs_with_errors() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestJobController(
        output_base_dir=Path("/output"),
        partition="testpartition",
        jwt_token="token",
        user_id=1,
        request_wrapper=http_wrapper,
    )

    http_wrapper.responses.append(MockResponse(status_code=200, json_data={"errors": ["hehe"]}, text=None))  # type: ignore

    with pytest.raises(Exception):
        controller.list_jobs()


def test_slurm_rest_job_controller_list_jobs_other_users_are_ignored() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestJobController(
        output_base_dir=Path("/output"),
        partition="testpartition",
        jwt_token="token",
        user_id=1,
        request_wrapper=http_wrapper,
    )

    http_wrapper.responses.append(MockResponse(status_code=200, json_data={"jobs": [{"user_id": 2}]}, text=None))  # type: ignore
    assert not controller.list_jobs()


def test_slurm_rest_job_controller_list_jobs_success() -> None:
    http_wrapper = MockHttpWrapper()
    controller = SlurmRestJobController(
        output_base_dir=Path("/output"),
        partition="testpartition",
        jwt_token="token",
        user_id=1,
        request_wrapper=http_wrapper,
    )

    http_wrapper.responses.append(
        MockResponse(  # type: ignore
            status_code=200,
            json_data={
                "jobs": [
                    {
                        "user_id": 1,
                        "job_id": 1,
                        "job_state": "RUNNING",
                        "start_time": 1625570627,
                    }
                ]
            },
            text=None,
        )
    )
    jobs = controller.list_jobs()
    assert len(jobs) == 1
    assert jobs[0].status == JobStatus.RUNNING
