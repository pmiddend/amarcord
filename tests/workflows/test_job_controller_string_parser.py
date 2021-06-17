from pathlib import Path

from amarcord.workflows.job_controller_factory import LocalJobControllerConfig
from amarcord.workflows.job_controller_factory import SlurmRestJobControllerConfig
from amarcord.workflows.job_controller_factory import parse_job_controller


def test_parse_job_controller_local(fs) -> None:
    workflow_result = Path("/tmp/workflow-results")
    sqlite_file = Path("/tmp/job-db.db")
    fs.create_dir(workflow_result)
    result = parse_job_controller(f"local://{workflow_result}?sqliteFile={sqlite_file}")
    assert isinstance(result, LocalJobControllerConfig)
    assert result.baseDir == workflow_result
    assert result.sqliteFile == sqlite_file


def test_parse_job_controller_slurm(fs) -> None:
    partition = "testpartition"
    user_id = 1
    jwt_token = "token"
    user = "pmidden"
    base_dir = "basedir"
    result = parse_job_controller(
        f"slurmrest://host:1337/path?baseDir={base_dir}&partition={partition}&userId={user_id}&jwtToken={jwt_token}&user={user}"
    )
    assert isinstance(result, SlurmRestJobControllerConfig)
    assert result.baseDir == Path(base_dir)
    assert result.user == user
    assert result.user_id == user_id
    assert result.partition == partition
    assert result.jwtToken == jwt_token
    assert result.url == "http://host:1337/path"
