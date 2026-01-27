# type: ignore[reportPrivateUsage]
import asyncio
import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import AsyncGenerator

import aiohttp
import pytest
import uvicorn
from aiohttp.client import ClientSession
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.asyncio import create_async_engine

import amarcord.cli.id29_pull_daemon as pull_daemon
import amarcord.cli.id29_push_daemon as push_daemon
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.beamtime_id import BeamtimeId
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.orm_utils import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.orm_utils import migrate
from amarcord.json_schema import JSONSchemaInteger
from amarcord.json_schema import JSONSchemaString
from amarcord.web.fastapi_utils import get_orm_sessionmaker_with_url
from amarcord.web.json_models import JsonAttributiIdAndRole
from amarcord.web.json_models import JsonBeamtimeInput
from amarcord.web.json_models import JsonBeamtimeOutput
from amarcord.web.json_models import JsonChemicalWithoutId
from amarcord.web.json_models import JsonCreateAttributoInput
from amarcord.web.json_models import JsonCreateAttributoOutput
from amarcord.web.json_models import JsonCreateExperimentTypeInput
from amarcord.web.json_models import JsonCreateExperimentTypeOutput
from amarcord.web.json_models import JsonReadIndexingResultsOutput
from amarcord.web.json_models import JsonReadRuns
from amarcord.web.json_models import JsonUserConfigurationSingleOutput

_BEAMTIME_ID = 1


async def init_db(url: str) -> None:
    engine = create_async_engine(url)
    await migrate(engine)


@pytest.fixture
def db_file(tmpdir: Path) -> Path:
    return tmpdir / "test.db"


@pytest.fixture
def db_url(db_file: Path) -> str:
    return f"sqlite+aiosqlite:///{db_file}"


@pytest.fixture
async def server_port(db_url: str) -> AsyncGenerator[int]:
    os.environ["DB_URL"] = db_url
    await init_db(db_url)

    config = uvicorn.Config("amarcord.cli.webserver:app", port=0, log_level="info")
    main_server = uvicorn.Server(config)
    server_task = asyncio.create_task(main_server.serve())

    # Not sure how to use anyio.Event here, so disable for now ('tis but a test)
    while not main_server.started:  # noqa: ASYNC110
        await asyncio.sleep(0.1)

    for server in main_server.servers:
        for socket in server.sockets:
            print(f"socket port {socket.getsockname()[1]}")  # noqa: T201
            yield socket.getsockname()[1]

    print("waiting for server completion")  # noqa: T201
    await main_server.shutdown()
    server_task.cancel()


@pytest.fixture
async def async_session(db_url: str) -> AsyncGenerator[AsyncSession]:
    os.environ["DB_URL"] = db_url
    await init_db(db_url)

    try:
        result = get_orm_sessionmaker_with_url(os.environ["DB_URL"])

        async with result() as session:
            yield session

    except:  # noqa: S110
        pass


# @pytest.fixture
# async def daemon_session(aiohttp_client: Any) -> aiohttp.ClientSession:
#     app = web.Application()
#     app.router.add_get(f"/api/attributi/{_BEAMTIME_ID}", _read_attributi)
#     return await aiohttp_client(app)


@pytest.fixture
async def http_client() -> AsyncGenerator[ClientSession]:
    async with aiohttp.ClientSession(
        timeout=aiohttp.ClientTimeout(5), raise_for_status=True
    ) as session:
        yield session


@dataclass
class ScenarioData:
    args: push_daemon.Arguments
    first_metadata_json_file: Path
    first_stream_file: Path
    geometry_file_contents: str
    beamtime_id: int
    config_file: push_daemon.ID29AttributoConfigFile
    amarcord_url: str


async def setup_test_scenario(
    tmp_path: Path, server_port: int, http_client: ClientSession
) -> ScenarioData:
    amarcord_url = f"http://localhost:{server_port}"

    async with http_client.post(
        f"{amarcord_url}/api/beamtimes",
        json=JsonBeamtimeInput(
            id=BeamtimeId(0),
            external_id="1",
            beamline="",
            proposal="",
            title="",
            comment="",
            start_local=1000,
            end_local=1500,
            analysis_output_path="/",
        ).model_dump(),
    ) as response:
        beamtime_id = JsonBeamtimeOutput(
            **(await response.json()),
        ).id

    _SAMPLE_ATTRIBUTO_NAME = "sample"  # noqa: N806
    _TAG_ATTRIBUTO_NAME = "tag"  # noqa: N806
    _PSEUDO_ATTRIBUTO_NAME = "MX_amarcord_test"  # noqa: N806
    _PSEUDO_BEAMLINE_ATTRIBUTO_NAME = "amarcord_test"  # noqa: N806
    attributo_descriptions = [
        JsonCreateAttributoInput(
            name=_SAMPLE_ATTRIBUTO_NAME,
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            attributo_type_integer=JSONSchemaInteger(
                type="integer", format="chemical-id"
            ),
            beamtime_id=beamtime_id,
        ),
        JsonCreateAttributoInput(
            name=_TAG_ATTRIBUTO_NAME,
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            attributo_type_string=JSONSchemaString(type="string", enum=None),
            beamtime_id=beamtime_id,
        ),
        JsonCreateAttributoInput(
            name=push_daemon.ID29_MAGIC_DIRECTORY_ATTRIBUTO,
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            attributo_type_string=JSONSchemaString(type="string", enum=None),
            beamtime_id=beamtime_id,
        ),
        JsonCreateAttributoInput(
            name=_PSEUDO_ATTRIBUTO_NAME,
            description="",
            group=ATTRIBUTO_GROUP_MANUAL,
            associated_table=AssociatedTable.RUN,
            attributo_type_string=JSONSchemaString(type="string", enum=None),
            beamtime_id=beamtime_id,
        ),
    ]

    attributo_name_to_id: dict[str, int] = {}
    for a in attributo_descriptions:
        async with http_client.post(
            f"{amarcord_url}/api/attributi",
            json=a.model_dump(),
        ) as response:
            response_json = JsonCreateAttributoOutput(
                **(await response.json()),
            )
            assert response_json.id > 0
            attributo_name_to_id[a.name] = response_json.id

    TEST_CHEMICAL_NAME = "test-chem"  # noqa: N806
    async with http_client.post(
        f"{amarcord_url}/api/chemicals",
        json=(
            JsonChemicalWithoutId(
                name=TEST_CHEMICAL_NAME,
                responsible_person="",
                attributi=[],
                chemical_type=ChemicalType.CRYSTAL,
                file_ids=[],
                beamtime_id=beamtime_id,
            )
        ).model_dump(),
    ) as response:
        assert (
            JsonCreateAttributoOutput(
                **(await response.json()),
            ).id
            > 0
        )

    async with http_client.post(
        f"{amarcord_url}/api/experiment-types",
        json=JsonCreateExperimentTypeInput(
            name="experiment type test",
            beamtime_id=beamtime_id,
            attributi=[
                JsonAttributiIdAndRole(
                    id=attributo_name_to_id[_SAMPLE_ATTRIBUTO_NAME],
                    role=ChemicalType.CRYSTAL,
                ),
                JsonAttributiIdAndRole(
                    id=attributo_name_to_id[_TAG_ATTRIBUTO_NAME],
                    role=ChemicalType.CRYSTAL,
                ),
            ],
        ).model_dump(),
    ) as response:
        response_json = JsonCreateExperimentTypeOutput(**await response.json())
        assert response_json.id > 0
        experiment_type_id = response_json.id

    async with http_client.patch(
        f"{amarcord_url}/api/user-config/{beamtime_id}/current-experiment-type-id/{experiment_type_id}"
    ) as response:
        response_json = JsonUserConfigurationSingleOutput(**await response.json())
        assert response_json.value_int == experiment_type_id

    args = push_daemon.Arguments()
    args.amarcord_beamtime_id = _BEAMTIME_ID
    # we don't compare file age since we don't want to wait a minute for the file to be old enough
    args.compare_stream_file_age = False
    base_path = tmp_path / "source"
    args.raw_data_path = base_path / "raw"
    args.raw_data_path.mkdir(parents=True, exist_ok=True)
    args.run_id_file = base_path / "run-id.txt"
    args.metadata_visited_file = base_path / "metadata-visited.txt"
    args.stream_visited_file = base_path / "stream-visited.txt"
    # This doesn't actually matter, we pass the parsed config file to the main loop iteration function
    args.attributo_config_file = Path("/attributo-config.json")
    args.amarcord_url = f"http://localhost:{server_port}"
    args.sample_attributo = _SAMPLE_ATTRIBUTO_NAME
    args.tag_attributo = _TAG_ATTRIBUTO_NAME

    # This is not necessarily where the metadata.json resides
    files_dir = base_path / "actual-files"
    files_dir.mkdir()

    # this is just to cover the case that iterating over the directory doesn't just cover files
    (files_dir / "somedir").mkdir()

    # Write a little test file to make the start/end date calculation algorithm happy
    with (files_dir / "somefile.txt").open("w", encoding="utf-8") as f:
        f.write("test")

    test_metadata_dir = args.raw_data_path / "test-dir"
    test_metadata_dir.mkdir()
    test_metadata_json_file = test_metadata_dir / "metadata.json"
    with test_metadata_json_file.open("w", encoding="utf-8") as f:
        json.dump(
            {
                "Sample_name": TEST_CHEMICAL_NAME,
                push_daemon.ID29_MAGIC_DIRECTORY_ATTRIBUTO: str(files_dir),
                _PSEUDO_BEAMLINE_ATTRIBUTO_NAME: "foo",
            },
            f,
        )

    test_streams_dir = args.raw_data_path / "streams-dir"
    test_streams_dir.mkdir()
    test_stream_file = test_metadata_dir / push_daemon.ID29_MAGIC_STREAM_FILE_NAME
    geometry_file_contents = "mygeom"
    with test_stream_file.open("w", encoding="utf-8") as f:
        f.write(f"""
Generated by CrystFEL 1.0
indexamajig -i foo -o bar.stream --some-option
----- Begin geometry
{geometry_file_contents}
----- End geometry
----- Begin unit
CrystFEL unit cell file version 1.0

lattice_type = orthorhombic
centering = C
a = 61.40 A
b = 122.6 A
c = 168.0 A
al = 90 deg
be = 90 deg
ga = 90 deg
----- End unit
----- Begin chunk
hit = 1
indexed_by = asdf
Image filename: {files_dir}/test.h5
----- End chunk
""")

    config_file_path = base_path / "config.json"
    with config_file_path.open("w", encoding="utf-8") as f:
        json.dump(
            [
                {
                    "beamline-name": _PSEUDO_BEAMLINE_ATTRIBUTO_NAME,
                    "attributo-name": _PSEUDO_ATTRIBUTO_NAME,
                    "attributo-type": push_daemon.ID29AttributoType.ATTRIBUTO_TYPE_STRING.value,
                },
                {
                    "beamline-name": push_daemon.ID29_MAGIC_DIRECTORY_ATTRIBUTO,
                    "attributo-name": push_daemon.ID29_MAGIC_DIRECTORY_ATTRIBUTO,
                    "attributo-type": push_daemon.ID29AttributoType.ATTRIBUTO_TYPE_STRING.value,
                },
            ],
            f,
        )
    config_file = push_daemon.id29_parse_attributo_config_file(config_file_path)

    return ScenarioData(
        args=args,
        first_metadata_json_file=test_metadata_json_file,
        first_stream_file=test_stream_file,
        geometry_file_contents=geometry_file_contents,
        config_file=config_file,
        amarcord_url=amarcord_url,
        beamtime_id=beamtime_id,
    )


async def test_simple_scenario(
    http_client: ClientSession, server_port: int, tmp_path: Path
) -> None:
    test_scenario = await setup_test_scenario(tmp_path, server_port, http_client)

    await push_daemon._main_loop_iteration(  # noqa: SLF001
        test_scenario.args, test_scenario.config_file, http_client
    )
    # For good measure: do it twice, shouldn't duplicate any runs/etc
    await push_daemon._main_loop_iteration(  # noqa: SLF001
        test_scenario.args, test_scenario.config_file, http_client
    )

    # we now assume our metadata.json file was read
    with test_scenario.args.metadata_visited_file.open("r", encoding="utf-8") as f:
        contents = f.read()
        assert str(test_scenario.first_metadata_json_file) in contents

    # and also, we should have a run now
    async with http_client.get(
        f"{test_scenario.amarcord_url}/api/runs/{test_scenario.beamtime_id}"
    ) as response:
        response_json = JsonReadRuns(**await response.json())

        assert len(response_json.runs) == 1

    # and the stream file was consumed
    with test_scenario.args.stream_visited_file.open("r", encoding="utf-8") as f:
        contents = f.read()
        assert str(test_scenario.first_stream_file) in contents

    async with http_client.get(
        f"{test_scenario.amarcord_url}/api/indexing?beamtimeId={test_scenario.beamtime_id}"
    ) as response:
        response_json = JsonReadIndexingResultsOutput(**await response.json())

        assert len(response_json.indexing_jobs) == 1
        assert response_json.indexing_jobs[0].stream_file == str(
            test_scenario.first_stream_file
        )


async def test_push_and_pull(
    tmp_path: Path, server_port: int, http_client: ClientSession, db_url: str
) -> None:
    test_scenario = await setup_test_scenario(tmp_path, server_port, http_client)

    await push_daemon._main_loop_iteration(  # noqa: SLF001
        test_scenario.args, test_scenario.config_file, http_client
    )

    args = pull_daemon.Arguments()
    args.db_connection_url = db_url
    args.amarcord_beamtime_id = test_scenario.beamtime_id
    args.path_prefix = str(test_scenario.first_stream_file.parent)
    target_path = tmp_path / "target"
    streams_path = target_path / "streams"
    streams_path.mkdir(parents=True)
    args.path_prefix_replacement = str(streams_path)
    print(f"will replace {test_scenario.first_stream_file.parent} with {streams_path}")  # noqa: T201
    args.simulate = True
    args.sshpass_path = "/usr/bin/sshpass"
    args.esrf_ssh_host = "esrf-hostname"
    args.esrf_user = "esrfuser"
    args.esrf_password = "esrfpassword"  # noqa: S105
    args.directory_attributo_name = push_daemon.ID29_MAGIC_DIRECTORY_ATTRIBUTO
    args.dont_copy_attributo_name = "dontcopy"
    # False for now, later on write a test for this, too
    args.copy_raw_data = False
    args.rsync_path = "/usr/bin/rsync"

    print("starting pull daemon loop")  # noqa: T201
    await pull_daemon._async_main(args)  # noqa: SLF001
    assert len(list(streams_path.glob("*.stream"))) == 1

    async with http_client.get(
        f"{test_scenario.amarcord_url}/api/indexing?beamtimeId={test_scenario.beamtime_id}"
    ) as response:
        response_json = JsonReadIndexingResultsOutput(**await response.json())

        assert len(response_json.indexing_jobs) == 1
        assert response_json.indexing_jobs[0].stream_file == str(
            next(iter(streams_path.glob("*.stream")))
        )
