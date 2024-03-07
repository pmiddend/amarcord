# working with these fixtures, this pylint error message doesn't make sense anymore
# pylint: disable=redefined-outer-name
import asyncio
import os
from io import BytesIO
from pathlib import Path
from typing import Generator
from unittest import TestCase
from zipfile import ZipFile

import pytest
from fastapi.testclient import TestClient
from openpyxl import load_workbook

from amarcord.amici.crystfel.merge_daemon import JsonMergeResultRootJson
from amarcord.cli.webserver import JsonAttributiIdAndRole
from amarcord.cli.webserver import JsonAttributo
from amarcord.cli.webserver import JsonAttributoValue
from amarcord.cli.webserver import JsonBeamtime
from amarcord.cli.webserver import JsonBeamtimeOutput
from amarcord.cli.webserver import JsonBeamtimeSchedule
from amarcord.cli.webserver import JsonBeamtimeScheduleOutput
from amarcord.cli.webserver import JsonBeamtimeScheduleRow
from amarcord.cli.webserver import JsonChangeRunExperimentType
from amarcord.cli.webserver import JsonChangeRunExperimentTypeOutput
from amarcord.cli.webserver import JsonCheckStandardUnitInput
from amarcord.cli.webserver import JsonCheckStandardUnitOutput
from amarcord.cli.webserver import JsonChemicalWithId
from amarcord.cli.webserver import JsonChemicalWithoutId
from amarcord.cli.webserver import JsonCreateAttributoInput
from amarcord.cli.webserver import JsonCreateAttributoOutput
from amarcord.cli.webserver import JsonCreateChemicalOutput
from amarcord.cli.webserver import JsonCreateDataSetFromRun
from amarcord.cli.webserver import JsonCreateDataSetFromRunOutput
from amarcord.cli.webserver import JsonCreateDataSetInput
from amarcord.cli.webserver import JsonCreateDataSetOutput
from amarcord.cli.webserver import JsonCreateExperimentTypeInput
from amarcord.cli.webserver import JsonCreateExperimentTypeOutput
from amarcord.cli.webserver import JsonCreateFileOutput
from amarcord.cli.webserver import JsonCreateLiveStreamSnapshotOutput
from amarcord.cli.webserver import JsonCreateOrUpdateRun
from amarcord.cli.webserver import JsonCreateOrUpdateRunOutput
from amarcord.cli.webserver import JsonDeleteAttributoInput
from amarcord.cli.webserver import JsonDeleteAttributoOutput
from amarcord.cli.webserver import JsonDeleteChemicalInput
from amarcord.cli.webserver import JsonDeleteDataSetInput
from amarcord.cli.webserver import JsonDeleteDataSetOutput
from amarcord.cli.webserver import JsonDeleteEventInput
from amarcord.cli.webserver import JsonDeleteExperimentType
from amarcord.cli.webserver import JsonDeleteExperimentTypeOutput
from amarcord.cli.webserver import JsonDeleteFileInput
from amarcord.cli.webserver import JsonDeleteFileOutput
from amarcord.cli.webserver import JsonEventInput
from amarcord.cli.webserver import JsonEventTopLevelInput
from amarcord.cli.webserver import JsonEventTopLevelOutput
from amarcord.cli.webserver import JsonIndexingJobUpdateOutput
from amarcord.cli.webserver import JsonIndexingResult
from amarcord.cli.webserver import JsonIndexingResultRootJson
from amarcord.cli.webserver import JsonMergeJobUpdateOutput
from amarcord.cli.webserver import JsonMergeParameters
from amarcord.cli.webserver import JsonPolarisation
from amarcord.cli.webserver import JsonReadAnalysisResults
from amarcord.cli.webserver import JsonReadAttributi
from amarcord.cli.webserver import JsonReadBeamtime
from amarcord.cli.webserver import JsonReadChemicals
from amarcord.cli.webserver import JsonReadDataSets
from amarcord.cli.webserver import JsonReadExperimentTypes
from amarcord.cli.webserver import JsonReadRuns
from amarcord.cli.webserver import JsonReadRunsBulkInput
from amarcord.cli.webserver import JsonReadRunsBulkOutput
from amarcord.cli.webserver import JsonRefinementResult
from amarcord.cli.webserver import JsonStartMergeJobForDataSetInput
from amarcord.cli.webserver import JsonStartMergeJobForDataSetOutput
from amarcord.cli.webserver import JsonStartRunOutput
from amarcord.cli.webserver import JsonUpdateAttributoConversionFlags
from amarcord.cli.webserver import JsonUpdateAttributoInput
from amarcord.cli.webserver import JsonUpdateAttributoOutput
from amarcord.cli.webserver import JsonUpdateBeamtimeInput
from amarcord.cli.webserver import JsonUpdateBeamtimeScheduleInput
from amarcord.cli.webserver import JsonUpdateLiveStream
from amarcord.cli.webserver import JsonUpdateRun
from amarcord.cli.webserver import JsonUpdateRunOutput
from amarcord.cli.webserver import JsonUpdateRunsBulkInput
from amarcord.cli.webserver import JsonUpdateRunsBulkOutput
from amarcord.cli.webserver import JsonUserConfigurationSingleOutput
from amarcord.cli.webserver import app
from amarcord.db.associated_table import AssociatedTable
from amarcord.db.async_dbcontext import AsyncDBContext
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.asyncdb import AsyncDB
from amarcord.db.chemical_type import ChemicalType
from amarcord.db.merge_model import MergeModel
from amarcord.db.merge_negative_handling import MergeNegativeHandling
from amarcord.db.merge_result import MergeResult
from amarcord.db.merge_result import MergeResultFom
from amarcord.db.merge_result import MergeResultOuterShell
from amarcord.db.merge_result import MergeResultShell
from amarcord.db.merge_result import RefinementResult
from amarcord.db.scale_intensities import ScaleIntensities
from amarcord.db.tables import create_tables_from_metadata
from amarcord.json_schema import JSONSchemaInteger
from amarcord.json_schema import JSONSchemaString

IN_MEMORY_DB_URL = "sqlite+aiosqlite://"

TEST_ATTRIBUTO_NAME = "testattributo"
TEST_ATTRIBUTO_NAME2 = "testattributo2"
TEST_CHEMICAL_NAME = "chemicalname"
TEST_CHEMICAL_RESPONSIBLE_PERSON = "Rosalind Franklin"


async def init_db(url: str) -> None:
    context = AsyncDBContext(url)
    await AsyncDB(context, create_tables_from_metadata(context.metadata)).migrate()


@pytest.fixture
def client(tmp_path: Path) -> Generator[TestClient, None, None]:
    url = f"{IN_MEMORY_DB_URL}/{tmp_path}/db"
    os.environ["DB_URL"] = url
    asyncio.run(init_db(url))
    yield TestClient(app)


def create_beamtime(client: TestClient, input_: JsonUpdateBeamtimeInput) -> int:
    response = JsonBeamtimeOutput(
        **client.post(
            "/api/beamtimes",
            json=input_.dict(),
        ).json()
    )
    assert response.id > 0
    return response.id


@pytest.fixture
def beamtime_id(client: TestClient) -> int:
    return create_beamtime(
        client,
        JsonUpdateBeamtimeInput(
            id=0,
            external_id="cool 1337",
            beamline="P12",
            proposal="BAG",
            title="Test beamtime",
            comment="comment",
            start=1,
            end=1000,
        ),
    )


@pytest.fixture
def second_beamtime_id(client: TestClient) -> int:
    return create_beamtime(
        client,
        JsonUpdateBeamtimeInput(
            id=0,
            external_id="second external",
            beamline="P13",
            proposal="BAG2",
            title="Test beamtime2",
            comment="comment2",
            start=1,
            end=1000,
        ),
    )


@pytest.fixture
def test_file_path() -> Path:
    # This is just some lame txt file, but it's enough for our purposes of testing. We don't need _actual_ files.
    return Path(__file__).parent / "test-file.txt"


@pytest.fixture
def test_file(client: TestClient, test_file_path: Path) -> int:
    # Upload the file
    with test_file_path.open("rb") as upload_file:
        raw_output = client.post(
            "/api/files",
            data={"description": "test description"},
            files={"file": upload_file},
        )
        output = JsonCreateFileOutput(**raw_output.json())

        assert output.file_name == "test-file.txt"
        assert output.description == "test description"
        assert output.type_ == "text/plain"
        assert output.size_in_bytes == 17
        return output.id


@pytest.fixture
def second_test_file(client: TestClient, test_file_path: Path) -> int:
    # Upload the file
    with test_file_path.open("rb") as upload_file:
        raw_output = client.post(
            "/api/files",
            data={"description": "test 2 description"},
            files={"file": upload_file},
        )
        output = JsonCreateFileOutput(**raw_output.json())
        return output.id


@pytest.fixture
def cell_description_attributo_id(client: TestClient, beamtime_id: int) -> int:
    input_ = JsonCreateAttributoInput(
        name="cell description",
        description="description",
        group=ATTRIBUTO_GROUP_MANUAL,
        associated_table=AssociatedTable.CHEMICAL,
        attributo_type_string=JSONSchemaString(type="string", enum=None),
        beamtime_id=beamtime_id,
    ).dict()
    response = JsonCreateAttributoOutput(
        **client.post(
            "/api/attributi",
            json=input_,
        ).json()
    )
    assert response.id > 0
    return response.id


@pytest.fixture
def point_group_attributo_id(client: TestClient, beamtime_id: int) -> int:
    input_ = JsonCreateAttributoInput(
        name="point group",
        description="description",
        group=ATTRIBUTO_GROUP_MANUAL,
        associated_table=AssociatedTable.CHEMICAL,
        attributo_type_string=JSONSchemaString(type="string", enum=None),
        beamtime_id=beamtime_id,
    ).dict()
    response_json = client.post(
        "/api/attributi",
        json=input_,
    ).json()
    response = JsonCreateAttributoOutput(**response_json)
    return response.id


@pytest.fixture
def run_string_attributo_id(client: TestClient, beamtime_id: int) -> int:
    input_ = JsonCreateAttributoInput(
        name="run_string",
        description="",
        group=ATTRIBUTO_GROUP_MANUAL,
        associated_table=AssociatedTable.RUN,
        attributo_type_string=JSONSchemaString(type="string", enum=None),
        beamtime_id=beamtime_id,
    ).dict()
    response = JsonCreateAttributoOutput(
        **client.post(
            "/api/attributi",
            json=input_,
        ).json()
    )
    assert response.id > 0
    return response.id


@pytest.fixture
def run_int_attributo_id(client: TestClient, beamtime_id: int) -> int:
    input_ = JsonCreateAttributoInput(
        name="run_int",
        description="",
        group=ATTRIBUTO_GROUP_MANUAL,
        associated_table=AssociatedTable.RUN,
        attributo_type_integer=JSONSchemaInteger(type="integer", format=None),
        beamtime_id=beamtime_id,
    ).dict()
    response = JsonCreateAttributoOutput(
        **client.post(
            "/api/attributi",
            json=input_,
        ).json()
    )
    assert response.id > 0
    return response.id


@pytest.fixture
def run_channel_1_chemical_attributo_id(client: TestClient, beamtime_id: int) -> int:
    input_ = JsonCreateAttributoInput(
        name="channel_1_chemical_id",
        description="",
        group=ATTRIBUTO_GROUP_MANUAL,
        associated_table=AssociatedTable.RUN,
        attributo_type_integer=JSONSchemaInteger(type="integer", format="chemical-id"),
        beamtime_id=beamtime_id,
    ).dict()
    response = JsonCreateAttributoOutput(
        **client.post(
            "/api/attributi",
            json=input_,
        ).json()
    )
    assert response.id > 0
    return response.id


@pytest.fixture
def chemical_experiment_type_id(
    client: TestClient, beamtime_id: int, run_channel_1_chemical_attributo_id: int
) -> int:
    input_ = JsonCreateExperimentTypeInput(
        name="experiment type test",
        beamtime_id=beamtime_id,
        attributi=[
            JsonAttributiIdAndRole(
                id=run_channel_1_chemical_attributo_id, role=ChemicalType.CRYSTAL
            )
        ],
    ).dict()
    response = JsonCreateExperimentTypeOutput(
        **client.post(
            "/api/experiment-types",
            json=input_,
        ).json()
    )
    assert response.id > 0
    return response.id


@pytest.fixture
def string_experiment_type_id(
    client: TestClient, beamtime_id: int, run_string_attributo_id: int
) -> int:
    input_ = JsonCreateExperimentTypeInput(
        name="experiment type test",
        beamtime_id=beamtime_id,
        attributi=[
            JsonAttributiIdAndRole(
                id=run_string_attributo_id, role=ChemicalType.CRYSTAL
            )
        ],
    ).dict()
    response = JsonCreateExperimentTypeOutput(
        **client.post(
            "/api/experiment-types",
            json=input_,
        ).json()
    )
    assert response.id > 0
    return response.id


# This is derliberately "90.0" instead of "90" because one of the processing steps normalizes it to 90.0, and
# if we have it as 90.0 anyways comparison is easier.
LYSO_CELL_DESCRIPTION = "tetragonal P c (79.2 79.2 38.0) (90.0 90.0 90.0)"
LYSO_POINT_GROUP = "4/mmm"


@pytest.fixture
def lyso_chemical_id(
    client: TestClient,
    beamtime_id: int,
    cell_description_attributo_id: int,
    point_group_attributo_id: int,
    test_file: int,
) -> int:
    response = JsonCreateChemicalOutput(
        **client.post(
            "/api/chemicals",
            json=JsonChemicalWithoutId(
                name=TEST_CHEMICAL_NAME,
                responsible_person=TEST_CHEMICAL_RESPONSIBLE_PERSON,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=cell_description_attributo_id,
                        # Good old Lyso!
                        attributo_value_str=LYSO_CELL_DESCRIPTION,
                    ),
                    JsonAttributoValue(
                        attributo_id=point_group_attributo_id,
                        # Good old Lyso!
                        attributo_value_str=LYSO_POINT_GROUP,
                    ),
                ],
                chemical_type=ChemicalType.CRYSTAL,
                file_ids=[test_file],
                beamtime_id=beamtime_id,
            ).dict(),
        ).json()
    )
    assert response.id > 0
    return response.id


@pytest.fixture
def second_lyso_chemical_id(
    client: TestClient,
    beamtime_id: int,
    cell_description_attributo_id: int,
    point_group_attributo_id: int,
) -> int:
    response = JsonCreateChemicalOutput(
        **client.post(
            "/api/chemicals",
            json=JsonChemicalWithoutId(
                name=TEST_CHEMICAL_NAME + "2",
                responsible_person=TEST_CHEMICAL_RESPONSIBLE_PERSON + "2",
                attributi=[
                    JsonAttributoValue(
                        attributo_id=cell_description_attributo_id,
                        attributo_value_str="foo3",
                    ),
                    JsonAttributoValue(
                        attributo_id=point_group_attributo_id,
                        attributo_value_str="bar3",
                    ),
                ],
                chemical_type=ChemicalType.CRYSTAL,
                file_ids=[],
                beamtime_id=beamtime_id,
            ).dict(),
        ).json()
    )
    assert response.id > 0
    return response.id


def test_read_single_beamtime(client: TestClient, beamtime_id: int) -> None:
    beamtime = JsonBeamtime(**client.get(f"/api/beamtimes/{beamtime_id}").json())

    assert beamtime == JsonBeamtime(
        id=beamtime_id,
        external_id="cool 1337",
        beamline="P12",
        proposal="BAG",
        title="Test beamtime",
        comment="comment",
        start=1,
        end=1000,
        chemical_names=[],
    )


def test_read_single_beamtime_chemical_names(
    client: TestClient,
    beamtime_id: int,
    # pylint: disable=unused-argument
    lyso_chemical_id: int,
) -> None:
    beamtimes = JsonReadBeamtime(**client.get("/api/beamtimes").json()).beamtimes

    assert beamtimes[0] == JsonBeamtime(
        id=beamtime_id,
        external_id="cool 1337",
        beamline="P12",
        proposal="BAG",
        title="Test beamtime",
        comment="comment",
        start=1,
        end=1000,
        chemical_names=[TEST_CHEMICAL_NAME],
    )


def test_edit_random_beamtime(client: TestClient, beamtime_id: int) -> None:
    response = JsonBeamtimeOutput(
        **client.patch(
            "/api/beamtimes",
            json=JsonUpdateBeamtimeInput(
                id=beamtime_id,
                external_id="cool 13372",
                beamline="P122",
                proposal="BAG2",
                title="Test beamtime2",
                comment="comment2",
                start=2,
                end=1002,
            ).dict(),
        ).json()
    )
    assert response.id == beamtime_id
    beamtimes = JsonReadBeamtime(**client.get("/api/beamtimes").json()).beamtimes
    assert len(beamtimes) == 1

    assert beamtimes[0] == JsonBeamtime(
        id=beamtime_id,
        external_id="cool 13372",
        beamline="P122",
        proposal="BAG2",
        title="Test beamtime2",
        comment="comment2",
        start=2,
        end=1002,
        chemical_names=[],
    )


def test_random_beamtime_creation_works(client: TestClient, beamtime_id: int) -> None:
    response = JsonReadBeamtime(**client.get("/api/beamtimes").json())
    assert len(response.beamtimes) == 1
    first_beamtime = response.beamtimes[0]
    assert first_beamtime == JsonBeamtime(
        id=beamtime_id,
        external_id="cool 1337",
        beamline="P12",
        proposal="BAG",
        title="Test beamtime",
        comment="comment",
        start=1,
        end=1000,
        chemical_names=[],
    )


def test_chemical_string_attributo_creation_works(
    client: TestClient, cell_description_attributo_id: int, beamtime_id: int
) -> None:
    response = JsonReadAttributi(**client.get(f"/api/attributi/{beamtime_id}").json())
    assert len(response.attributi) == 1
    first_attributo = response.attributi[0]
    assert first_attributo == JsonAttributo(
        id=cell_description_attributo_id,
        associated_table=AssociatedTable.CHEMICAL,
        description="description",
        group="manual",
        name="cell description",
        attributo_type_string=JSONSchemaString(type="string", enum=None),
    )


def test_chemical_string_attributo_creation_works_in_presence_of_second_beamtime(
    client: TestClient,
    cell_description_attributo_id: int,
    beamtime_id: int,
    second_beamtime_id: int,
) -> None:
    # We have a second beam time simply to demonstrate that creating an attributo in one beam time and then requesting
    # attributi from a different beam time works.
    response = JsonReadAttributi(**client.get(f"/api/attributi/{beamtime_id}").json())
    assert len(response.attributi) == 1
    first_attributo = response.attributi[0]
    assert first_attributo == JsonAttributo(
        id=cell_description_attributo_id,
        associated_table=AssociatedTable.CHEMICAL,
        description="description",
        group="manual",
        name="cell description",
        attributo_type_string=JSONSchemaString(type="string", enum=None),
    )

    # Second beamtime shouldn't have attributi
    assert not JsonReadAttributi(
        **client.get(f"/api/attributi/{second_beamtime_id}").json()
    ).attributi


def _mock_string_attributo_value(aid: int, s: str) -> JsonAttributoValue:
    return JsonAttributoValue(
        attributo_id=aid,
        attributo_value_str=s,
        attributo_value_bool=None,
        attributo_value_float=None,
        attributo_value_int=None,
        attributo_value_list_bool=None,
        attributo_value_list_float=None,
        attributo_value_list_str=None,
    )


def read_chemicals(client: TestClient, beamtime_id: int) -> JsonReadChemicals:
    return JsonReadChemicals(**client.get(f"/api/chemicals/{beamtime_id}").json())


def test_chemical_creation(
    client: TestClient,
    lyso_chemical_id: int,
    cell_description_attributo_id: int,
    point_group_attributo_id: int,
    beamtime_id: int,
    test_file: int,
) -> None:
    """
    Test if the creation of the 'lyso_chemical_id' actually worked, by checking the return value of the list chemicals function
    """
    response = read_chemicals(client, beamtime_id)
    assert len(response.chemicals) == 1
    first_chemical = response.chemicals[0]
    assert first_chemical.id == lyso_chemical_id
    assert first_chemical.attributi == [
        _mock_string_attributo_value(
            cell_description_attributo_id, LYSO_CELL_DESCRIPTION
        ),
        _mock_string_attributo_value(point_group_attributo_id, LYSO_POINT_GROUP),
    ]
    assert first_chemical.beamtime_id == beamtime_id
    assert first_chemical.chemical_type == ChemicalType.CRYSTAL
    assert first_chemical.name == "chemicalname"
    assert first_chemical.responsible_person == "Rosalind Franklin"
    assert len(first_chemical.files) == 1
    assert first_chemical.files[0].id == test_file


def test_chemical_creation_and_deletion(
    client: TestClient,
    beamtime_id: int,
    lyso_chemical_id: int,
    second_lyso_chemical_id: int,
) -> None:
    # can't use "client.delete" since that doesn't get JSON as an input
    client.request(
        "DELETE",
        "/api/chemicals",
        json=JsonDeleteChemicalInput(id=lyso_chemical_id).dict(),
    )

    response = read_chemicals(client, beamtime_id)

    assert len(response.chemicals) == 1
    assert response.chemicals[0].id == second_lyso_chemical_id


def test_chemical_creation_and_update(
    client: TestClient,
    lyso_chemical_id: int,
    cell_description_attributo_id: int,
    point_group_attributo_id: int,
    beamtime_id: int,
    second_test_file: int,
) -> None:
    """
    Take the 'lyso_chemical_id' and update it, then check if the list contains just one element and it's the up-to-date version
    """
    patched_chemical_json = JsonChemicalWithId(
        id=lyso_chemical_id,
        attributi=[
            # previously: foo
            _mock_string_attributo_value(cell_description_attributo_id, "foo2"),
            # previously: bar
            _mock_string_attributo_value(point_group_attributo_id, "bar2"),
        ],
        beamtime_id=beamtime_id,
        chemical_type=ChemicalType.CRYSTAL,
        # The first chemical has "test_file" as files attached to it. Here, we swap out the files, and see if that works.
        file_ids=[second_test_file],
        # new name
        name="chemicalname2",
        # new responsible person
        responsible_person="Rosalind Franklin2",
    )

    single_response = JsonCreateChemicalOutput(
        **client.patch("/api/chemicals", json=patched_chemical_json.dict()).json()
    )
    assert single_response.id == lyso_chemical_id

    response = read_chemicals(client, beamtime_id)

    # still only one chemical => the update didn't accidentally create another chemical
    assert len(response.chemicals) == 1
    assert response.chemicals[0].name == "chemicalname2"
    assert response.chemicals[0].files[0].id == second_test_file


def test_create_and_delete_event_without_live_stream_and_files(
    client: TestClient,
    beamtime_id: int,
    second_beamtime_id: int,
) -> None:
    create_event_response = JsonEventTopLevelOutput(
        **client.post(
            "/api/events",
            json=JsonEventTopLevelInput(
                beamtime_id=beamtime_id,
                event=JsonEventInput(
                    # The "level" has to be "user" here because we're testing read_runs below, which only includes user messages.
                    source="mysource",
                    text="mytext",
                    level="user",
                    fileIds=[],
                ),
                with_live_stream=False,
            ).dict(),
        ).json()
    )
    assert create_event_response.id > 0

    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())
    assert len(read_runs_output.events) == 1
    e = read_runs_output.events[0]
    assert e.id == create_event_response.id
    assert e.source == "mysource"
    assert e.text == "mytext"

    # Second beamtime should be unaffected
    assert not JsonReadRuns(
        **client.get(f"api/runs/{second_beamtime_id}").json()
    ).events

    # can't use "client.delete" since that doesn't get JSON as an input
    client.request(
        "DELETE",
        "/api/events",
        json=JsonDeleteEventInput(id=create_event_response.id).dict(),
    )

    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())
    assert not read_runs_output.events


def test_upload_and_retrieve_file(client: TestClient) -> None:
    test_file = Path(__file__).parent / "test-file.txt"

    # Upload the file
    with test_file.open("rb") as upload_file:
        raw_output = client.post(
            "/api/files",
            data={"description": "test description"},
            files={"file": upload_file},
        )
        output = JsonCreateFileOutput(**raw_output.json())

        assert output.file_name == "test-file.txt"
        assert output.description == "test description"
        assert output.type_ == "text/plain"
        assert output.size_in_bytes == 17

    # Retrieve the contents (not the metadata)
    with test_file.open("rb") as upload_file:
        assert client.get(f"/api/files/{output.id}").content == upload_file.read()


def test_create_event_with_file(
    client: TestClient, beamtime_id: int, test_file: int
) -> None:
    JsonEventTopLevelOutput(
        **client.post(
            "/api/events",
            json=JsonEventTopLevelInput(
                beamtime_id=beamtime_id,
                event=JsonEventInput(
                    source="mysource", text="mytext", level="user", fileIds=[test_file]
                ),
                with_live_stream=False,
            ).dict(),
        ).json()
    )

    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())
    assert len(read_runs_output.events) == 1
    assert len(read_runs_output.events[0].files) == 1
    assert read_runs_output.events[0].files[0].id == test_file


def test_create_event_with_live_stream(
    client: TestClient, beamtime_id: int, test_file_path: Path
) -> None:
    # Upload the file: for the live stream, it's important to use the proper file name, as that's the criterion for a file to be
    # a live stream (yes, ugly, I know).
    with test_file_path.open("rb") as upload_file:
        raw_output = client.post(
            "/api/files",
            data={"description": "test description"},
            # To pass a file name as well as the file contents, httpx uses a tuple.
            files={"file": (f"live-stream-image-{beamtime_id}", upload_file)},
        )
        file_id = JsonCreateFileOutput(**raw_output.json()).id

    JsonEventTopLevelOutput(
        **client.post(
            "/api/events",
            json=JsonEventTopLevelInput(
                beamtime_id=beamtime_id,
                event=JsonEventInput(
                    source="mysource", level="user", text="mytext", fileIds=[]
                ),
                with_live_stream=True,
            ).dict(),
        ).json()
    )

    # Read events -- assume we "magically" got a file attached to the event - a copy of our live stream image
    # (the actual live stream image doesn't work, because it gets updated in-place)
    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())
    assert len(read_runs_output.events) == 1
    assert len(read_runs_output.events[0].files) == 1
    assert read_runs_output.events[0].files[0].id > file_id


def test_create_experiment_type(
    client: TestClient, beamtime_id: int, chemical_experiment_type_id: int
) -> None:
    read_ets_output = JsonReadExperimentTypes(
        **client.get(f"/api/experiment-types/{beamtime_id}").json()
    )
    assert len(read_ets_output.experiment_types) == 1
    ets = read_ets_output.experiment_types
    assert ets[0].id == chemical_experiment_type_id


def test_create_or_update_run_fails_without_experiment_type(
    client: TestClient,
    beamtime_id: int,
    # we want the experiment type to be created, but not used here
    # pylint: disable=unused-argument
    chemical_experiment_type_id: int,
    run_string_attributo_id: int,
) -> None:
    # Let's make the external run ID deliberately high
    external_run_id = 1000

    response = client.post(
        f"/api/runs/{external_run_id}",
        json=JsonCreateOrUpdateRun(
            beamtime_id=beamtime_id,
            attributi=[
                JsonAttributoValue(
                    attributo_id=run_string_attributo_id,
                    attributo_value_str="foo",
                )
            ],
            started=1,
            stopped=None,
        ).dict(),
    )

    # Doesn't work, because we haven't set the current experiment type yet
    assert response.status_code == 400


def set_current_experiment_type(client: TestClient, beamtime_id: int, id_: int) -> None:
    option_set_result = JsonUserConfigurationSingleOutput(
        **client.patch(
            f"/api/user-config/{beamtime_id}/current-experiment-type-id/{id_}"
        ).json()
    )

    assert option_set_result.value_int == id_


def enable_crystfel_online(client: TestClient, beamtime_id: int) -> None:
    option_set_result = JsonUserConfigurationSingleOutput(
        **client.patch(f"/api/user-config/{beamtime_id}/online-crystfel/True").json()
    )

    assert option_set_result.value_bool


def test_create_and_update_run_after_setting_experiment_type_no_crystfel_online(
    client: TestClient,
    beamtime_id: int,
    chemical_experiment_type_id: int,
    run_string_attributo_id: int,
) -> None:
    # Set the experiment type (otherwise creating a run will fail - see above)
    set_current_experiment_type(client, beamtime_id, chemical_experiment_type_id)

    # Let's make the external run ID deliberately high
    external_run_id = 1000

    run_string_attributo_value = "foo"
    # Create the run and check the result
    response = JsonCreateOrUpdateRunOutput(
        **client.post(
            f"/api/runs/{external_run_id}",
            json=JsonCreateOrUpdateRun(
                beamtime_id=beamtime_id,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=run_string_attributo_id,
                        attributo_value_str=run_string_attributo_value,
                    )
                ],
                started=1,
                stopped=None,
            ).dict(),
        ).json()
    )

    assert response.indexing_result_id is None
    assert response.run_created
    assert response.run_internal_id is not None and response.run_internal_id > 0
    assert not response.error_message

    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())

    assert len(read_runs_output.runs) == 1
    run = read_runs_output.runs[0]
    assert run.external_id == external_run_id
    assert run.stopped is None
    assert run.started == 1
    assert run.experiment_type_id == chemical_experiment_type_id
    assert not run.data_sets
    assert not run.running_indexing_jobs
    assert len(run.attributi) == 1
    attributo = run.attributi[0]
    assert attributo.attributo_id == run_string_attributo_id
    assert attributo.attributo_value_str == run_string_attributo_value

    # Now, update the run
    response = JsonCreateOrUpdateRunOutput(
        **client.post(
            f"/api/runs/{external_run_id}",
            json=JsonCreateOrUpdateRun(
                beamtime_id=beamtime_id,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=run_string_attributo_id,
                        # Update it with a simple "2" at the end!
                        attributo_value_str=run_string_attributo_value + "2",
                    )
                ],
                started=1,
                # and signal a stop
                stopped=2,
            ).dict(),
        ).json()
    )

    assert response.indexing_result_id is None
    assert not response.run_created
    assert not response.error_message
    assert (
        response.run_internal_id is not None
        and response.run_internal_id > 0
        and response.run_internal_id == read_runs_output.runs[0].id
    )

    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())

    assert len(read_runs_output.runs) == 1
    run = read_runs_output.runs[0]
    assert run.external_id == external_run_id
    assert run.stopped == 2
    assert run.started == 1
    assert run.experiment_type_id == chemical_experiment_type_id
    assert not run.data_sets
    assert not run.running_indexing_jobs
    assert len(run.attributi) == 1
    attributo = run.attributi[0]
    assert attributo.attributo_id == run_string_attributo_id
    # repeat our updated "2" again
    assert attributo.attributo_value_str == run_string_attributo_value + "2"


def test_create_and_update_run_after_setting_experiment_type_crystfel_online(
    client: TestClient,
    beamtime_id: int,
    chemical_experiment_type_id: int,
    run_channel_1_chemical_attributo_id: int,
    lyso_chemical_id: int,
) -> None:
    # Set the experiment type (otherwise creating a run will fail - see above)
    set_current_experiment_type(client, beamtime_id, chemical_experiment_type_id)
    enable_crystfel_online(client, beamtime_id)

    # Let's make the external run ID deliberately high
    external_run_id = 1000

    # Create the run and check the result
    response = JsonCreateOrUpdateRunOutput(
        **client.post(
            f"/api/runs/{external_run_id}",
            json=JsonCreateOrUpdateRun(
                beamtime_id=beamtime_id,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=run_channel_1_chemical_attributo_id,
                        attributo_value_int=lyso_chemical_id,
                    )
                ],
                started=1,
                stopped=None,
            ).dict(),
        ).json()
    )

    assert response.run_created
    assert response.run_internal_id is not None and response.run_internal_id > 0
    assert not response.error_message
    assert response.indexing_result_id is not None and response.indexing_result_id > 0


def test_create_and_update_run_with_patch(
    client: TestClient,
    beamtime_id: int,
    chemical_experiment_type_id: int,
    run_channel_1_chemical_attributo_id: int,
    run_string_attributo_id: int,
    lyso_chemical_id: int,
) -> None:
    set_current_experiment_type(client, beamtime_id, chemical_experiment_type_id)

    # Let's make the external run ID deliberately high
    external_run_id = 1000

    update_run_raw_output = client.post(
        f"/api/runs/{external_run_id}",
        json=JsonCreateOrUpdateRun(
            beamtime_id=beamtime_id,
            attributi=[
                # we don't even mention the second run attributo here, since we're going to add it later and test if that works
                JsonAttributoValue(
                    attributo_id=run_channel_1_chemical_attributo_id,
                    attributo_value_int=lyso_chemical_id,
                )
            ],
            started=1,
            stopped=None,
        ).dict(),
    ).json()

    # Create the run and check the result
    create_response = JsonCreateOrUpdateRunOutput(**update_run_raw_output)
    assert create_response.run_internal_id is not None

    update_response = JsonUpdateRunOutput(
        **client.patch(
            "/api/runs",
            json=JsonUpdateRun(
                id=create_response.run_internal_id,
                beamtime_id=beamtime_id,
                experiment_type_id=chemical_experiment_type_id,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=run_string_attributo_id,
                        attributo_value_str="some string",
                    ),
                ],
            ).dict(),
        ).json()
    )

    assert update_response.result

    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())

    # Updating a run, you can specify attributi to update. However,
    # the attributes you specify are only the attributes that are
    # updated. All other attributes should stay the same.
    assert len(read_runs_output.runs[0].attributi) == 2
    cell_description_attributo = next(
        iter(
            a
            for a in read_runs_output.runs[0].attributi
            if a.attributo_id == run_string_attributo_id
        ),
        None,
    )
    assert cell_description_attributo is not None
    assert cell_description_attributo.attributo_value_str == "some string"


def test_create_and_stop_run(
    client: TestClient,
    beamtime_id: int,
    chemical_experiment_type_id: int,
    run_channel_1_chemical_attributo_id: int,
    lyso_chemical_id: int,
) -> None:
    set_current_experiment_type(client, beamtime_id, chemical_experiment_type_id)

    # Let's make the external run ID deliberately high
    external_run_id = 1000

    # Create the run and check the result
    create_response = JsonCreateOrUpdateRunOutput(
        **client.post(
            f"/api/runs/{external_run_id}",
            json=JsonCreateOrUpdateRun(
                beamtime_id=beamtime_id,
                attributi=[
                    # we don't even mention the second run attributo here, since we're going to add it later and test if that works
                    JsonAttributoValue(
                        attributo_id=run_channel_1_chemical_attributo_id,
                        attributo_value_int=lyso_chemical_id,
                    )
                ],
                started=1,
                stopped=None,
            ).dict(),
        ).json()
    )

    assert create_response.run_created

    client.get(f"/api/runs/stop-latest/{beamtime_id}")

    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())

    assert read_runs_output.runs[0].stopped is not None


def test_update_indexing_job(
    client: TestClient,
    beamtime_id: int,
    chemical_experiment_type_id: int,
    run_channel_1_chemical_attributo_id: int,
    lyso_chemical_id: int,
) -> None:
    # Set the experiment type (otherwise creating a run will fail - see above)
    set_current_experiment_type(client, beamtime_id, chemical_experiment_type_id)
    # Enable CrystFEL online so an indexing job will be created
    enable_crystfel_online(client, beamtime_id)

    # Let's make the external run ID deliberately high
    external_run_id = 1000

    # Create the run and check the result
    create_run_response = JsonCreateOrUpdateRunOutput(
        **client.post(
            f"/api/runs/{external_run_id}",
            json=JsonCreateOrUpdateRun(
                beamtime_id=beamtime_id,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=run_channel_1_chemical_attributo_id,
                        attributo_value_int=lyso_chemical_id,
                    )
                ],
                started=1,
                stopped=None,
            ).dict(),
        ).json()
    )

    assert create_run_response.run_internal_id is not None
    assert (
        create_run_response.indexing_result_id is not None
        and create_run_response.indexing_result_id > 0
    )

    update_indexing_job_response = JsonIndexingJobUpdateOutput(
        **client.post(
            f"/api/indexing/{create_run_response.indexing_result_id}",
            json=JsonIndexingResultRootJson(
                error=None,
                result=JsonIndexingResult(
                    # More or less random values, we don't care about the specifics here
                    frames=200,
                    # Hit rate 50%
                    hits=100,
                    # Indexing rate 20%
                    indexed_frames=20,
                    indexed_crystals=25,
                    done=True,
                    detector_shift_x_mm=0.5,
                    detector_shift_y_mm=-0.5,
                ),
            ).dict(),
        ).json()
    )

    assert update_indexing_job_response.result

    # The result of this indexing you can query in various places. One of the most prominent ones is the "read runs" call.
    # For that to work, however, we need a data set.
    #
    # The test will be "unnecessarily" long now, but let's test the "create data set from run" feature just now
    create_data_set_response = JsonCreateDataSetFromRunOutput(
        **client.post(
            "/api/data-sets/from-run",
            json=JsonCreateDataSetFromRun(
                run_internal_id=create_run_response.run_internal_id,
                beamtime_id=beamtime_id,
                experiment_type_id=chemical_experiment_type_id,
            ).dict(),
        ).json()
    )

    assert create_data_set_response.data_set_id > 0

    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())
    assert len(read_runs_output.data_sets) == 1
    assert read_runs_output.data_sets[0].id == create_data_set_response.data_set_id
    assert read_runs_output.data_sets[0].attributi == [
        JsonAttributoValue(
            attributo_id=run_channel_1_chemical_attributo_id,
            attributo_value_int=lyso_chemical_id,
        )
    ]
    assert (
        read_runs_output.data_sets[0].experiment_type_id == chemical_experiment_type_id
    )
    summary = read_runs_output.data_sets[0].summary
    assert summary is not None
    assert summary.hit_rate == pytest.approx(50, 0.01)
    assert summary.indexing_rate == pytest.approx(20, 0.01)
    assert summary.detector_shift_x_mm == pytest.approx(0.5, 0.01)
    assert summary.detector_shift_y_mm == pytest.approx(-0.5, 0.01)

    # Another place is the analysis view
    analysis_response = JsonReadAnalysisResults(
        **client.get(f"/api/analysis/analysis-results/{beamtime_id}").json()
    )

    assert len(analysis_response.data_sets) == 1
    assert analysis_response.data_sets[0].experiment_type == chemical_experiment_type_id
    assert len(analysis_response.data_sets[0].data_sets) == 1
    assert (
        analysis_response.data_sets[0].data_sets[0].data_set.id
        == create_data_set_response.data_set_id
    )
    assert len(analysis_response.data_sets[0].data_sets[0].runs) == 1
    assert analysis_response.data_sets[0].data_sets[0].runs[0] == str(external_run_id)
    assert analysis_response.data_sets[0].data_sets[0].number_of_indexing_results == 1


def test_change_run_experiment_type(
    client: TestClient,
    beamtime_id: int,
    chemical_experiment_type_id: int,
    string_experiment_type_id: int,
    run_channel_1_chemical_attributo_id: int,
    lyso_chemical_id: int,
) -> None:
    set_current_experiment_type(client, beamtime_id, chemical_experiment_type_id)

    # Let's make the external run ID deliberately high
    external_run_id = 1000

    create_response = JsonCreateOrUpdateRunOutput(
        **client.post(
            f"/api/runs/{external_run_id}",
            json=JsonCreateOrUpdateRun(
                beamtime_id=beamtime_id,
                attributi=[
                    # we don't even mention the second run attributo here, since we're going to add it later and test if that works
                    JsonAttributoValue(
                        attributo_id=run_channel_1_chemical_attributo_id,
                        attributo_value_int=lyso_chemical_id,
                    )
                ],
                started=1,
                stopped=None,
            ).dict(),
        ).json()
    )
    assert create_response.run_created

    # To check if just one run has a different experiment type ID in the end, create another run that isn't changed
    external_run_id_2 = external_run_id + 1
    second_create_response = JsonCreateOrUpdateRunOutput(
        **client.post(
            f"/api/runs/{external_run_id_2}",
            json=JsonCreateOrUpdateRun(
                beamtime_id=beamtime_id,
                attributi=[
                    # we don't even mention the second run attributo here, since we're going to add it later and test if that works
                    JsonAttributoValue(
                        attributo_id=run_channel_1_chemical_attributo_id,
                        attributo_value_int=lyso_chemical_id,
                    )
                ],
                started=1,
                stopped=None,
            ).dict(),
        ).json()
    )
    assert second_create_response.run_created
    assert create_response.run_internal_id is not None

    output = JsonChangeRunExperimentTypeOutput(
        **client.post(
            "/api/experiment-types/change-for-run",
            json=JsonChangeRunExperimentType(
                run_internal_id=create_response.run_internal_id,
                beamtime_id=beamtime_id,
                experiment_type_id=string_experiment_type_id,
            ).dict(),
        ).json()
    )

    assert output.result

    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())

    assert read_runs_output.runs[1].id == create_response.run_internal_id


def test_create_and_delete_data_set(
    client: TestClient,
    beamtime_id: int,
    chemical_experiment_type_id: int,
    lyso_chemical_id: int,
    run_channel_1_chemical_attributo_id: int,
) -> None:
    create_response = JsonCreateDataSetOutput(
        **client.post(
            "/api/data-sets",
            json=JsonCreateDataSetInput(
                beamtime_id=beamtime_id,
                experiment_type_id=chemical_experiment_type_id,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=run_channel_1_chemical_attributo_id,
                        # Good old Lyso!
                        attributo_value_int=lyso_chemical_id,
                    )
                ],
            ).dict(),
        ).json()
    )

    assert create_response.id > 0

    # Now retrieve the data set list
    data_sets_response = JsonReadDataSets(
        **client.get(f"/api/data-sets/{beamtime_id}").json()
    )
    assert len(data_sets_response.data_sets) == 1
    assert data_sets_response.data_sets[0].id == create_response.id
    assert (
        data_sets_response.data_sets[0].experiment_type_id
        == chemical_experiment_type_id
    )
    assert data_sets_response.data_sets[0].attributi == [
        JsonAttributoValue(
            attributo_id=run_channel_1_chemical_attributo_id,
            # Good old Lyso!
            attributo_value_int=lyso_chemical_id,
        )
    ]

    # ...and remove the data set again
    assert JsonDeleteDataSetOutput(
        **client.request(
            "DELETE",
            "/api/data-sets",
            json=JsonDeleteDataSetInput(id=create_response.id).dict(),
        ).json()
    ).result

    assert not JsonReadDataSets(
        **client.get(f"/api/data-sets/{beamtime_id}").json()
    ).data_sets


def test_start_merge_job(
    client: TestClient,
    beamtime_id: int,
    chemical_experiment_type_id: int,
    run_channel_1_chemical_attributo_id: int,
    lyso_chemical_id: int,
    test_file: int,
) -> None:
    # Set the experiment type (otherwise creating a run will fail - see above)
    set_current_experiment_type(client, beamtime_id, chemical_experiment_type_id)
    # Enable CrystFEL online so an indexing job will be created
    enable_crystfel_online(client, beamtime_id)

    # Let's make the external run ID deliberately high
    external_run_id = 1000

    # Create the run and check the result
    create_run_response = JsonCreateOrUpdateRunOutput(
        **client.post(
            f"/api/runs/{external_run_id}",
            json=JsonCreateOrUpdateRun(
                beamtime_id=beamtime_id,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=run_channel_1_chemical_attributo_id,
                        attributo_value_int=lyso_chemical_id,
                    )
                ],
                started=1,
                stopped=None,
            ).dict(),
        ).json()
    )

    assert create_run_response.run_internal_id is not None
    assert (
        create_run_response.indexing_result_id is not None
        and create_run_response.indexing_result_id > 0
    )

    assert JsonIndexingJobUpdateOutput(
        **client.post(
            f"/api/indexing/{create_run_response.indexing_result_id}",
            json=JsonIndexingResultRootJson(
                error=None,
                result=JsonIndexingResult(
                    # More or less random values, we don't care about the specifics here
                    frames=200,
                    # Hit rate 50%
                    hits=100,
                    # Indexing rate 20%
                    indexed_frames=20,
                    indexed_crystals=25,
                    done=True,
                    detector_shift_x_mm=0.5,
                    detector_shift_y_mm=-0.5,
                ),
            ).dict(),
        ).json()
    ).result

    # The result of this indexing you can query in various places. One of the most prominent ones is the "read runs" call.
    # For that to work, however, we need a data set.
    #
    # The test will be "unnecessarily" long now, but let's test the "create data set from run" feature just now
    create_data_set_response = JsonCreateDataSetFromRunOutput(
        **client.post(
            "/api/data-sets/from-run",
            json=JsonCreateDataSetFromRun(
                run_internal_id=create_run_response.run_internal_id,
                beamtime_id=beamtime_id,
                experiment_type_id=chemical_experiment_type_id,
            ).dict(),
        ).json()
    )

    assert create_data_set_response.data_set_id > 0

    start_merge_job_response = JsonStartMergeJobForDataSetOutput(
        **client.post(
            f"/api/merging/{create_data_set_response.data_set_id}/start",
            json=JsonStartMergeJobForDataSetInput(
                # Literally random stuff here, doesn't matter.
                strict_mode=False,
                beamtime_id=beamtime_id,
                merge_model=MergeModel.UNITY,
                scale_intensities=ScaleIntensities.OFF,
                post_refinement=False,
                iterations=3,
                polarisation=JsonPolarisation(angle=30, percent=50),
                negative_handling=MergeNegativeHandling.IGNORE,
                start_after=None,
                stop_after=None,
                rel_b=1.0,
                no_pr=False,
                force_bandwidth=None,
                force_radius=None,
                force_lambda=None,
                no_delta_cc_half=False,
                max_adu=None,
                min_measurements=1,
                logs=False,
                min_res=None,
                push_res=None,
                w=None,
            ).dict(),
        ).json()
    )
    assert start_merge_job_response.merge_result_id > 0

    merge_result = MergeResult(
        mtz_file_id=test_file,
        fom=MergeResultFom(
            # Again, more or less completely random stuff here
            snr=1.0,
            wilson=None,
            ln_k=None,
            discarded_reflections=1,
            one_over_d_from=1.1,
            one_over_d_to=1.2,
            redundancy=1.3,
            completeness=1.4,
            measurements_total=10,
            reflections_total=11,
            reflections_possible=12,
            r_split=1.5,
            r1i=1.6,
            r2=1.7,
            cc=1.8,
            ccstar=1.9,
            ccano=2.0,
            crdano=2.1,
            rano=2.2,
            rano_over_r_split=2.3,
            d1sig=2.4,
            d2sig=2.5,
            outer_shell=MergeResultOuterShell(
                resolution=2.6,
                ccstar=2.7,
                r_split=2.8,
                cc=2.9,
                unique_reflections=20,
                completeness=3.0,
                redundancy=3.1,
                snr=3.2,
                min_res=3.3,
                max_res=3.4,
            ),
        ),
        detailed_foms=[
            MergeResultShell(
                one_over_d_centre=4.0,
                nref=3,
                d_over_a=4.1,
                min_res=4.2,
                max_res=4.3,
                cc=4.4,
                ccstar=4.5,
                r_split=4.6,
                reflections_possible=4,
                completeness=4.7,
                measurements=5,
                redundancy=4.8,
                snr=4.9,
                mean_i=5.0,
            )
        ],
        refinement_results=[
            RefinementResult(
                pdb_file_id=test_file,
                mtz_file_id=test_file,
                r_free=0.5,
                r_work=0.6,
                rms_bond_angle=180.0,
                rms_bond_length=2.0,
            )
        ],
    )

    # Now finish merge job
    finish_merge_job_response = JsonMergeJobUpdateOutput(
        **client.post(
            f"/api/merging/{start_merge_job_response.merge_result_id}",
            json=JsonMergeResultRootJson(
                error=None,
                result=merge_result,
            ).dict(),
        ).json()
    )

    assert finish_merge_job_response.result

    # Get the analysis view and find our result!
    analysis_response = JsonReadAnalysisResults(
        **client.get(f"/api/analysis/analysis-results/{beamtime_id}").json()
    )

    assert len(analysis_response.data_sets) == 1
    first_ds = analysis_response.data_sets[0]
    assert len(first_ds.data_sets) == 1
    first_ds_ds = first_ds.data_sets[0]
    assert len(first_ds_ds.merge_results) == 1
    first_mr = first_ds_ds.merge_results[0]
    assert first_mr.id == start_merge_job_response.merge_result_id
    assert first_mr.cell_description == LYSO_CELL_DESCRIPTION
    assert first_mr.point_group == LYSO_POINT_GROUP
    assert first_mr.runs == [str(external_run_id)]
    assert first_mr.parameters == JsonMergeParameters(
        point_group=LYSO_POINT_GROUP,
        cell_description=LYSO_CELL_DESCRIPTION,
        negative_handling=MergeNegativeHandling.IGNORE,
        merge_model=MergeModel.UNITY,
        scale_intensities=ScaleIntensities.OFF,
        post_refinement=False,
        iterations=3,
        polarisation=JsonPolarisation(angle=30, percent=50),
        start_after=None,
        stop_after=None,
        rel_b=1.0,
        no_pr=False,
        force_bandwidth=None,
        force_radius=None,
        force_lambda=None,
        no_delta_cc_half=False,
        max_adu=None,
        min_measurements=1,
        logs=False,
        min_res=None,
        push_res=None,
        w=None,
    )
    assert len(first_mr.refinement_results) == 1
    refinement_result_id = first_mr.refinement_results[0].id
    assert first_mr.refinement_results[0] == JsonRefinementResult(
        id=refinement_result_id,
        merge_result_id=start_merge_job_response.merge_result_id,
        pdb_file_id=test_file,
        mtz_file_id=test_file,
        r_free=0.5,
        r_work=0.6,
        rms_bond_angle=180.0,
        rms_bond_length=2.0,
    )
    assert first_mr.state_queued is None
    assert first_mr.state_error is None
    assert first_mr.state_running is None
    assert first_mr.state_done is not None
    assert first_mr.state_done.result == merge_result


def test_start_run(
    client: TestClient,
    beamtime_id: int,
    chemical_experiment_type_id: int,
) -> None:
    # Set the experiment type (otherwise creating a run will fail - see above)
    set_current_experiment_type(client, beamtime_id, chemical_experiment_type_id)

    # Let's make the external run ID deliberately high
    external_run_id = 1000

    # Create the run and check the result
    start_run_response = JsonStartRunOutput(
        **client.get(f"/api/runs/{external_run_id}/start/{beamtime_id}").json()
    )

    assert start_run_response.run_internal_id is not None
    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())

    assert len(read_runs_output.runs) == 1
    assert read_runs_output.runs[0].id == start_run_response.run_internal_id


def test_start_run_no_experiment_type_set(
    client: TestClient,
    beamtime_id: int,
) -> None:
    # Let's make the external run ID deliberately high
    external_run_id = 1000

    # Create the run and check the result
    start_run_response = client.get(f"/api/runs/{external_run_id}/start/{beamtime_id}")

    assert start_run_response.status_code == 400


def test_read_and_update_runs_bulk(
    client: TestClient,
    beamtime_id: int,
    chemical_experiment_type_id: int,
    string_experiment_type_id: int,
    run_string_attributo_id: int,
    run_int_attributo_id: int,
    run_channel_1_chemical_attributo_id: int,
) -> None:
    # Set the experiment type (otherwise creating a run will fail - see above)
    set_current_experiment_type(client, beamtime_id, chemical_experiment_type_id)

    run_definitions = [
        {"id": 1000, "string-attributo": "foo", "int-attributo": 3},
        {"id": 1001, "string-attributo": "bar", "int-attributo": 4},
        {"id": 1002, "string-attributo": None, "int-attributo": 4},
        {"id": 1003, "string-attributo": "bar", "int-attributo": None},
    ]

    run_internal_ids: list[int] = []

    for run_definition in run_definitions:
        response = JsonCreateOrUpdateRunOutput(
            **client.post(
                f"/api/runs/{run_definition['id']}",
                json=JsonCreateOrUpdateRun(
                    beamtime_id=beamtime_id,
                    attributi=(
                        [
                            JsonAttributoValue(
                                attributo_id=run_string_attributo_id,
                                attributo_value_str=run_definition["string-attributo"],  # type: ignore
                            )
                        ]
                        if run_definition["string-attributo"] is not None
                        else []
                    )
                    + (
                        [
                            JsonAttributoValue(
                                attributo_id=run_int_attributo_id,
                                attributo_value_int=run_definition["int-attributo"],  # type: ignore
                            )
                        ]
                        if run_definition["int-attributo"] is not None
                        else []
                    ),
                    started=1,
                    stopped=None,
                ).dict(),
            ).json()
        )
        assert response.run_created
        assert response.run_internal_id is not None
        run_internal_ids.append(response.run_internal_id)

    read_output = JsonReadRunsBulkOutput(
        **client.post(
            "/api/runs-bulk",
            json=JsonReadRunsBulkInput(
                beamtime_id=beamtime_id,
                external_run_ids=[x["id"] for x in run_definitions],  # type: ignore
            ).dict(),
        ).json()
    )

    assert read_output.experiment_type_ids == [chemical_experiment_type_id]
    assert read_output.experiment_types

    # Annoying redirect to Python's "unittest" framework, but it's a very elegant solution to the problem
    # of comparing two lists of elements that are not hashable (as BaseModel, right now, is)
    tc = TestCase()
    tc.maxDiff = None
    bulk_values_chemical: list[JsonAttributoValue] = [
        y
        for x in read_output.attributi_values
        if x.attributo_id == run_channel_1_chemical_attributo_id
        for y in x.values
    ]
    assert not bulk_values_chemical
    bulk_values_string = [
        y
        for x in read_output.attributi_values
        if x.attributo_id == run_string_attributo_id
        for y in x.values
    ]
    tc.assertCountEqual(
        bulk_values_string,
        [
            JsonAttributoValue(
                attributo_id=run_string_attributo_id,
                attributo_value_str="foo",
            ),
            JsonAttributoValue(
                attributo_id=run_string_attributo_id,
                attributo_value_str="bar",
            ),
        ],
    )
    bulk_values_int = [
        y
        for x in read_output.attributi_values
        if x.attributo_id == run_int_attributo_id
        for y in x.values
    ]
    tc.assertCountEqual(
        bulk_values_int,
        [
            JsonAttributoValue(
                attributo_id=run_int_attributo_id,
                attributo_value_int=3,
            ),
            JsonAttributoValue(
                attributo_id=run_int_attributo_id,
                attributo_value_int=4,
            ),
        ],
    )

    # Let's first just update one of the attributi: the string one, and set it to something completely different, so
    # there's a change in every run. Also, let's change the experiment type
    write_output = JsonUpdateRunsBulkOutput(
        **client.patch(
            "/api/runs-bulk",
            json=JsonUpdateRunsBulkInput(
                beamtime_id=beamtime_id,
                external_run_ids=[x["id"] for x in run_definitions],  # type: ignore
                attributi=[
                    JsonAttributoValue(
                        attributo_id=run_string_attributo_id, attributo_value_str="qux"
                    )
                ],
                new_experiment_type_id=string_experiment_type_id,
            ).dict(),
        ).json()
    )

    assert write_output.result

    read_runs_output = JsonReadRuns(**client.get(f"/api/runs/{beamtime_id}").json())

    for run in read_runs_output.runs:
        assert run.experiment_type_id == string_experiment_type_id

        # First test: did our string update succeed?
        string_attributo = next(
            iter(
                [a for a in run.attributi if a.attributo_id == run_string_attributo_id]
            ),
            None,
        )
        assert string_attributo is not None
        assert string_attributo.attributo_value_str == "qux"

        # Second test: is the run "the same"? Check that the
        # previously unset attribute for the chemical ID is still not
        # there.
        channel_1_attributo = next(
            iter(
                [
                    a
                    for a in run.attributi
                    if a.attributo_id == run_channel_1_chemical_attributo_id
                ]
            ),
            None,
        )
        assert channel_1_attributo is None


def test_create_file_simple(client: TestClient, test_file_path: Path) -> None:
    # Upload the file
    with test_file_path.open("rb") as upload_file:
        raw_output = client.post(
            "/api/files/simple/txt",
            # Important: don't use "files=" here since we're sending the data without multipart shenenigans
            content=upload_file.read(),
        )
        print(raw_output.json())
        output = JsonCreateFileOutput(**raw_output.json())

        assert output.file_name.endswith(".txt")
        assert output.description == ""
        assert output.type_ == "text/plain"
        assert output.size_in_bytes == 17
        assert output.id > 0

    # Retrieve the contents (not the metadata)
    with test_file_path.open("rb") as upload_file:
        assert client.get(f"/api/files/{output.id}").content == upload_file.read()


def test_read_and_update_user_config(client: TestClient, beamtime_id: int) -> None:
    result_ap = JsonUserConfigurationSingleOutput(
        **client.get(f"/api/user-config/{beamtime_id}/auto-pilot").json()
    )
    assert result_ap.value_bool is not None
    assert result_ap.value_int is None

    result_ap = JsonUserConfigurationSingleOutput(
        **client.patch(f"/api/user-config/{beamtime_id}/auto-pilot/True").json()
    )
    assert result_ap.value_bool

    result_ap = JsonUserConfigurationSingleOutput(
        **client.patch(f"/api/user-config/{beamtime_id}/auto-pilot/False").json()
    )
    assert not result_ap.value_bool

    result_co = JsonUserConfigurationSingleOutput(
        **client.get(f"/api/user-config/{beamtime_id}/online-crystfel").json()
    )
    assert result_co.value_bool is not None
    assert result_co.value_int is None

    result_co = JsonUserConfigurationSingleOutput(
        **client.patch(f"/api/user-config/{beamtime_id}/online-crystfel/True").json()
    )
    assert result_co.value_bool

    result_co = JsonUserConfigurationSingleOutput(
        **client.patch(f"/api/user-config/{beamtime_id}/online-crystfel/False").json()
    )
    assert not result_co.value_bool


def test_read_experiment_types(
    client: TestClient, beamtime_id: int, chemical_experiment_type_id: int
) -> None:
    result = JsonReadExperimentTypes(
        **client.get(f"/api/experiment-types/{beamtime_id}").json()
    )

    assert len(result.experiment_types) == 1
    assert result.experiment_types[0].id == chemical_experiment_type_id


def test_delete_experiment_types(
    client: TestClient,
    beamtime_id: int,
    chemical_experiment_type_id: int,
    run_channel_1_chemical_attributo_id: int,
    lyso_chemical_id: int,
) -> None:
    # to really test the experiment type deletion thing, let's create a data set for the experiment type as well
    create_response = JsonCreateDataSetOutput(
        **client.post(
            "/api/data-sets",
            json=JsonCreateDataSetInput(
                beamtime_id=beamtime_id,
                experiment_type_id=chemical_experiment_type_id,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=run_channel_1_chemical_attributo_id,
                        # Good old Lyso!
                        attributo_value_int=lyso_chemical_id,
                    )
                ],
            ).dict(),
        ).json()
    )

    assert create_response.id > 0

    result = JsonDeleteExperimentTypeOutput(
        **client.request(
            "DELETE",
            "/api/experiment-types",
            json=JsonDeleteExperimentType(id=chemical_experiment_type_id).dict(),
        ).json()
    )

    assert result.result

    assert not JsonReadExperimentTypes(
        **client.get(f"/api/experiment-types/{beamtime_id}").json()
    ).experiment_types

    # Now retrieve the data set list, should be empty
    data_sets_response = JsonReadDataSets(
        **client.get(f"/api/data-sets/{beamtime_id}").json()
    )
    assert not data_sets_response.data_sets


def test_delete_file(client: TestClient, test_file: int) -> None:
    result = JsonDeleteFileOutput(
        **client.request(
            "DELETE",
            "/api/files",
            json=JsonDeleteFileInput(id=test_file).dict(),
        ).json()
    )
    assert result.id == test_file

    assert client.get(f"/api/files/{test_file}").status_code == 404


def test_update_beamtime_schedule(
    client: TestClient, beamtime_id: int, lyso_chemical_id: int
) -> None:
    schedule_rows = [
        JsonBeamtimeScheduleRow(
            users="users",
            date="date",
            shift="shift",
            comment="comment",
            td_support="td_support",
            chemicals=[lyso_chemical_id],
        ),
        JsonBeamtimeScheduleRow(
            users="users2",
            date="date2",
            shift="shift2",
            comment="comment2",
            td_support="td_support2",
            chemicals=[],
        ),
    ]
    update_result = JsonBeamtimeScheduleOutput(
        **client.post(
            "/api/schedule",
            json=JsonUpdateBeamtimeScheduleInput(
                beamtime_id=beamtime_id,
                schedule=schedule_rows,
            ).dict(),
        ).json()
    )

    assert update_result.schedule == schedule_rows

    get_response = JsonBeamtimeSchedule(
        **client.get(f"/api/schedule/{beamtime_id}").json()
    )
    assert get_response.schedule == schedule_rows


def test_create_live_stream_snapshot(
    client: TestClient, beamtime_id: int, test_file_path: Path
) -> None:
    # Upload the file: for the live stream, it's important to use the proper file name, as that's the criterion for a file to be
    # a live stream (yes, ugly, I know).
    with test_file_path.open("rb") as upload_file:
        raw_output = client.post(
            "/api/files",
            data={"description": "test description"},
            # To pass a file name as well as the file contents, httpx uses a tuple.
            files={"file": (f"live-stream-image-{beamtime_id}", upload_file)},
        )
        file = JsonCreateFileOutput(**raw_output.json())

    snapshot_response = JsonCreateLiveStreamSnapshotOutput(
        **client.get(f"/api/live-stream/snapshot/{beamtime_id}").json()
    )

    assert snapshot_response.id != file.id
    assert snapshot_response.size_in_bytes == file.size_in_bytes


def test_update_live_stream(
    client: TestClient, beamtime_id: int, test_file_path: Path
) -> None:
    # Upload the file: for the live stream, it's important to use the proper file name, as that's the criterion for a file to be
    # a live stream (yes, ugly, I know).
    with test_file_path.open("rb") as upload_file:
        raw_output = client.post(
            "/api/files",
            data={"description": "test description"},
            # To pass a file name as well as the file contents, httpx uses a tuple.
            files={"file": (f"live-stream-image-{beamtime_id}", upload_file)},
        )
        original_file = JsonCreateFileOutput(**raw_output.json())

        raw_output = client.post(
            f"/api/live-stream/{beamtime_id}",
            # no file name needed here, it's implicit taht it's "the live stream"
            files={"file": upload_file},
        )
        new_file = JsonUpdateLiveStream(**raw_output.json())

        assert new_file.id == original_file.id


def test_check_standard_unit(client: TestClient) -> None:
    # First, test normalization
    output = JsonCheckStandardUnitOutput(
        **client.post(
            "/api/unit", json=JsonCheckStandardUnitInput(input="mm").dict()
        ).json()
    )
    assert output.error is None
    assert output.normalized == "1 millimeter"

    # Now try an invalid unit
    output = JsonCheckStandardUnitOutput(
        **client.post(
            "/api/unit", json=JsonCheckStandardUnitInput(input="bananas").dict()
        ).json()
    )
    assert output.error is not None
    assert output.normalized is None

    # Finally, empty string is also an important edge case
    output = JsonCheckStandardUnitOutput(
        **client.post(
            "/api/unit", json=JsonCheckStandardUnitInput(input="").dict()
        ).json()
    )
    assert output.error is not None
    assert output.normalized is None


def test_update_attributo(client: TestClient, beamtime_id: int) -> None:
    attributo_response = JsonCreateAttributoOutput(
        **client.post(
            "/api/attributi",
            json=JsonCreateAttributoInput(
                name="some attributo",
                description="description",
                group=ATTRIBUTO_GROUP_MANUAL,
                associated_table=AssociatedTable.CHEMICAL,
                attributo_type_string=JSONSchemaString(type="string", enum=None),
                beamtime_id=beamtime_id,
            ).dict(),
        ).json()
    )
    assert attributo_response.id > 0

    chemical_response = JsonCreateChemicalOutput(
        **client.post(
            "/api/chemicals",
            json=JsonChemicalWithoutId(
                name=TEST_CHEMICAL_NAME,
                responsible_person=TEST_CHEMICAL_RESPONSIBLE_PERSON,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=attributo_response.id,
                        # this is a string, but it encodes an integer, so we can change the type further down to "int"
                        attributo_value_str="1",
                    ),
                ],
                chemical_type=ChemicalType.CRYSTAL,
                file_ids=[],
                beamtime_id=beamtime_id,
            ).dict(),
        ).json()
    )
    assert chemical_response.id > 0

    updated_attributo = JsonAttributo(
        id=attributo_response.id,
        # Change all attributes, except the associated table
        name="new name",
        description="new description",
        group="new group",
        associated_table=AssociatedTable.CHEMICAL,
        # even change the type to int here
        attributo_type_integer=JSONSchemaInteger(type="integer", format=None),
    )

    attributo_update_response = JsonUpdateAttributoOutput(
        **client.patch(
            "/api/attributi",
            json=JsonUpdateAttributoInput(
                attributo=updated_attributo,
                beamtime_id=beamtime_id,
                conversion_flags=JsonUpdateAttributoConversionFlags(ignoreUnits=True),
            ).dict(),
        ).json()
    )
    assert attributo_update_response.id > 0

    # After updating, check that the attributi list is updated.
    read_attributi_response = JsonReadAttributi(
        **client.get(f"/api/attributi/{beamtime_id}").json()
    )
    assert len(read_attributi_response.attributi) == 1

    assert read_attributi_response.attributi[0] == updated_attributo

    # Also, check that the chemicals list is proper.
    chemical_list = read_chemicals(client, beamtime_id)
    assert len(chemical_list.chemicals) == 1
    assert len(chemical_list.chemicals[0].attributi) == 1
    assert chemical_list.chemicals[0].attributi[0].attributo_value_str is None
    assert chemical_list.chemicals[0].attributi[0].attributo_value_int == 1


def test_delete_attributo(client: TestClient, beamtime_id: int) -> None:
    attributo_response = JsonCreateAttributoOutput(
        **client.post(
            "/api/attributi",
            json=JsonCreateAttributoInput(
                name="some attributo",
                description="description",
                group=ATTRIBUTO_GROUP_MANUAL,
                associated_table=AssociatedTable.CHEMICAL,
                attributo_type_string=JSONSchemaString(type="string", enum=None),
                beamtime_id=beamtime_id,
            ).dict(),
        ).json()
    )
    assert attributo_response.id > 0

    chemical_response = JsonCreateChemicalOutput(
        **client.post(
            "/api/chemicals",
            json=JsonChemicalWithoutId(
                name=TEST_CHEMICAL_NAME,
                responsible_person=TEST_CHEMICAL_RESPONSIBLE_PERSON,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=attributo_response.id,
                        # this is a string, but it encodes an integer, so we can change the type further down to "int"
                        attributo_value_str="1",
                    ),
                ],
                chemical_type=ChemicalType.CRYSTAL,
                file_ids=[],
                beamtime_id=beamtime_id,
            ).dict(),
        ).json()
    )
    assert chemical_response.id > 0

    output = JsonDeleteAttributoOutput(
        **client.request(
            "DELETE",
            "/api/attributi",
            json=JsonDeleteAttributoInput(id=attributo_response.id).dict(),
        ).json()
    )
    assert output.id == attributo_response.id
    read_attributi_response = JsonReadAttributi(
        **client.get(f"/api/attributi/{beamtime_id}").json()
    )
    assert not read_attributi_response.attributi


def test_download_spreadsheet(
    client: TestClient,
    chemical_experiment_type_id: int,
    run_string_attributo_id: int,
    beamtime_id: int,
) -> None:
    # Set the experiment type (otherwise creating a run will fail - see above)
    set_current_experiment_type(client, beamtime_id, chemical_experiment_type_id)

    # Let's make the external run ID deliberately high
    external_run_id = 1000

    run_string_attributo_value = "foo"
    # Create the run and check the result
    create_run_response = JsonCreateOrUpdateRunOutput(
        **client.post(
            f"/api/runs/{external_run_id}",
            json=JsonCreateOrUpdateRun(
                beamtime_id=beamtime_id,
                attributi=[
                    JsonAttributoValue(
                        attributo_id=run_string_attributo_id,
                        attributo_value_str=run_string_attributo_value,
                    )
                ],
                started=1,
                stopped=None,
            ).dict(),
        ).json()
    )
    assert (
        create_run_response.run_internal_id is not None
        and create_run_response.run_internal_id > 0
    )

    response = client.get(f"/api/{beamtime_id}/spreadsheet.zip")
    print(response)
    with ZipFile(BytesIO(response.content)) as zipf:
        names = zipf.namelist()
        assert len(names) == 1
        assert names[0].endswith(".xlsx")

        with zipf.open(names[0]) as sheet:
            # This deviation with BytesIO(read) is apparently necessary. Bad.
            wb = load_workbook(filename=BytesIO(sheet.read()))

            ws = wb.active

            # This test is highly incomplete. Let's add more conditions once we encounter bugs in this thing (yes, that's how testing works!)
            assert ws.title == "Runs"
            assert ws["A1"].value == "ID"
            assert ws["A2"].value == external_run_id
