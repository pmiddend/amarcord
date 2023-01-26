import datetime

from amarcord.cli.webserver import app
from amarcord.cli.webserver import db
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.attributi import ATTRIBUTO_STARTED
from amarcord.db.attributi import ATTRIBUTO_STOPPED
from amarcord.db.chemical_type import ChemicalType
from amarcord.json_checker import JSONChecker
from amarcord.json_types import JSONDict

IN_MEMORY_DB_URL = "sqlite+aiosqlite://"

TEST_ATTRIBUTO_NAME = "testattributo"
TEST_ATTRIBUTO_NAME2 = "testattributo2"
TEST_chemical_NAME = "chemicalname"


async def test_read_chemicals() -> None:
    app.config.update(
        {
            "DB_URL": "sqlite+aiosqlite://",
            "DB_ECHO": False,
            "HAS_ARTIFICIAL_DELAY": False,
        },
    )
    await db.initialize_db()
    client = app.test_client()

    result = await client.get("/api/chemicals")
    json = JSONChecker(await result.json, "response")

    assert len(json.retrieve_safe_list("chemicals")) == 0


async def test_update_chemicals() -> None:
    app.config.update(
        {
            "DB_URL": "sqlite+aiosqlite://",
            "DB_ECHO": False,
            "HAS_ARTIFICIAL_DELAY": False,
        },
    )
    await db.initialize_db()
    client = app.test_client()

    await client.post(
        "/api/attributi",
        json={
            "name": TEST_ATTRIBUTO_NAME,
            "description": "description",
            "group": ATTRIBUTO_GROUP_MANUAL,
            "associatedTable": "chemical",
            "type": {"type": "string"},
        },
    )
    await client.post(
        "/api/attributi",
        json={
            "name": TEST_ATTRIBUTO_NAME2,
            "description": "description",
            "group": ATTRIBUTO_GROUP_MANUAL,
            "associatedTable": "chemical",
            "type": {"type": "string"},
        },
    )

    result = await client.post(
        "/api/chemicals",
        json={
            "name": TEST_chemical_NAME,
            "attributi": {TEST_ATTRIBUTO_NAME: "foo", TEST_ATTRIBUTO_NAME2: "bar"},
            "type": ChemicalType.CRYSTAL.value,
            "fileIds": [],
        },
    )

    json = JSONChecker(await result.json, "response")
    chemical_id = json.retrieve_safe_int("id")

    result = await client.patch(
        "/api/chemicals",
        json={
            "id": chemical_id,
            "name": TEST_chemical_NAME,
            "type": ChemicalType.CRYSTAL.value,
            # Only update hte second attributo. The first should stay the same.
            "attributi": {TEST_ATTRIBUTO_NAME2: "baz"},
            "fileIds": [],
        },
    )

    assert result.status_code == 200

    result = await client.get("/api/chemicals")
    json = JSONChecker(await result.json, "response")

    chemicals: list[JSONDict] = json.retrieve_safe_array("chemicals")
    assert len(chemicals) == 1

    assert chemicals[0]["id"] == chemical_id
    assert chemicals[0]["attributi"][TEST_ATTRIBUTO_NAME] == "foo"  # type: ignore
    assert chemicals[0]["attributi"][TEST_ATTRIBUTO_NAME2] == "baz"  # type: ignore


async def test_data_sets() -> None:
    app.config.update(
        {
            "DB_URL": "sqlite+aiosqlite://",
            "DB_ECHO": False,
            "HAS_ARTIFICIAL_DELAY": False,
        },
    )
    await db.initialize_db()
    client = app.test_client()

    attributo_name = "attributo1"
    result = await client.post(
        "/api/attributi",
        json={
            "name": attributo_name,
            "description": "description",
            "group": ATTRIBUTO_GROUP_MANUAL,
            "associatedTable": "run",
            "type": {"type": "string"},
        },
    )

    assert result.status_code == 200
    assert (await result.json) == {}

    experiment_type = "experiment_type"
    result = await client.post(
        "/api/experiment-types",
        json={
            "name": experiment_type,
            "attributi": [{"name": attributo_name, "role": ChemicalType.CRYSTAL.value}],
        },
    )

    assert result.status_code == 200
    result_json = await result.json
    assert result_json == {"id": 1}

    et_id = result_json["id"]

    result = await client.post(
        "/api/data-sets",
        json={"experiment-type-id": et_id, "attributi": {attributo_name: "3"}},
    )

    assert result.status_code == 200
    assert (await result.json) == {"id": 1}

    result = await client.post(
        "/api/data-sets",
        json={"experiment-type-id": et_id, "attributi": {}},
    )

    assert result.status_code == 200
    assert "error" in (await result.json)


async def test_start_and_stop_runs() -> None:
    app.config.update(
        {
            "DB_URL": "sqlite+aiosqlite://",
            "DB_ECHO": False,
            "HAS_ARTIFICIAL_DELAY": False,
        },
    )
    await db.initialize_db()
    client = app.test_client()

    # Start run 1
    await client.get("/api/runs/1/start")

    runs_response = await client.get("/api/runs")
    runs_response_json = await runs_response.json

    assert "runs" in runs_response_json
    assert len(runs_response_json["runs"]) == 1
    assert runs_response_json["runs"][0]["attributi"][ATTRIBUTO_STARTED] is not None
    assert ATTRIBUTO_STOPPED not in runs_response_json["runs"][0]["attributi"]

    # Stop the run
    await client.get("/api/runs/stop-latest")

    runs_response = await client.get("/api/runs")
    runs_response_json = await runs_response.json

    assert "runs" in runs_response_json
    assert len(runs_response_json["runs"]) == 1
    stopped = runs_response_json["runs"][0]["attributi"][ATTRIBUTO_STOPPED] is not None
    assert stopped is not None


async def test_filter_runs_by_started_date() -> None:
    app.config.update(
        {
            "DB_URL": "sqlite+aiosqlite://",
            "DB_ECHO": False,
            "HAS_ARTIFICIAL_DELAY": False,
        },
    )
    await db.initialize_db()
    client = app.test_client()

    # Start run 1
    await client.get("/api/runs/1/start")

    runs_response = await client.get("/api/runs")
    runs_response_json = await runs_response.json

    assert "runs" in runs_response_json
    assert len(runs_response_json["runs"]) == 1
    assert runs_response_json["runs"][0]["attributi"][ATTRIBUTO_STARTED] is not None
    assert ATTRIBUTO_STOPPED not in runs_response_json["runs"][0]["attributi"]

    # Stop the run
    await client.get("/api/runs/stop-latest")

    runs_response = await client.get("/api/runs")
    runs_response_json = await runs_response.json

    assert "runs" in runs_response_json
    assert len(runs_response_json["runs"]) == 1
    stopped = runs_response_json["runs"][0]["attributi"][ATTRIBUTO_STOPPED] is not None
    assert stopped is not None
    assert runs_response_json["filter-dates"] == [
        datetime.date.today().strftime("%Y-%m-%d")
    ]

    # get the (non-existing) runs from another day
    runs_filtered_response = await client.get("/api/runs?date=2000-01-01")
    runs_filtered_response_json = await runs_filtered_response.json
    assert len(runs_filtered_response_json["runs"]) == 0


async def test_attributi_are_presorted() -> None:
    app.config.update(
        {
            "DB_URL": "sqlite+aiosqlite://",
            "DB_ECHO": False,
            "HAS_ARTIFICIAL_DELAY": False,
        },
    )
    # Don't pre-initialize started/stopped, so we can really check if the order mattered
    await db.initialize_db()
    client = app.test_client()

    testattributo = "testattributo"
    await client.post(
        "/api/attributi",
        json={
            "name": testattributo,
            "description": "",
            "group": ATTRIBUTO_GROUP_MANUAL,
            "associatedTable": "run",
            "type": {"type": "string"},
        },
    )
    await client.post(
        "/api/attributi",
        json={
            "name": ATTRIBUTO_STARTED,
            "description": "",
            "group": ATTRIBUTO_GROUP_MANUAL,
            "associatedTable": "run",
            "type": {"type": "integer"},
        },
    )
    await client.post(
        "/api/attributi",
        json={
            "name": ATTRIBUTO_STOPPED,
            "description": "",
            "group": ATTRIBUTO_GROUP_MANUAL,
            "associatedTable": "run",
            "type": {"type": "integer"},
        },
    )

    runs_response = await client.get("/api/runs")
    runs_response_json = await runs_response.json

    assert "attributi" in runs_response_json
    # We need at least "started" and "stopped"
    assert len(runs_response_json["attributi"]) >= 2
    assert runs_response_json["attributi"][0]["name"] == ATTRIBUTO_STARTED
    assert runs_response_json["attributi"][1]["name"] == ATTRIBUTO_STOPPED
    assert runs_response_json["attributi"][2]["name"] == testattributo


async def test_create_data_set_with_unspecified_boolean() -> None:
    app.config.update(
        {
            "DB_URL": IN_MEMORY_DB_URL,
            "DB_ECHO": False,
            "HAS_ARTIFICIAL_DELAY": False,
        },
    )
    await db.initialize_db()
    client = app.test_client()

    attributo_name = "attributo1"
    await client.post(
        "/api/attributi",
        json={
            "name": attributo_name,
            "description": "description",
            "group": ATTRIBUTO_GROUP_MANUAL,
            "associatedTable": "run",
            "type": {"type": "boolean"},
        },
    )
    attributo_name2 = "attributo2"
    await client.post(
        "/api/attributi",
        json={
            "name": attributo_name2,
            "description": "description",
            "group": ATTRIBUTO_GROUP_MANUAL,
            "associatedTable": "run",
            "type": {"type": "string"},
        },
    )

    experiment_type = "experiment_type"
    result = await client.post(
        "/api/experiment-types",
        json={
            "name": experiment_type,
            "attributi": [
                {"name": attributo_name, "role": ChemicalType.CRYSTAL.value},
                {"name": attributo_name2, "role": ChemicalType.CRYSTAL.value},
            ],
        },
    )

    result_json = await result.json

    assert result.status_code == 200
    assert (await result.json) == {"id": 1}

    et_id = result_json["id"]

    result = await client.post(
        "/api/data-sets",
        # Only set string attributo here
        json={"experiment-type-id": et_id, "attributi": {attributo_name2: "3"}},
    )

    assert result.status_code == 200
    assert (await result.json) == {"id": 1}

    result = await client.get("/api/data-sets")
    json = JSONChecker(await result.json, "response")

    assert len(json.retrieve_safe_list("data-sets")) == 1
    # Here, we expect the boolean attribute to be filled with false implicitly
    # noinspection PyTypeChecker
    assert json.retrieve_safe_list("data-sets")[0]["attributi"][attributo_name] is False


async def test_create_data_set_with_unspecified_string() -> None:
    app.config.update(
        {
            "DB_URL": IN_MEMORY_DB_URL,
            "DB_ECHO": False,
            "HAS_ARTIFICIAL_DELAY": False,
        },
    )
    await db.initialize_db()
    client = app.test_client()

    attributo_name = "attributo1"
    await client.post(
        "/api/attributi",
        json={
            "name": attributo_name,
            "description": "description",
            "group": ATTRIBUTO_GROUP_MANUAL,
            "associatedTable": "run",
            "type": {"type": "boolean"},
        },
    )
    attributo_name2 = "attributo2"
    await client.post(
        "/api/attributi",
        json={
            "name": attributo_name2,
            "description": "description",
            "group": ATTRIBUTO_GROUP_MANUAL,
            "associatedTable": "run",
            "type": {"type": "string"},
        },
    )

    experiment_type = "experiment_type"
    result = await client.post(
        "/api/experiment-types",
        json={
            "name": experiment_type,
            "attributi": [
                {"name": attributo_name, "role": ChemicalType.CRYSTAL.value},
                {"name": attributo_name2, "role": ChemicalType.CRYSTAL.value},
            ],
        },
    )

    result_json = await result.json

    assert result.status_code == 200
    assert (await result.json) == {"id": 1}

    et_id = result_json["id"]

    result = await client.post(
        "/api/data-sets",
        # Only set the boolean attributo here, leave string out - that's not possible!
        json={"experiment-type-id": et_id, "attributi": {attributo_name: False}},
    )

    assert result.status_code == 200
    assert "error" in (await result.json)
