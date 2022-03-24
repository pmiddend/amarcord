from amarcord.cli.webserver import app, db
from amarcord.db.asyncdb import ATTRIBUTO_GROUP_MANUAL
from amarcord.db.attributi import ATTRIBUTO_STARTED, ATTRIBUTO_STOPPED
from amarcord.json_checker import JSONChecker


async def test_read_samples() -> None:
    app.config.update(
        {
            "DB_URL": "sqlite+aiosqlite://",
            "DB_ECHO": False,
            "HAS_ARTIFICIAL_DELAY": False,
        },
    )
    await db.initialize_db()
    client = app.test_client()

    result = await client.get("/api/samples")
    json = JSONChecker(await result.json, "response")

    assert len(json.retrieve_safe_list("samples")) == 0


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
            "attributi-names": [attributo_name],
        },
    )

    assert result.status_code == 200
    assert (await result.json) == {}

    result = await client.post(
        "/api/data-sets",
        json={"experiment-type": experiment_type, "attributi": {attributo_name: "3"}},
    )

    assert result.status_code == 200
    assert (await result.json) == {"id": 1}

    result = await client.post(
        "/api/data-sets",
        json={"experiment-type": experiment_type, "attributi": {}},
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
