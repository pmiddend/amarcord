from amarcord.cli.webserver import app, db
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
            "group": "manual",
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
