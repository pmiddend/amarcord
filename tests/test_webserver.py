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
