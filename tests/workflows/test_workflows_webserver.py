import json


def test_add_and_get_tool(client):
    # Assume empty tools list
    rv = client.get("/api/workflows/tools")
    assert rv.get_json() == {"tools": []}

    # Add another tool
    result = client.post(
        "/api/workflows/tools",
        data=dict(
            name="newtool",
            executablePath="/usr/bin/test",
            extraFiles=json.dumps(["/tmp/testfile"]),
            commandLine="foo${bar}baz",
            description="lol",
        ),
    )

    assert result.status_code == 200
    result_json = result.get_json()
    assert result_json["tools"]
    assert result_json["tools"][0]["toolId"] != 0
    assert result_json["tools"][0]["name"] == "newtool"
