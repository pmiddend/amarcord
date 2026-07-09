# ruff: disable[ERA001]
import json

from fastapi.openapi.utils import get_openapi

from amarcord.cli.webserver import app

schema = get_openapi(
    version="2.5.0",
    routes=app.routes,
    title="AMARCORD OpenAPI",
    openapi_version="3.0.3",
)

# Also, the generated schema contains a type for HTTP validation
# errors (whatever that is) that has a property "loc" which is:

# "loc": {
#    "items": {
#      "anyOf": [
#        {
#          "type": "string"
#        },
#        {
#          "type": "integer"
#        }
#      ]
#    },
#    "type": "array",
#    "title": "Location"
#  },

# Which completely trips up the Elm generator (probably because of the
# "anyOf" thingies, so we repair it:

try:
    schema["components"]["schemas"]["ValidationError"]["properties"]["loc"]["items"] = {
        "type": "string",
    }
    # these two have no type associated to them and will be resolved
    # to nothing in the Elm code, so we just delete them
    del schema["components"]["schemas"]["ValidationError"]["properties"]["input"]
    del schema["components"]["schemas"]["ValidationError"]["properties"]["ctx"]

    # This used to be "format": "binary" but now is
    # "contentMediaType": "application/octet-stream", which the Elm
    # converter then converts into a plain "string" instead of a
    # "File". It also complains about the spec here, so let's just add
    # the format field and be done with it.
    for binary in (
        "Body_create_file_api_files_post",
        "Body_update_live_stream_api_live_stream__beamtimeId__post",
        "Body_bulk_import_api_run_bulk_import__beamtimeId__post",
    ):
        schema["components"]["schemas"][binary]["properties"]["file"]["format"] = (
            "binary"
        )
except KeyError:
    # This is fine, we might not have the type in here.
    pass

print(json.dumps(schema))  # noqa: T201
# ruff: enable[ERA001]
