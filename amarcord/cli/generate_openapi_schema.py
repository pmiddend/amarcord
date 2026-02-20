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
except KeyError:
    # This is fine, we might not have the type in here.
    pass

print(json.dumps(schema))  # noqa: T201
# ruff: enable[ERA001]
