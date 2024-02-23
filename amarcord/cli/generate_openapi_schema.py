import json

from fastapi.openapi.utils import get_openapi

from amarcord.cli.webserver import app

schema = get_openapi(version="2.5.0", routes=app.routes, title="AMARCORD OpenAPI", openapi_version="3.0.3")

# The openapi-generator-cli doesn't support version 3.1 which FastAPI
# generates (as of August 2023), and generates bogus Elm output
# (containing "AnyType" which isn't declared anywhere). We can
# actually fix this using this explicit change.
# schema["openapi"] = "3.0.0"

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

# schema["openapi"] = "3.0.0"

try:
    schema["components"]["schemas"]["ValidationError"]["properties"]["loc"]["items"] = {
        "type": "string"
    }
except KeyError:
    # This is fine, we might not have the type in here.
    pass

print(json.dumps(schema))
