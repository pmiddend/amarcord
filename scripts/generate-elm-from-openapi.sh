#!/usr/bin/env bash

set -euo pipefail

# This script first generates the "openapi.json" file which contains
# the OpenAPI schema and then uses openapi-generator-cli generate Elm
# code for it.

# A little helper function because "exit" only gets an integer (the
# program's exit code) and we want to output a little message also
die() {
    echo "$@"
    exit 1
}

command -v "python" >/dev/null || die "Python not found, cannot generate openapi.json"
command -v "openapi-generator-cli" >/dev/null || die "openapi-generator-cli not found, cannot generate Elm code"

MY_TARGET_DIR="frontend/generated"

[ -d "$MY_TARGET_DIR" ] || die "couldn't find target directory $MY_TARGET_DIR - are you in the root of the project?"

# We store the temporary files in a temporary directory which we want
# to throw away once the script is done. This is the preferred way to
# do this
MY_TEMP_DIR="$(mktemp -d)"
trap '{ rm -rf -- "$MY_TEMP_DIR"; }' EXIT

MY_OPENAPI_JSON_FILE="${MY_TEMP_DIR}/openapi.json"

# This is weird, I know, but "generate_openapi_schema.py" actually
# creates the "app" and gets the schema from it. On creation, the
# constructor checks if the static files directory (output/) is
# present. This isn't the case, because that's only relevant for
# development builds. So we create it here.
mkdir -p frontend/output
python amarcord/cli/generate_openapi_schema.py  > "$MY_OPENAPI_JSON_FILE"
echo "generated openapi.json"

# From pydantic-2 on (somehow it depends on pydantic, not fastapi), for the JSON schema discriminated unions, we get objects with "const" in them. For example: "const": "integer". These are not recognized by openapi-generator-cli yet, see
#
# https://github.com/OpenAPITools/openapi-generator/issues/10445
#
# To circumvent it, we remove the fields - for now.
sed -i -e 's/"const": "[^"]*", //g' -e 's/, "const": "[^"]*"\}/}/g' "$MY_OPENAPI_JSON_FILE"

openapi-generator-cli generate --generator-name elm --input-spec "$MY_OPENAPI_JSON_FILE" --output "$MY_TEMP_DIR"
echo "generated Elm code"

# This warrants an explanation: the openapi-generator (at least for
# Elm) always wants the concrete address of the server to generate an
# API for. So for instance, it wants "https://myapi.com/api" or
# something. It'll create a snippet of Elm code like the following:
#
# url = Url.Builder.crossOrigin req.basePath req.pathParams req.queryParams
#
# Which will generate an absolute URL. However, we want a relative URL, so we replace the snippet to...
#
# url = Url.Builder.relative req.pathParams req.queryParams
#
# Note the lack of basePath here, we don't need it anymore.
#
# The Json.Decode.Field "type_" thing is a bit tricky as well. We have a union with a string field to
# discriminate which union it is. This string field is called "type". The generator, however, makes
# "type_" out of this, since (I think) it believes that it cannot use the keyword "type" here, which
# is not true - you can use any characters and words in strings!
#
# So we're helping the generator here.
sed -i -e 's#crossOrigin req.basePath#relative#' "$MY_TEMP_DIR/src/Api.elm"
sed -i -e 's/, AttributoTypeType(..), .*//' -e 's/, ValueTypeType(..), .*//' -e 's/, ItemsType(..), .*//' -e 's/, TypeType(..), .*//' -e 's/Json.Decode.field "type_"/Json.Decode.field "type"/' "$MY_TEMP_DIR/src/Api/Data.elm"

cp -R "$MY_TEMP_DIR"/src/* "$MY_TARGET_DIR"

