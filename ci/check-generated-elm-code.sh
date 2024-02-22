#!/usr/bin/env bash

set -euo pipefail

rm -rf old-generated
cp -R frontend/generated old-generated
./scripts/generate-elm-from-openapi.sh

diff -r frontend/generated old-generated
