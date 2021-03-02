#!/usr/bin/env bash

set -e

[ -f schema.json ] || exit 1

rm -rf src/App/AmarQL
npx purescript-graphql-client-generator --input-json schema.json --output src/App/AmarQL --api App.AmarQL
