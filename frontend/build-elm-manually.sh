#!/usr/bin/env bash

set -euo pipefail

die() {
    echo "$1"
    exit 1
}

command -v elm > /dev/null || die "couldn't find elm executable, cannot proceed"

mkdir -p build-output
elm make src/Main.elm --optimize --output build-output/main.js
cp -R ./src/index.html ./assets/* build-output

