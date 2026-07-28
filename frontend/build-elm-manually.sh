#!/usr/bin/env bash

set -euo pipefail

die() {
    echo "$1"
    exit 1
}

download_elm() {
    echo "Downloading elm."
    command -v curl > /dev/null || die "Could not download elm, no \"curl\" installed."
    command -v gunzip > /dev/null || die "Could not download elm, no \"gunzip\" installed."
    curl -L -o elm.gz https://github.com/elm/compiler/releases/download/0.19.1/binary-for-linux-64-bit.gz
    gunzip elm.gz
    chmod +x elm
    echo "Downloaded elm locally."
}

ELM_EXECUTABLE="elm"

if ! command -v "$ELM_EXECUTABLE" > /dev/null; then
    download_elm
    ELM_EXECUTABLE="./elm"
fi

command -v "$ELM_EXECUTABLE" || die "Could not find or install \"elm\" - quitting."

mkdir -p output
"$ELM_EXECUTABLE" make src/Main.elm --optimize --output output/main.js
cp -R ./src/index.html ./assets/* output

