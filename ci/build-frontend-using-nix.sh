#!/usr/bin/env bash

set -eu
set -o pipefail

cd frontend || exit 1
nix-build
mkdir output
cp result/Main.min.js output/main.js
cp src/index.html ./*.css ./*.png output/
