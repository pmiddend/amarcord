#!/usr/bin/env bash

set -eu
set -o pipefail

mkdir -p output

echo "Downloading mtz.js"
curl --silent 'https://raw.githubusercontent.com/uglymol/uglymol.github.io/master/wasm/mtz.js' --output output/mtz.js
echo "Downloading mtz.wasm"
curl --silent 'https://raw.githubusercontent.com/uglymol/uglymol.github.io/master/wasm/mtz.wasm' --output output/mtz.wasm
echo "Downloading uglymol.min.js"
curl --silent 'https://gitlab.desy.de/cfel-sc-public/uglymol/-/raw/amarcord-fixes/uglymol.min.js?inline=false' --output output/uglymol.min.js
echo "Copying uglymol custom element"
cp uglymol-custom-element.js output/
echo "All dependencies downloaded to output/"
