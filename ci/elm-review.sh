#!/usr/bin/env bash

set -eu
set -o pipefail

cd frontend
nix-shell --run elm-review
