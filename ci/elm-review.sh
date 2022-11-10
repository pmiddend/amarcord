#!/usr/bin/env bash

set -eu
set -o pipefail

cd frontend
nix develop '..#frontend' --command elm-review
