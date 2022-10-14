#!/usr/bin/env bash

set -eu
set -o pipefail

cd frontend
nix develop --command elm-review
