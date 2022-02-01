#!/usr/bin/env bash

set -eu
set -o pipefail

elm2nix convert > elm-srcs.nix
elm2nix snapshot > versions.dat
