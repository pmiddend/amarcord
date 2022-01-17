#!/usr/bin/env bash

set -eu
set -o pipefail

poetry install
poetry run mypy amarcord tests
poetry run pylint amarcord tests
