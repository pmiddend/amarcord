#!/usr/bin/env bash

set -eu
set -o pipefail

poetry install
poetry run pytest --cov=amarcord/ --junitxml=report.xml tests
poetry run coverage xml
