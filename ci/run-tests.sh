#!/usr/bin/env bash

set -eu
set -o pipefail

pytest --cov=amarcord/ --junitxml=report.xml tests
coverage xml
