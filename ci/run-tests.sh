#!/usr/bin/env bash

set -eu
set -o pipefail

# Taken liberally from
# https://docs.gitlab.com/ee/ci/testing/test_coverage_visualization.html#python-example
pytest --cov=amarcord/ --cov-branch --cov-report xml:coverage.xml --junitxml=report.xml tests
coverage xml
