#!/usr/bin/env bash

set -eu
set -o pipefail

# Taken liberally from
# https://docs.gitlab.com/ee/ci/testing/test_coverage_visualization.html#python-example
# and
# https://docs.gitlab.com/ee/ci/testing/test_coverage_visualization/cobertura.html#python-example
#
# Note that --cov-report term is necessary, because GitLab CI parses this output to
# determine the percentage values.
pytest --cov=amarcord/ --cov-branch --cov-report term --cov-report xml:coverage.xml --junitxml=report.xml tests
coverage xml
