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
#
# Regarding "-n 5", this is an empirically-derived value for the number
# of parallel tests. More doesn't mean better because of
# initialization time for example.
pytest -n 5 --cov=amarcord/ --cov-branch --cov-report term --cov-report xml:coverage.xml --junitxml=report.xml tests
coverage xml
