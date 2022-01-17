#!/usr/bin/env bash

set -eu
set -o pipefail

# without-hashes because:
# https://github.com/python-poetry/poetry/issues/3472
poetry export --dev --without-hashes -o requirements-new.txt
diff requirements.txt requirements-new.txt
