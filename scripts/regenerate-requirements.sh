#!/usr/bin/env bash

set -eu
set -o pipefail

echo "Regenerating requirements.txt using uv."
uv pip compile pyproject.toml -o requirements.txt
echo "Done, requirements.txt updated!"

