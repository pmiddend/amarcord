#!/usr/bin/env bash

set -eu
set -o pipefail

echo "Regenerating requirements.txt using poetry."
poetry export --dev --without-hashes > requirements.txt
echo "Done, requirements.txt updated!"

