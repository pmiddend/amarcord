#!/usr/bin/env bash

set -euo pipefail

python amarcord/cli/migrate.py "$1"
