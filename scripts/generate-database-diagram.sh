#!/usr/bin/env bash

set -uo pipefail

python amarcord/cli/upgrade_db_to_latest.py --db-connection-url 'sqlite+aiosqlite:///testdb'

schemacrawler --server sqlite --database ./testdb --command brief --output-format=png --output-file=docs/source/database.png --info-level standard --portable-names
