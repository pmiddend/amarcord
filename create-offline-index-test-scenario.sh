#!/usr/bin/env bash

export AMARCORD_DB_URL=sqlite+aiosqlite:////tmp/new-test.db

rm -f /tmp/new-test.db
python amarcord/cli/offline_index_test_scenario.py --db-connection-url="$AMARCORD_DB_URL"
