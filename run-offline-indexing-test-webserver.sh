#!/usr/bin/env bash

export AMARCORD_DB_URL=sqlite+aiosqlite:////tmp/new-test.db

export FLASK_ENV=development
export FLASK_APP='amarcord.cli.webserver'
export LOG_OUTPUT_FILE_PREFIX="/tmp/amarcord-test-webserver"

export DB_URL="$AMARCORD_DB_URL"
python amarcord/cli/upgrade_db_to_latest.py --db-connection-url="$AMARCORD_DB_URL"
uvicorn --port 5001 --host 0.0.0.0 amarcord.cli.webserver:app --reload
