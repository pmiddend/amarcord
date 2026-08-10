#!/usr/bin/env bash

set -euo pipefail

log() {
    echo "$(date --iso-8601=seconds): $1"
}

die() {
    log "$1"
    exit 1
}


if [ $# -eq 0 ]; then
    if [ -z "${TMP_DIR+x}" ]; then
	log "TMP_DIR is not set, trying with /tmp instead"
	MY_TMP_DIR=/tmp
    else
	MY_TMP_DIR="$TMP_DIR"
    fi
    [ -d "$MY_TMP_DIR" ] || die "Directory \"$MY_TMP_DIR\" is not a directory, cannot find temporary directory to create database in; quitting."
    MY_DATABASE_FILE="$MY_TMP_DIR/amarcord.db"
else
    if [ "$1" = "--help" ]; then
	echo "usage: $(basename "$0") <database-file>"
	echo ""
	echo "Specifying the database file is optional. If not given, a temporary file name will be used."
	exit 1
    fi
    MY_DATABASE_FILE="$1"
fi

if [ -f "$MY_DATABASE_FILE" ]; then
    log "Found existing database $MY_DATABASE_FILE"
else
    log "Creating an empty database inside \"$MY_DATABASE_FILE\"."
fi

command -v uv > /dev/null || die "Could not find \"uv\", cannot run Python programs. Please install it according to the instructions at https://docs.astral.sh/uv/#installation"

MY_DB_URL="sqlite+aiosqlite:///${MY_DATABASE_FILE}"

log "Migrating database to latest version..."
uv run amarcord-upgrade-db-to-latest --db-connection-url "$MY_DB_URL"
log "Migration complete!"

log "Building frontend..."
cd frontend
./build-elm-manually.sh
cd ..
log "Frontend build!"

log "Running web server..."
DB_URL="$MY_DB_URL" uv run uvicorn --port 5001 --host 0.0.0.0 amarcord.cli.webserver:app --reload
