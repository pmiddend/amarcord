#!/usr/bin/env bash

set -eu

PYTHONPATH=$(pwd)
export PYTHONPATH

DB_URL_FILE=$(pwd)/cypress.sqlite
DB_URL=sqlite:///$DB_URL_FILE

echo "=> removing DB"
rm -f "$DB_URL_FILE"
echo "<= removing DB"


echo "database url: $DB_URL"
echo "=> creating hostal tables"
python snippets/create_hostal_tables.py --db-connection-url "$DB_URL"
echo "<= creating hostal tables"

echo "make sure the webserver and client is running in the background"
# shellcheck disable=SC2034
read -r line

echo "=> running tests"
cd purescript-frontend
direnv exec . npm run cypress:run
echo "<= running tests"


