#!/usr/bin/env bash

set -eux

apt-get update -y && apt-get install -y npm libtinfo5 libgtk2.0-0 libgtk-3-0 libgbm-dev libnotify-dev libgconf-2-4 libnss3 libxss1 libasound2 libxtst6 xauth xvfb

pip install -e .

PYTHONPATH="$(pwd)"
export PYTHONPATH

echo "Waiting for initialized database..."
while true; do
  if python snippets/create_hostal_tables.py --db-connection-url "$AMARCORD_DB_URL"; then
	  break
  fi
  sleep 5s;
done

echo "Database created and initialized"

echo "Initializing NPM repo"

pushd purescript-frontend
npm ci

npm install wait-on

echo "Waiting for AMARCORD server to come up"
npx wait-on http://amarcord-app:5000

echo "Running tests..."
npm run cypress:run -- --config baseUrl=http://amarcord-app:5000
