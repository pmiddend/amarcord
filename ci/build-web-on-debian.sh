#!/usr/bin/env bash

set -eux

cd purescript-frontend

SECONDS=0
apt-get update
apt-get install -y libtinfo5 nodejs npm git
duration=$SECONDS
# shellcheck disable=SC2004
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed for apt-get."

SECONDS=0
npm install
duration=$SECONDS
# shellcheck disable=SC2004
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed for npm install."
SECONDS=0
npx spago install
duration=$SECONDS
# shellcheck disable=SC2004
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed for spago install."

SECONDS=0
npm run build-prod
duration=$SECONDS
# shellcheck disable=SC2004
echo "$(($duration / 60)) minutes and $(($duration % 60)) seconds elapsed for build-prod."
