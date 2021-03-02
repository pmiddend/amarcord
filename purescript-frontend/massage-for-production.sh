#!/bin/sh

set -eux

sed -i -e "s/latest/$CI_COMMIT_SHORT_SHA/" src/App/Router.purs
sed -i -e "s/baseUrl =.*/baseUrl = \"\/graphql\"/" src/App/Config.purs
