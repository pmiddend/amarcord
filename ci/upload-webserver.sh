#!/usr/bin/env bash

# I tried putting just ./ci/upload-webserver.sh into .gitlab-ci.yml but it told me "not found". Weird...

set -eu
set -o pipefail

mkdir -p /kaniko/.docker
echo "{\"auths\":{\"$CI_REGISTRY\":{\"username\":\"$CI_REGISTRY_USER\",\"password\":\"$CI_REGISTRY_PASSWORD\"}}}" > /kaniko/.docker/config.json
/kaniko/executor --context "$CI_PROJECT_DIR" --dockerfile "$CI_PROJECT_DIR/Dockerfile" --destination "$IMAGE_SLUG_TAG" --destination "$IMAGE_COMMIT_TAG"
