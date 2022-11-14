#!/usr/bin/env bash

set -eu
set -o pipefail
set -x

IMAGE_SLUG_TAG="$CI_REGISTRY_IMAGE:$CI_COMMIT_REF_SLUG"
echo "Pushing image to registry under docker://${IMAGE_SLUG_TAG}"
eval "$(nix build '.#amarcord-docker-image' --print-out-paths)" | gzip --fast | nix develop --command skopeo --insecure-policy copy --dest-creds "$CI_REGISTRY_USER":"$CI_REGISTRY_PASSWORD" docker-archive:/dev/stdin "docker://${IMAGE_SLUG_TAG}"
echo "Done"
