#!/usr/bin/env bash

set -eu
set -o pipefail
set -x

echo "Building Docker image, storing into image.tar.gz"
nix build '.#amarcord-docker-image' -o image.tar.gz
IMAGE_SLUG_TAG="$CI_REGISTRY_IMAGE:$CI_COMMIT_REF_SLUG"
echo "Pushing image to registry under docker://${IMAGE_SLUG_TAG}"
nix develop --command skopeo --insecure-policy copy --dest-creds "$CI_REGISTRY_USER":"$CI_REGISTRY_PASSWORD" docker-archive:image.tar.gz "docker://${IMAGE_SLUG_TAG}"
echo "Done"
