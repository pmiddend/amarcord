#!/usr/bin/env bash

# We're referencing some png files directly from Purescript source
# code, which parcel has a hard time figuring out. See
# https://github.com/parcel-bundler/parcel/issues/1411
npx parcel build static/*png
npx spago build && npm run serve
