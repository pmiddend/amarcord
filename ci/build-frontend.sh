#!/usr/bin/env bash

set -eu
set -o pipefail

cd frontend || exit  1
mkdir output

if [ -n "$CI_COMMIT_SHORT_SHA" ]; then
  sed -i -e "s/\"dev\"/\"$CI_COMMIT_SHORT_SHA\"/" src/Amarcord/Version.elm
fi

npm install uglify-js elm
npx elm make src/Main.elm --optimize --output main.js
npx uglifyjs main.js --compress "pure_funcs=[F2,F3,F4,F5,F6,F7,F8,F9,A2,A3,A4,A5,A6,A7,A8,A9],pure_getters,keep_fargs=false,unsafe_comps,unsafe" | npx uglifyjs --mangle --output output/main.js
echo "Original .js file: $(du -sh main.js)"
echo "Minified .js file: $(du -sh output/main.js)"
cp src/index.html ./*.svg ./*.css ./*.png ./*.jpg output/
