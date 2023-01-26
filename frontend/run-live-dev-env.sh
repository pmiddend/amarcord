#!/usr/bin/env bash

cp ./*.css ./*.png output/

PATH="$PWD:${PATH}" npx elm-go src/Main.elm\
    --start-page=src/index.html\
    --pushstate\
    --hot\
    --proxy-prefix "/api"\
    --proxy-host 'http://0.0.0.0:5000/api'\
    --dir=output/\
    --\
    --output=output/main.js
