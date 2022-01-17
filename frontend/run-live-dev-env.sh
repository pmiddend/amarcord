#!/usr/bin/env bash

PATH="$PWD:${PATH}" npx elm-live src/Amarcord/Main.elm\
    --pushstate\
    --start-page=src/index.html\
    --proxy-prefix "/api"\
    --proxy-host 'http://localhost:5000/api'\
    --\
    --output=main.js
