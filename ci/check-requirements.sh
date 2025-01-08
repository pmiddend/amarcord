#!/usr/bin/env bash

set -eu
set -o pipefail

uv pip compile pyproject.toml -o requirements-new.txt
# Thanks https://unix.stackexchange.com/questions/17040/how-to-diff-files-ignoring-comments-lines-starting-with
diff -u -B <(grep -vE '^\s*(#|$)' requirements.txt)  <(grep -vE '^\s*(#|$)' requirements-new.txt)
